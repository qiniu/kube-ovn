package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	kubeovnv1 "github.com/kubeovn/kube-ovn/pkg/apis/kubeovn/v1"
	"github.com/kubeovn/kube-ovn/pkg/util"
)

// The nftable LB service feature makes a vpc-nat-gw act like kube-proxy for a Service: it
// watches the Service and its EndpointSlices and programs the share-DNAT dataplane (nft
// numgen random map) of the gateway so that new connections are load balanced across the
// ready backends and pinned by conntrack.
//
// Binding model:
//   - `ovn.kubernetes.io/vpc_nat_gw` (util.VpcNatGatewayAnnotation) names the target gateway
//     and is the feature switch: without it nothing happens.
//   - A ClusterIP Service needs nothing else: the DNAT external address is its own ClusterIP,
//     i.e. the gateway makes the ClusterIP reachable from inside the VPC.
//   - A LoadBalancer Service additionally requires `ovn.kubernetes.io/eip`
//     (util.EipAnnotation), naming an existing IptablesEIP that must live on the annotated
//     gateway; its address is the DNAT external address and is published as the ingress IP.
//
// TODO: allocate an EIP from the default macvlan external network pool when
//
//	`ovn.kubernetes.io/eip` is absent on a LoadBalancer Service: set its natGwDp to the
//	annotated gateway, wait for its IPv4 address, and release it on Service deletion or
//	annotation change.
//
// Single write path:
//
//	The Service is the only source of truth and it drives the gateway dataplane directly
//	(createNftDnatMapInPod / deleteNftDnatMapInPod). The IptablesDnatRule objects written
//	afterwards are *records* only: they make the programmed forwarding visible in one lookup
//	(`kubectl get dnat`) and keep the EIP in-use accounting (util.EipUIDLabel) working exactly
//	like hand-written DNAT rules. They never drive the dataplane: the IptablesDnatRule
//	controller skips every object carrying util.NftableLbSvcRecordLabel, so a record can not
//	become a second writer of the same nft identity.
//
//	The records double as the ledger of what has been programmed: an identity that exists as a
//	record but is no longer desired is removed from the gateway. Records are labelled with the
//	owning Service (util.NftableLbSvcNsLabel / util.NftableLbSvcNameLabel) because several
//	Services of the same tenant may share one EIP, so the external address does not identify
//	the owner. The labels only scope the ledger; they drive no configuration. A crash between
//	programming the dataplane and writing the record can leave a stale nft identity behind;
//	this is accepted (delete the Service's records and let it reconcile, or recreate the
//	gateway).
//
// Sharing one EIP across Services:
//
//	Supported, and isolated per identity (eip + port + protocol), not per Service: each
//	Service programs only its own ports and only its own records. Two Services declaring the
//	same port on the same EIP would be a single nft map with two writers, so the
//	lexicographically smaller Service key keeps that identity and the other skips it with a
//	Warning event (see dropNftableLbConflictingIdentities).
//
// Traffic policy scope (by design):
//
//	This feature intentionally aligns with kube-proxy's *Cluster* traffic policy only. It
//	always load balances across all Ready endpoints and does NOT implement
//	ExternalTrafficPolicy/InternalTrafficPolicy=Local, topology-aware routing, or
//	terminating-endpoint fallback. Client source IP is therefore not preserved.
//
// Boundaries (deliberate, do not "fix" by adding guards):
//   - The ClusterIP scenario only works if traffic to the Service CIDR actually reaches the
//     gateway (the VPC routes it there); this feature programs the gateway, it does not
//     create routes.
//   - The feature must stay enabled for a claimed Service to be reconciled. Disabling it
//     while Services carry the finalizer leaves them undeletable until it is enabled again.
//   - A hand-written IptablesDnatRule on the same EIP:port as a Service is an operator
//     planning error, not a case this code arbitrates: both would program the same nft map.
//   - Residue is tolerated over complexity. A crash between programming and recording, or a
//     record deleted by hand, can leave nft rules nobody reclaims; delete the Service (or
//     recreate the gateway) to clear them.

// nftableLbSvcTarget is the resolved gateway binding of a Service: where to program the share
// DNAT (gw, its VPC) and which external address the backends are reached through.
type nftableLbSvcTarget struct {
	gw         string
	vpc        string
	externalIP string
	eip        *kubeovnv1.IptablesEIP // nil in the ClusterIP scenario
}

// nftableLbIdentity is a share DNAT identity: one nft map keyed by external address, protocol
// and port, holding all backends of a single Service port.
type nftableLbIdentity struct {
	protocol     string
	externalPort string
}

// nftableLbSvcQualifies reports whether a Service should be handled by the nftable LB service
// feature: it must target a gateway, and a LoadBalancer must also reference an EIP.
func nftableLbSvcQualifies(svc *v1.Service) bool {
	if svc.Annotations[util.VpcNatGatewayAnnotation] == "" {
		return false
	}
	switch svc.Spec.Type {
	case v1.ServiceTypeClusterIP:
		return true
	case v1.ServiceTypeLoadBalancer:
		return svc.Annotations[util.EipAnnotation] != ""
	default:
		return false
	}
}

// enqueueNftableLbService enqueues a Service key for nftable LB reconciliation. Qualification
// and cleanup decisions are made in the handler so a Service that stops qualifying still gets
// its dataplane and records cleaned up.
func (c *Controller) enqueueNftableLbService(key string) {
	if c.config == nil || !c.config.EnableNftableLbSvc || c.addOrUpdateNftableLbSvcQueue == nil || key == "" {
		return
	}
	klog.V(3).Infof("enqueue add/update nftable lb service %s", key)
	c.addOrUpdateNftableLbSvcQueue.Add(key)
}

// enqueueNftableLbServicesForPod enqueues the Services backed by the pod. Endpoint readiness
// arrives through EndpointSlice events; the pod itself only matters for its kube-ovn NIC
// addresses, so callers must filter updates that leave those unchanged (see
// nftableLbPodAddressesChanged) to avoid re-executing nft commands in the gateway for every
// unrelated pod status update.
func (c *Controller) enqueueNftableLbServicesForPod(pod *v1.Pod) {
	if c.config == nil || !c.config.EnableNftableLbSvc || pod == nil || c.endpointSlicesLister == nil {
		return
	}
	slices, err := c.endpointSlicesLister.EndpointSlices(pod.Namespace).List(labels.Everything())
	if err != nil {
		klog.Errorf("failed to find endpoint slices for pod %s/%s: %v", pod.Namespace, pod.Name, err)
		return
	}
	seen := make(map[string]struct{})
	for _, endpointSlice := range slices {
		for _, endpoint := range endpointSlice.Endpoints {
			if endpoint.TargetRef == nil || endpoint.TargetRef.Kind != "Pod" || endpoint.TargetRef.Name != pod.Name {
				continue
			}
			if key := findServiceKey(endpointSlice); key != "" {
				seen[key] = struct{}{}
			}
		}
	}
	for key := range seen {
		c.enqueueNftableLbService(key)
	}
}

// nftableLbSvcChanged reports whether a Service update touched anything this feature reads.
// It keeps the Service's own status writes (the published ingress IP) and unrelated metadata
// churn from re-executing nft commands in the gateway.
func nftableLbSvcChanged(oldSvc, newSvc *v1.Service) bool {
	return oldSvc.Annotations[util.VpcNatGatewayAnnotation] != newSvc.Annotations[util.VpcNatGatewayAnnotation] ||
		oldSvc.Annotations[util.EipAnnotation] != newSvc.Annotations[util.EipAnnotation] ||
		oldSvc.Spec.Type != newSvc.Spec.Type ||
		!slices.Equal(oldSvc.Spec.ClusterIPs, newSvc.Spec.ClusterIPs) ||
		oldSvc.Spec.SessionAffinity != newSvc.Spec.SessionAffinity ||
		!equality.Semantic.DeepEqual(oldSvc.Spec.SessionAffinityConfig, newSvc.Spec.SessionAffinityConfig) ||
		!equality.Semantic.DeepEqual(oldSvc.Spec.Ports, newSvc.Spec.Ports) ||
		oldSvc.DeletionTimestamp.IsZero() != newSvc.DeletionTimestamp.IsZero() ||
		!slices.Equal(oldSvc.Finalizers, newSvc.Finalizers)
}

// nftableLbPodAddressesChanged reports whether any kube-ovn NIC address of the pod changed.
// Those annotations are the only pod field the share DNAT backends are built from.
func nftableLbPodAddressesChanged(oldPod, newPod *v1.Pod) bool {
	const suffix = ".kubernetes.io/ip_address"
	for key, value := range newPod.Annotations {
		if strings.HasSuffix(key, suffix) && oldPod.Annotations[key] != value {
			return true
		}
	}
	for key, value := range oldPod.Annotations {
		if strings.HasSuffix(key, suffix) && newPod.Annotations[key] != value {
			return true
		}
	}
	return false
}

// enqueueNftableLbServicesForNatGw enqueues every Service bound to the gateway. It is the
// replay path: after the gateway pod is (re)created its nft rules are empty, and reconciling
// the owning Services reprograms them from the single write path.
func (c *Controller) enqueueNftableLbServicesForNatGw(natGwName string) {
	if c.config == nil || !c.config.EnableNftableLbSvc || natGwName == "" || c.svcIndexer == nil {
		return
	}
	services, err := c.svcIndexer.ByIndex(IndexServiceByNftableLbGateway, natGwName)
	if err != nil {
		klog.Errorf("failed to find nftable lb services for nat gateway %s: %v", natGwName, err)
		return
	}
	for _, obj := range services {
		if svc, ok := obj.(*v1.Service); ok {
			c.enqueueNftableLbService(svc.Namespace + "/" + svc.Name)
		}
	}
}

func (c *Controller) enqueueNftableLbServicesForEIP(eipName string) {
	if c.config == nil || !c.config.EnableNftableLbSvc || c.svcIndexer == nil || eipName == "" {
		return
	}
	services, err := c.svcIndexer.ByIndex(IndexServiceByNftableLbEip, eipName)
	if err != nil {
		klog.Errorf("failed to find nftable lb services for eip %s: %v", eipName, err)
		return
	}
	for _, obj := range services {
		if svc, ok := obj.(*v1.Service); ok {
			c.enqueueNftableLbService(svc.Namespace + "/" + svc.Name)
		}
	}
}

func (c *Controller) handleAddOrUpdateNftableLbService(key string) error {
	if !c.config.EnableNftableLbSvc {
		return nil
	}

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	klog.Infof("handle add/update nftable lb service %s", key)

	cachedSvc, err := c.servicesLister.Services(namespace).Get(name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			// The finalizer guarantees cleanup ran before the Service disappeared.
			return nil
		}
		klog.Error(err)
		return err
	}

	if !cachedSvc.DeletionTimestamp.IsZero() || !nftableLbSvcQualifies(cachedSvc) {
		return c.cleanupNftableLbService(cachedSvc)
	}

	target, err := c.resolveNftableLbSvcTarget(cachedSvc)
	if err != nil {
		return err
	}
	if target == nil {
		return c.cleanupNftableLbService(cachedSvc)
	}

	// Claim the Service before touching the gateway: a deletion racing with programming must
	// be held until the nft rules are removed again.
	if err = c.addNftableLbSvcFinalizer(cachedSvc); err != nil {
		return err
	}

	endpointSlices, err := c.endpointSlicesLister.EndpointSlices(namespace).List(labels.Set{discoveryv1.LabelServiceName: name}.AsSelector())
	if err != nil {
		klog.Errorf("failed to list endpoint slices for nftable lb service %s: %v", key, err)
		return err
	}
	desired := buildNftableLbBackends(cachedSvc, endpointSlices, c.nftableLbBackendResolver(cachedSvc, target.vpc))
	if err = c.dropNftableLbConflictingIdentities(cachedSvc, target, desired); err != nil {
		return err
	}

	if err = c.programNftableLbService(cachedSvc, target, desired); err != nil {
		return err
	}
	if err = c.reconcileNftableLbSvcRecords(cachedSvc, target, desired); err != nil {
		return err
	}
	if target.eip == nil {
		return nil
	}
	if err = c.ensureNftableLbSvcIngressIP(cachedSvc, target.externalIP); err != nil {
		klog.Errorf("failed to set ingress ip for nftable lb service %s: %v", key, err)
		return err
	}
	return nil
}

// resolveNftableLbSvcTarget resolves the gateway and external address of a qualifying Service.
// It returns (nil, nil) when the Service currently cannot be programmed (missing or terminating
// gateway, EIP on another gateway, no IPv4 external address), in which case the caller cleans
// up whatever was programmed before.
func (c *Controller) resolveNftableLbSvcTarget(svc *v1.Service) (*nftableLbSvcTarget, error) {
	key := svc.Namespace + "/" + svc.Name
	gwName := svc.Annotations[util.VpcNatGatewayAnnotation]
	natGw, err := c.vpcNatGatewayLister.Get(gwName)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			klog.Warningf("nftable lb service %s targets nat gateway %s which does not exist", key, gwName)
			return nil, nil
		}
		klog.Errorf("failed to get nat gateway %s for nftable lb service %s: %v", gwName, key, err)
		return nil, err
	}
	if !natGw.DeletionTimestamp.IsZero() {
		return nil, nil
	}
	target := &nftableLbSvcTarget{gw: gwName, vpc: natGw.Spec.Vpc}

	if svc.Spec.Type != v1.ServiceTypeLoadBalancer {
		// ClusterIP scenario: the ClusterIP itself is the DNAT external address.
		target.externalIP = firstIPv4(svc.Spec.ClusterIPs)
		if target.externalIP == "" {
			klog.Warningf("nftable lb service %s has no IPv4 cluster ip, skipping (share dnat is IPv4 only)", key)
			return nil, nil
		}
		return target, nil
	}

	eipName := svc.Annotations[util.EipAnnotation]
	eip, err := c.GetEip(eipName)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, nil
		}
		// EIP not ready yet: requeue and retry once it has an IPv4 address.
		klog.Errorf("nftable lb service %s references eip %s which is not ready: %v", key, eipName, err)
		return nil, err
	}
	// share DNAT is implemented with `ip daddr`/`ip saddr` and only supports IPv4
	if util.CheckProtocol(eip.Status.IP) != kubeovnv1.ProtocolIPv4 {
		klog.Errorf("nftable lb service %s references eip %s without an IPv4 address, skipping (share dnat is IPv4 only)", key, eipName)
		return nil, nil
	}
	if eip.Spec.NatGwDp != gwName {
		klog.Errorf("nftable lb service %s targets nat gateway %s but eip %s lives on %s, skipping", key, gwName, eipName, eip.Spec.NatGwDp)
		c.recorder.Eventf(svc, v1.EventTypeWarning, "NftableLbSvcEipMismatch",
			"eip %s lives on nat gateway %q, not on the annotated gateway %q", eipName, eip.Spec.NatGwDp, gwName)
		return nil, nil
	}
	target.externalIP, target.eip = eip.Status.IP, eip
	return target, nil
}

// addNftableLbSvcFinalizer claims the Service before anything is programmed, so a deletion
// racing with programming is held until the nft rules have been removed again.
func (c *Controller) addNftableLbSvcFinalizer(svc *v1.Service) error {
	if slices.Contains(svc.Finalizers, util.NftableLbSvcFinalizer) {
		return nil
	}
	updated := svc.DeepCopy()
	updated.Finalizers = append(updated.Finalizers, util.NftableLbSvcFinalizer)
	if _, err := c.config.KubeClient.CoreV1().Services(svc.Namespace).Update(context.Background(), updated, metav1.UpdateOptions{}); err != nil {
		klog.Errorf("failed to add finalizer to nftable lb service %s/%s: %v", svc.Namespace, svc.Name, err)
		return err
	}
	return nil
}

// dropNftableLbConflictingIdentities removes the identities another Service on the same EIP
// already owns. Sharing an EIP is supported, but a given eip:port:protocol is a single nft map
// and can only have one writer, so the lexicographically smaller Service key wins it and the
// other backs off (and retries, to take over once the identity is released).
func (c *Controller) dropNftableLbConflictingIdentities(svc *v1.Service, target *nftableLbSvcTarget, desired map[nftableLbIdentity][]string) error {
	if target.eip == nil || len(desired) == 0 {
		// A ClusterIP belongs to exactly one Service, so its identities cannot be contested.
		return nil
	}
	key := svc.Namespace + "/" + svc.Name
	peers, err := c.svcIndexer.ByIndex(IndexServiceByNftableLbEip, target.eip.Name)
	if err != nil {
		klog.Errorf("failed to find services sharing eip %s: %v", target.eip.Name, err)
		return err
	}
	conflicted := false
	for _, obj := range peers {
		peer, ok := obj.(*v1.Service)
		if !ok {
			continue
		}
		peerKey := peer.Namespace + "/" + peer.Name
		if peerKey >= key || !peer.DeletionTimestamp.IsZero() || !nftableLbSvcQualifies(peer) {
			continue
		}
		for _, identity := range nftableLbSvcIdentities(peer) {
			if _, ok := desired[identity]; !ok {
				continue
			}
			delete(desired, identity)
			conflicted = true
			klog.Warningf("nftable lb service %s yields %s:%s (%s) to service %s",
				key, target.externalIP, identity.externalPort, identity.protocol, peerKey)
			c.recorder.Eventf(svc, v1.EventTypeWarning, "NftableLbSvcConflict",
				"eip %s port %s/%s is already used by service %s; this service will not program it",
				target.eip.Name, identity.externalPort, identity.protocol, peerKey)
		}
	}
	if conflicted {
		// retry so this Service can take over once the identity is released
		c.addOrUpdateNftableLbSvcQueue.AddAfter(key, 10*time.Second)
	}
	return nil
}

// nftableLbSvcIdentities returns the share DNAT identities a Service declares, one per tcp/udp
// service port. It is Service intent only: a Service with no ready backend still owns them.
func nftableLbSvcIdentities(svc *v1.Service) []nftableLbIdentity {
	identities := make([]nftableLbIdentity, 0, len(svc.Spec.Ports))
	for _, port := range svc.Spec.Ports {
		protocol := strings.ToLower(string(port.Protocol))
		if protocol != "tcp" && protocol != "udp" {
			continue
		}
		identities = append(identities, nftableLbIdentity{protocol: protocol, externalPort: strconv.Itoa(int(port.Port))})
	}
	return identities
}

// programNftableLbService writes the desired share DNAT identities into the gateway and removes
// the ones that are no longer desired. The previously programmed set is read back from the
// records, which also covers identities left on a gateway the Service no longer points at.
func (c *Controller) programNftableLbService(svc *v1.Service, target *nftableLbSvcTarget, desired map[nftableLbIdentity][]string) error {
	key := svc.Namespace + "/" + svc.Name
	affinity, affinityTimeout := nftableLbSvcSessionAffinity(svc)

	for identity, backends := range desired {
		if err := c.createNftDnatMapInPod(target.gw, identity.protocol, target.externalIP, identity.externalPort,
			backends, affinity, affinityTimeout); err != nil {
			klog.Errorf("failed to program share dnat %s:%s (%s) for service %s: %v",
				target.externalIP, identity.externalPort, identity.protocol, key, err)
			return err
		}
		klog.Infof("programmed share dnat %s:%s (%s) for service %s on gateway %s with backends %v",
			target.externalIP, identity.externalPort, identity.protocol, key, target.gw, backends)
	}

	records, err := c.nftableLbSvcRecords(svc)
	if err != nil {
		return err
	}
	for _, record := range records {
		identity := nftableLbIdentity{protocol: record.Spec.Protocol, externalPort: record.Spec.ExternalPort}
		gw := record.Labels[util.VpcNatGatewayNameLabel]
		externalIP := record.Labels[util.EipV4IpLabel]
		if gw == target.gw && externalIP == target.externalIP {
			if _, ok := desired[identity]; ok {
				continue
			}
		}
		if err := c.removeNftableLbIdentity(gw, identity.protocol, externalIP, identity.externalPort); err != nil {
			klog.Errorf("failed to remove stale share dnat %s:%s (%s) of service %s from gateway %s: %v",
				externalIP, identity.externalPort, identity.protocol, key, gw, err)
			return err
		}
	}
	return nil
}

// cleanupNftableLbService removes everything the Service has programmed: the share DNAT
// identities in the gateway, the records, the published ingress IP, and finally the finalizer.
// A Service this mode never claimed is left untouched, including its status (it may be a
// LoadBalancer handled by another provider).
func (c *Controller) cleanupNftableLbService(svc *v1.Service) error {
	if !slices.Contains(svc.Finalizers, util.NftableLbSvcFinalizer) {
		return nil
	}
	records, err := c.nftableLbSvcRecords(svc)
	if err != nil {
		return err
	}
	for _, record := range records {
		gw := record.Labels[util.VpcNatGatewayNameLabel]
		if err = c.removeNftableLbIdentity(gw, record.Spec.Protocol, record.Labels[util.EipV4IpLabel], record.Spec.ExternalPort); err != nil {
			klog.Errorf("failed to remove share dnat %s:%s of service %s/%s from gateway %s: %v",
				record.Labels[util.EipV4IpLabel], record.Spec.ExternalPort, svc.Namespace, svc.Name, gw, err)
			return err
		}
		if err = c.deleteNftableLbSvcRecord(record.Name); err != nil {
			return err
		}
	}
	if err = c.clearNftableLbSvcIngressIP(svc); err != nil {
		return err
	}
	return c.removeNftableLbSvcFinalizer(svc)
}

// removeNftableLbIdentity deletes a share DNAT identity from the gateway. A missing gateway
// pod is success: the nft rules live in the pod's network namespace and died with it.
func (c *Controller) removeNftableLbIdentity(gw, protocol, externalIP, externalPort string) error {
	if err := c.deleteNftDnatMapInPod(gw, protocol, externalIP, externalPort); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

// nftableLbSvcRecords lists the records written for a Service. Several Services of one tenant
// may share an EIP, so the ledger is scoped by the owning Service labels, not by address.
func (c *Controller) nftableLbSvcRecords(svc *v1.Service) ([]*kubeovnv1.IptablesDnatRule, error) {
	records, err := c.iptablesDnatRulesLister.List(labels.SelectorFromSet(labels.Set{
		util.NftableLbSvcRecordLabel: "true",
		util.NftableLbSvcNsLabel:     svc.Namespace,
		util.NftableLbSvcNameLabel:   svc.Name,
	}))
	if err != nil {
		klog.Errorf("failed to list nftable lb dnat records of service %s/%s: %v", svc.Namespace, svc.Name, err)
		return nil, err
	}
	return records, nil
}

// reconcileNftableLbSvcRecords makes the records match what has just been programmed: one
// record per (identity, backend). Records are written after the dataplane, so a failure here
// leaves the gateway correct and is retried.
func (c *Controller) reconcileNftableLbSvcRecords(svc *v1.Service, target *nftableLbSvcTarget, desired map[nftableLbIdentity][]string) error {
	wanted := buildNftableLbSvcRecords(svc, target, desired)
	existing, err := c.nftableLbSvcRecords(svc)
	if err != nil {
		return err
	}
	needRequeue := false
	for _, record := range existing {
		want, ok := wanted[record.Name]
		if ok && nftableLbRecordEqual(record, want) {
			delete(wanted, record.Name)
			continue
		}
		if !record.DeletionTimestamp.IsZero() {
			// still terminating: recreate it on a later pass
			if _, stillWanted := wanted[record.Name]; stillWanted {
				delete(wanted, record.Name)
				needRequeue = true
			}
			continue
		}
		if err = c.deleteNftableLbSvcRecord(record.Name); err != nil {
			return err
		}
		if _, stillWanted := wanted[record.Name]; stillWanted {
			// Recreating in the same pass would hit the terminating object.
			delete(wanted, record.Name)
			needRequeue = true
		}
	}
	for _, record := range wanted {
		if _, err = c.config.KubeOvnClient.KubeovnV1().IptablesDnatRules().Create(context.Background(), record, metav1.CreateOptions{}); err != nil {
			if k8serrors.IsAlreadyExists(err) {
				// a previous incarnation is still terminating; write it on a later pass
				needRequeue = true
				continue
			}
			klog.Errorf("failed to create nftable lb dnat record %s for service %s/%s: %v", record.Name, svc.Namespace, svc.Name, err)
			return err
		}
		if err = c.patchNftableLbSvcRecordStatus(record, target); err != nil {
			return err
		}
	}
	if needRequeue {
		// The records lag the dataplane, which is already correct; catch up once the deleted
		// objects are gone.
		c.addOrUpdateNftableLbSvcQueue.AddAfter(svc.Namespace+"/"+svc.Name, 2*time.Second)
	}
	return nil
}

func (c *Controller) deleteNftableLbSvcRecord(name string) error {
	if err := c.config.KubeOvnClient.KubeovnV1().IptablesDnatRules().Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		klog.Errorf("failed to delete nftable lb dnat record %s: %v", name, err)
		return err
	}
	klog.Infof("deleted nftable lb dnat record %s", name)
	return nil
}

// patchNftableLbSvcRecordStatus fills in the status of a record so that the programmed
// forwarding is visible in a single `kubectl get dnat`.
func (c *Controller) patchNftableLbSvcRecordStatus(record *kubeovnv1.IptablesDnatRule, target *nftableLbSvcTarget) error {
	status := &kubeovnv1.IptablesDnatRuleStatus{
		Ready:        true,
		V4ip:         target.externalIP,
		NatGwDp:      target.gw,
		Protocol:     record.Spec.Protocol,
		InternalIP:   record.Spec.InternalIP,
		InternalPort: record.Spec.InternalPort,
		ExternalPort: record.Spec.ExternalPort,
	}
	bytes, err := status.Bytes()
	if err != nil {
		klog.Error(err)
		return err
	}
	if _, err = c.config.KubeOvnClient.KubeovnV1().IptablesDnatRules().Patch(context.Background(), record.Name,
		types.MergePatchType, bytes, metav1.PatchOptions{}, "status"); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		klog.Errorf("failed to patch status of nftable lb dnat record %s: %v", record.Name, err)
		return err
	}
	return nil
}

// nftableLbRecordEqual reports whether an existing record already describes the wanted state.
func nftableLbRecordEqual(a, b *kubeovnv1.IptablesDnatRule) bool {
	return a.Spec == b.Spec &&
		a.Labels[util.EipV4IpLabel] == b.Labels[util.EipV4IpLabel] &&
		a.Labels[util.VpcNatGatewayNameLabel] == b.Labels[util.VpcNatGatewayNameLabel] &&
		a.Labels[util.EipUIDLabel] == b.Labels[util.EipUIDLabel]
}

// buildNftableLbSvcRecords builds one record per (identity, backend), keyed by record name.
func buildNftableLbSvcRecords(svc *v1.Service, target *nftableLbSvcTarget, desired map[nftableLbIdentity][]string) map[string]*kubeovnv1.IptablesDnatRule {
	affinity, affinityTimeout := nftableLbSvcSessionAffinity(svc)
	eipName, eipUID := "", ""
	if target.eip != nil {
		eipName, eipUID = target.eip.Name, string(target.eip.UID)
	}
	records := make(map[string]*kubeovnv1.IptablesDnatRule)
	for identity, backends := range desired {
		for _, backend := range backends {
			backendIP, backendPort, ok := strings.Cut(backend, ":")
			if !ok {
				continue
			}
			name := nftableLbDnatRuleName(svc.Namespace, svc.Name, identity.protocol, identity.externalPort, backendIP, backendPort)
			recordLabels := map[string]string{
				util.NftableLbSvcRecordLabel: "true",
				util.NftableLbSvcNsLabel:     svc.Namespace,
				util.NftableLbSvcNameLabel:   svc.Name,
				util.EipV4IpLabel:            target.externalIP,
				util.VpcNatGatewayNameLabel:  target.gw,
				util.VpcDnatEPortLabel:       identity.externalPort,
			}
			if eipUID != "" {
				// Same claim a hand-written DNAT rule makes, so the EIP in-use check
				// (getIptablesEipNat) counts this Service and the EIP cannot be released
				// while it is being forwarded to.
				recordLabels[util.EipUIDLabel] = eipUID
			}
			var recordAnnotations map[string]string
			if eipName != "" {
				recordAnnotations = map[string]string{util.VpcEipAnnotation: eipName}
			}
			records[name] = &kubeovnv1.IptablesDnatRule{
				ObjectMeta: metav1.ObjectMeta{Name: name, Labels: recordLabels, Annotations: recordAnnotations},
				Spec: kubeovnv1.IptablesDnatRuleSpec{
					EIP:                           eipName,
					ExternalPort:                  identity.externalPort,
					Protocol:                      identity.protocol,
					InternalIP:                    backendIP,
					InternalPort:                  backendPort,
					Type:                          kubeovnv1.DnatRuleTypeShare,
					SessionAffinity:               affinity,
					SessionAffinityTimeoutSeconds: affinityTimeout,
				},
			}
		}
	}
	return records
}

// nftableLbSvcSessionAffinity translates the Service's client-IP session affinity into the
// share DNAT fields. All backends of one identity share it, matching kube-proxy where
// affinity is a per-ServicePort property.
func nftableLbSvcSessionAffinity(svc *v1.Service) (string, int32) {
	if svc.Spec.SessionAffinity != v1.ServiceAffinityClientIP {
		return kubeovnv1.DnatSessionAffinityNone, 0
	}
	var timeout int32
	if cfg := svc.Spec.SessionAffinityConfig; cfg != nil && cfg.ClientIP != nil && cfg.ClientIP.TimeoutSeconds != nil {
		timeout = *cfg.ClientIP.TimeoutSeconds
	}
	return kubeovnv1.DnatSessionAffinityClientIP, timeout
}

func (c *Controller) removeNftableLbSvcFinalizer(svc *v1.Service) error {
	if !slices.Contains(svc.Finalizers, util.NftableLbSvcFinalizer) {
		return nil
	}
	updated := svc.DeepCopy()
	updated.Finalizers = slices.DeleteFunc(updated.Finalizers, func(f string) bool { return f == util.NftableLbSvcFinalizer })
	if _, err := c.config.KubeClient.CoreV1().Services(svc.Namespace).Update(context.Background(), updated, metav1.UpdateOptions{}); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		klog.Errorf("failed to remove finalizer from nftable lb service %s/%s: %v", svc.Namespace, svc.Name, err)
		return err
	}
	return nil
}

// ensureNftableLbSvcIngressIP sets the Service's status.loadBalancer.ingress to the EIP's
// IPv4 address so the LoadBalancer reports the externally reachable IP instead of staying
// <pending>. It is a no-op when the ingress list already advertises exactly that IP.
func (c *Controller) ensureNftableLbSvcIngressIP(svc *v1.Service, ip string) error {
	if len(svc.Status.LoadBalancer.Ingress) == 1 && svc.Status.LoadBalancer.Ingress[0].IP == ip {
		return nil
	}
	updated := svc.DeepCopy()
	updated.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{IP: ip}}
	if _, err := c.config.KubeClient.CoreV1().Services(svc.Namespace).UpdateStatus(context.Background(), updated, metav1.UpdateOptions{}); err != nil {
		klog.Errorf("failed to update status of nftable lb service %s/%s: %v", svc.Namespace, svc.Name, err)
		return err
	}
	klog.Infof("set nftable lb service %s/%s ingress ip to %s", svc.Namespace, svc.Name, ip)
	return nil
}

// clearNftableLbSvcIngressIP removes a LoadBalancer ingress IP that this mode previously
// published (so EXTERNAL-IP returns to <pending> once the Service leaves nftable-lb-svc
// mode). It is a no-op when there is nothing to clear or the Service is being deleted.
func (c *Controller) clearNftableLbSvcIngressIP(svc *v1.Service) error {
	if !svc.DeletionTimestamp.IsZero() || len(svc.Status.LoadBalancer.Ingress) == 0 {
		return nil
	}
	updated := svc.DeepCopy()
	updated.Status.LoadBalancer.Ingress = nil
	if _, err := c.config.KubeClient.CoreV1().Services(svc.Namespace).UpdateStatus(context.Background(), updated, metav1.UpdateOptions{}); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		klog.Errorf("failed to clear ingress ip of nftable lb service %s/%s: %v", svc.Namespace, svc.Name, err)
		return err
	}
	klog.Infof("cleared nftable lb service %s/%s ingress ip", svc.Namespace, svc.Name)
	return nil
}

func endpointPortMatchesServicePort(port discoveryv1.EndpointPort, servicePort v1.ServicePort) bool {
	if port.Name == nil {
		if servicePort.Name != "" {
			return false
		}
	} else if *port.Name != servicePort.Name {
		return false
	}
	return port.Protocol == nil || *port.Protocol == servicePort.Protocol
}

// buildNftableLbBackends resolves the Service into share DNAT identities and their backends
// ("ip:port"). backendIP maps each ready endpoint to the single IPv4 the gateway must DNAT to
// (the NIC in the gateway's VPC; see nftableLbBackendResolver); endpoints it cannot resolve
// are skipped. An identity with no backend is not returned: an empty nft map cannot be
// programmed, and the identity is then removed as stale.
func buildNftableLbBackends(svc *v1.Service, endpointSlices []*discoveryv1.EndpointSlice, backendIP func(discoveryv1.Endpoint) (string, bool)) map[nftableLbIdentity][]string {
	desired := make(map[nftableLbIdentity][]string)
	for _, port := range svc.Spec.Ports {
		protocol := strings.ToLower(string(port.Protocol))
		// share DNAT only supports tcp/udp
		if protocol != "tcp" && protocol != "udp" {
			klog.Warningf("skipping service %s/%s port %d: nftable lb service only supports tcp/udp, got %s",
				svc.Namespace, svc.Name, port.Port, port.Protocol)
			continue
		}
		identity := nftableLbIdentity{protocol: protocol, externalPort: strconv.Itoa(int(port.Port))}

		for _, endpointSlice := range endpointSlices {
			var targetPort int32
			for _, p := range endpointSlice.Ports {
				if endpointPortMatchesServicePort(p, port) && p.Port != nil {
					targetPort = *p.Port
					break
				}
			}
			if targetPort == 0 {
				continue
			}
			for _, endpoint := range endpointSlice.Endpoints {
				// Cluster traffic policy only (see file header): every Ready endpoint is a
				// backend, regardless of node locality.
				if !endpointReady(endpoint) {
					continue
				}
				address, ok := backendIP(endpoint)
				if !ok {
					continue
				}
				desired[identity] = append(desired[identity], fmt.Sprintf("%s:%d", address, targetPort))
			}
		}
	}
	return desired
}

// nftableLbNicCandidate is a resolvable kube-ovn NIC of a backend pod: its IPv4 address and
// the VPC its subnet belongs to. Used to pick the backend IP reachable from the gateway.
type nftableLbNicCandidate struct {
	ipv4 string
	vpc  string
}

// selectNftableLbBackendIPv4 returns the IPv4 of the NIC that sits in gwVpc, i.e. the address
// the vpc-nat-gw (which lives in gwVpc) can actually DNAT to. A single-NIC backend in the
// gateway VPC matches on its only NIC. When a backend has several NICs in the gateway VPC the
// lowest IPv4 is chosen deterministically: a dual-NIC backend (one default-VPC NIC for
// kube-proxy + one gateway-VPC NIC for the gateway) covers ~99% of cases and has exactly one
// match, so per-NIC selection is intentionally not exposed.
func selectNftableLbBackendIPv4(candidates []nftableLbNicCandidate, gwVpc string) (string, bool) {
	var matches []string
	for _, candidate := range candidates {
		if candidate.ipv4 != "" && candidate.vpc == gwVpc {
			matches = append(matches, candidate.ipv4)
		}
	}
	if len(matches) == 0 {
		return "", false
	}
	slices.Sort(matches)
	return matches[0], true
}

// nftableLbBackendResolver returns a resolver mapping a ready endpoint to the single IPv4 the
// gateway must DNAT to. For a multi-NIC backend it selects the NIC in the gateway's VPC
// (gwVpc); a backend that has kube-ovn NICs but none in gwVpc is unreachable from the gateway
// and is skipped (with a one-shot Warning event) to avoid a black-hole share DNAT. An endpoint
// whose Pod target no longer exists is skipped for the same reason; only endpoints without a
// Pod target (e.g. externally maintained EndpointSlices) fall back to their primary IPv4.
func (c *Controller) nftableLbBackendResolver(svc *v1.Service, gwVpc string) func(discoveryv1.Endpoint) (string, bool) {
	type resolved struct {
		ip string
		ok bool
	}
	// Cache one result per backend pod: a pod appears once per Service port, so memoizing
	// both avoids repeated lister traversals and emits at most one skip event per pod.
	cache := make(map[string]resolved)
	return func(ep discoveryv1.Endpoint) (string, bool) {
		primary := firstIPv4(ep.Addresses)
		if gwVpc == "" {
			return primary, primary != ""
		}
		pod := c.nftableLbEndpointPod(ep, svc.Namespace)
		if pod == nil {
			// An endpoint that still references a Pod which no longer exists must not fall
			// back to its recorded address: the backend is gone and DNATing to it would black
			// hole traffic (kube-ovn removes automatic EndpointSlices entries, but manually
			// maintained EndpointSlices without a Service selector are not updated for us).
			// Endpoints without a Pod target still fall back to their primary IPv4.
			if ep.TargetRef != nil && ep.TargetRef.Kind == "Pod" && ep.TargetRef.Name != "" {
				return "", false
			}
			return primary, primary != ""
		}
		podKey := pod.Namespace + "/" + pod.Name
		if r, done := cache[podKey]; done {
			return r.ip, r.ok
		}

		// default to the primary endpoint IP (best effort for non-kube-ovn pods)
		ip, ok := primary, primary != ""
		candidates := c.nftableLbPodNicCandidates(pod)
		if selected, matched := selectNftableLbBackendIPv4(candidates, gwVpc); matched {
			ip, ok = selected, true
		} else if len(candidates) > 0 {
			// has kube-ovn NICs but none in the gateway VPC: unreachable from the gateway
			ip, ok = "", false
			c.recorder.Eventf(svc, v1.EventTypeWarning, "NftableLbSvcBackendSkipped",
				"backend pod %s has no NIC in gateway VPC %q; skipping it to avoid an unreachable share DNAT", podKey, gwVpc)
		}
		cache[podKey] = resolved{ip, ok}
		return ip, ok
	}
}

// nftableLbEndpointPod returns the backend Pod referenced by an endpoint, or nil when the
// endpoint has no Pod target or the pod is not in cache.
func (c *Controller) nftableLbEndpointPod(ep discoveryv1.Endpoint, defaultNamespace string) *v1.Pod {
	if ep.TargetRef == nil || ep.TargetRef.Kind != "Pod" || ep.TargetRef.Name == "" {
		return nil
	}
	namespace := ep.TargetRef.Namespace
	if namespace == "" {
		namespace = defaultNamespace
	}
	pod, err := c.podsLister.Pods(namespace).Get(ep.TargetRef.Name)
	if err != nil {
		return nil
	}
	return pod
}

// nftableLbPodNicCandidates returns one candidate per resolvable kube-ovn NIC of the pod
// (primary and attached), pairing the NIC's IPv4 with the VPC of its subnet.
func (c *Controller) nftableLbPodNicCandidates(pod *v1.Pod) []nftableLbNicCandidate {
	providers, err := c.getPodProviders(pod)
	if err != nil {
		klog.Warningf("failed to get providers for backend pod %s/%s: %v", pod.Namespace, pod.Name, err)
		return nil
	}
	var candidates []nftableLbNicCandidate
	for _, provider := range providers {
		subnetName, err := getSubnetByProvider(pod, provider)
		if err != nil {
			continue
		}
		subnet, err := c.subnetsLister.Get(subnetName)
		if err != nil {
			continue
		}
		ips := strings.Split(pod.Annotations[fmt.Sprintf(util.IPAddressAnnotationTemplate, provider)], ",")
		candidates = append(candidates, nftableLbNicCandidate{
			ipv4: firstIPv4(ips),
			vpc:  subnet.Spec.Vpc,
		})
	}
	return candidates
}

// firstIPv4 returns the first IPv4 address in the list, or "" when there is none.
func firstIPv4(addresses []string) string {
	for _, address := range addresses {
		if util.CheckProtocol(address) == kubeovnv1.ProtocolIPv4 {
			return address
		}
	}
	return ""
}

// nftableLbDnatRuleName builds a deterministic, DNS-1123 compliant name that uniquely
// encodes the record identity. The name is "lb-<sanitized svc name>-<12 hex hash>"; the
// svc name portion is truncated so the total length never exceeds 63 characters.
func nftableLbDnatRuleName(namespace, name, protocol, externalPort, backendIP, internalPort string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{namespace, name, protocol, externalPort, backendIP, internalPort}, "/")))
	hash := hex.EncodeToString(sum[:])[:12]

	const prefix = "lb-"
	// reserve room for prefix, the "-" separator and the 12-char hash
	maxNameLen := 63 - len(prefix) - 1 - len(hash)
	svcPart := name
	if len(svcPart) > maxNameLen {
		svcPart = svcPart[:maxNameLen]
	}
	return fmt.Sprintf("%s%s-%s", prefix, svcPart, hash)
}
