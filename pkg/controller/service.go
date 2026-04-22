package controller

import (
	"context"
	"fmt"
	"maps"
	"net"
	"reflect"
	"slices"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/kubeovn/kube-ovn/pkg/util"
)

type vpcService struct {
	Vips     []string
	Vpc      string
	Protocol v1.Protocol
	Svc      *v1.Service
}

type updateSvcObject struct {
	key      string
	oldPorts []v1.ServicePort
	newPorts []v1.ServicePort
}

func (c *Controller) enqueueAddService(obj any) {
	svc := obj.(*v1.Service)
	key := cache.MetaObjectToName(svc).String()
	klog.V(3).Infof("enqueue add endpoint %s", key)
	c.addOrUpdateEndpointSliceQueue.Add(key)

	if c.config.EnableLbSvc || c.config.EnableBgpLbVip {
		klog.V(3).Infof("enqueue add service %s for lb processing", key)
		c.addServiceQueue.Add(key)
	}
}

func (c *Controller) enqueueDeleteService(obj any) {
	var svc *v1.Service
	switch t := obj.(type) {
	case *v1.Service:
		svc = t
	case cache.DeletedFinalStateUnknown:
		s, ok := t.Obj.(*v1.Service)
		if !ok {
			klog.Warningf("unexpected object type: %T", t.Obj)
			return
		}
		svc = s
	default:
		klog.Warningf("unexpected type: %T", obj)
		return
	}

	klog.Infof("enqueue delete service %s/%s", svc.Namespace, svc.Name)

	vip, ok := svc.Annotations[util.SwitchLBRuleVipsAnnotation]
	if ok || svc.Spec.ClusterIP != v1.ClusterIPNone && svc.Spec.ClusterIP != "" || svc.Annotations[util.ServiceExternalIPFromSubnetAnnotation] != "" {
		if c.config.EnableNP {
			netpols, err := c.svcMatchNetworkPolicies(svc)
			if err != nil {
				utilruntime.HandleError(err)
				return
			}

			for _, np := range netpols {
				c.updateNpQueue.Add(np)
			}
		}

		ips := util.ServiceClusterIPs(*svc)
		if ok {
			ips = strings.Split(vip, ",")
		}

		if svc.Annotations[util.ServiceExternalIPFromSubnetAnnotation] != "" {
			for _, ingress := range svc.Status.LoadBalancer.Ingress {
				ips = append(ips, ingress.IP)
			}
		}

		for _, port := range svc.Spec.Ports {
			vpcSvc := &vpcService{
				Protocol: port.Protocol,
				Vpc:      svc.Annotations[util.VpcAnnotation],
				Svc:      svc,
			}
			for _, ip := range ips {
				vpcSvc.Vips = append(vpcSvc.Vips, util.JoinHostPort(ip, port.Port))
			}
			klog.V(3).Infof("delete vpc service: %v", vpcSvc)
			c.deleteServiceQueue.Add(vpcSvc)
		}
	}
}

func (c *Controller) enqueueUpdateService(oldObj, newObj any) {
	oldSvc := oldObj.(*v1.Service)
	newSvc := newObj.(*v1.Service)
	if oldSvc.ResourceVersion == newSvc.ResourceVersion {
		return
	}

	oldClusterIps := getVipIps(oldSvc)
	newClusterIps := getVipIps(newSvc)
	var ipsToDel []string
	for _, oldClusterIP := range oldClusterIps {
		if !slices.Contains(newClusterIps, oldClusterIP) {
			ipsToDel = append(ipsToDel, oldClusterIP)
		}
	}

	key := cache.MetaObjectToName(newSvc).String()
	klog.V(3).Infof("enqueue update service %s", key)
	if len(ipsToDel) != 0 {
		ipsToDelStr := strings.Join(ipsToDel, ",")
		key = strings.Join([]string{key, ipsToDelStr}, "#")
	}

	updateSvc := &updateSvcObject{
		key:      key,
		oldPorts: oldSvc.Spec.Ports,
		newPorts: newSvc.Spec.Ports,
	}
	c.updateServiceQueue.Add(updateSvc)
}

func (c *Controller) handleDeleteService(service *vpcService) error {
	key := cache.MetaObjectToName(service.Svc).String()

	c.svcKeyMutex.LockKey(key)
	defer func() { _ = c.svcKeyMutex.UnlockKey(key) }()
	klog.Infof("handle delete service %s", key)

	// OVN LB VIP cleanup is only relevant when the classic OVN LB mode is active.
	// When EnableLb=false (e.g. EnableBgpLbVip-only mode), no OVN LB objects are
	// created, so attempting to delete from them would always fail with "not found
	// load balancer" and cause an infinite retry loop.
	if c.config.EnableLb {
		svcs, err := c.servicesLister.Services(v1.NamespaceAll).List(labels.Everything())
		if err != nil {
			klog.Errorf("failed to list svc, %v", err)
			return err
		}
		var (
			vpcLB             [2]string
			vpcLbConfig       = c.GenVpcLoadBalancer(service.Vpc)
			ignoreHealthCheck = true
		)

		switch service.Protocol {
		case v1.ProtocolTCP:
			vpcLB = [2]string{vpcLbConfig.TCPLoadBalancer, vpcLbConfig.TCPSessLoadBalancer}
		case v1.ProtocolUDP:
			vpcLB = [2]string{vpcLbConfig.UDPLoadBalancer, vpcLbConfig.UDPSessLoadBalancer}
		case v1.ProtocolSCTP:
			vpcLB = [2]string{vpcLbConfig.SctpLoadBalancer, vpcLbConfig.SctpSessLoadBalancer}
		}

		for _, vip := range service.Vips {
			var (
				ip    string
				found bool
			)
			ip = parseVipAddr(vip)

			for _, svc := range svcs {
				if slices.Contains(util.ServiceClusterIPs(*svc), ip) {
					found = true
					break
				}
			}
			if found {
				continue
			}

			for _, lb := range vpcLB {
				if c.config.EnableOVNLBPreferLocal {
					if err = c.OVNNbClient.LoadBalancerDeleteIPPortMapping(lb, vip); err != nil {
						klog.Errorf("failed to delete ip port mapping for vip %s from LB %s: %v", vip, lb, err)
						return err
					}
				}

				if err = c.OVNNbClient.LoadBalancerDeleteVip(lb, vip, ignoreHealthCheck); err != nil {
					klog.Errorf("failed to delete vip %s from LB %s: %v", vip, lb, err)
					return err
				}
			}
		}
	}

	if service.Svc.Spec.Type == v1.ServiceTypeLoadBalancer && c.config.EnableLbSvc {
		if err := c.deleteLbSvc(service.Svc); err != nil {
			klog.Errorf("failed to delete service %s, %v", service.Svc.Name, err)
			return err
		}
	}

	if service.Svc.Spec.Type == v1.ServiceTypeLoadBalancer && c.config.EnableBgpLbVip {
		if err := c.cleanBgpLbVipService(service.Svc); err != nil {
			klog.Errorf("failed to clean bgp-lb-vip for service %s: %v", service.Svc.Name, err)
			return err
		}
	}

	return nil
}

func (c *Controller) handleUpdateService(svcObject *updateSvcObject) error {
	key := svcObject.key
	keys := strings.Split(key, "#")
	key = keys[0]
	var ipsToDel []string
	if len(keys) == 2 {
		ipsToDelStr := keys[1]
		ipsToDel = strings.Split(ipsToDelStr, ",")
	}

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		klog.Error(err)
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	c.svcKeyMutex.LockKey(key)
	defer func() { _ = c.svcKeyMutex.UnlockKey(key) }()
	klog.Infof("handle update service %s", key)

	svc, err := c.servicesLister.Services(namespace).Get(name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		klog.Error(err)
		return err
	}

	ips := getVipIps(svc)

	vpcName := svc.Annotations[util.VpcAnnotation]
	if vpcName == "" {
		vpcName = c.config.ClusterRouter
	}
	vpc, err := c.vpcsLister.Get(vpcName)
	if err != nil {
		klog.Errorf("failed to get vpc %s of lb, %v", vpcName, err)
		return err
	}

	tcpLb, udpLb, sctpLb := vpc.Status.TCPLoadBalancer, vpc.Status.UDPLoadBalancer, vpc.Status.SctpLoadBalancer
	oTCPLb, oUDPLb, oSctpLb := vpc.Status.TCPSessionLoadBalancer, vpc.Status.UDPSessionLoadBalancer, vpc.Status.SctpSessionLoadBalancer
	if svc.Spec.SessionAffinity == v1.ServiceAffinityClientIP {
		tcpLb, udpLb, sctpLb, oTCPLb, oUDPLb, oSctpLb = oTCPLb, oUDPLb, oSctpLb, tcpLb, udpLb, sctpLb
	}

	var tcpVips, udpVips, sctpVips []string
	for _, port := range svc.Spec.Ports {
		for _, ip := range ips {
			switch port.Protocol {
			case v1.ProtocolTCP:
				tcpVips = append(tcpVips, util.JoinHostPort(ip, port.Port))
			case v1.ProtocolUDP:
				udpVips = append(udpVips, util.JoinHostPort(ip, port.Port))
			case v1.ProtocolSCTP:
				sctpVips = append(sctpVips, util.JoinHostPort(ip, port.Port))
			}
		}
	}

	var (
		needUpdateEndpointQueue = false
		ignoreHealthCheck       = true
	)

	// for service update
	updateVip := func(lbName, oLbName string, svcVips []string) error {
		if len(lbName) == 0 {
			return nil
		}

		lb, err := c.OVNNbClient.GetLoadBalancer(lbName, false)
		if err != nil {
			klog.Errorf("failed to get LB %s: %v", lbName, err)
			return err
		}
		lbVIPs := maps.Clone(lb.Vips)
		klog.V(3).Infof("existing vips of LB %s: %v", lbName, lbVIPs)
		for _, vip := range svcVips {
			if err := c.OVNNbClient.LoadBalancerDeleteVip(oLbName, vip, ignoreHealthCheck); err != nil {
				klog.Errorf("failed to delete vip %s from LB %s: %v", vip, oLbName, err)
				return err
			}

			if _, ok := lbVIPs[vip]; !ok {
				klog.Infof("add vip %s to LB %s", vip, lbName)
				needUpdateEndpointQueue = true
			}
		}
		for vip := range lbVIPs {
			if ip := parseVipAddr(vip); (slices.Contains(ips, ip) && !slices.Contains(svcVips, vip)) || slices.Contains(ipsToDel, ip) {
				klog.Infof("remove stale vip %s from LB %s", vip, lbName)
				if err := c.OVNNbClient.LoadBalancerDeleteVip(lbName, vip, ignoreHealthCheck); err != nil {
					klog.Errorf("failed to delete vip %s from LB %s: %v", vip, lbName, err)
					return err
				}
			}
		}

		if len(oLbName) == 0 {
			return nil
		}

		oLb, err := c.OVNNbClient.GetLoadBalancer(oLbName, false)
		if err != nil {
			klog.Errorf("failed to get LB %s: %v", oLbName, err)
			return err
		}
		oLbVIPs := maps.Clone(oLb.Vips)
		klog.V(3).Infof("existing vips of LB %s: %v", oLbName, oLbVIPs)
		for vip := range oLbVIPs {
			if ip := parseVipAddr(vip); slices.Contains(ips, ip) || slices.Contains(ipsToDel, ip) {
				klog.Infof("remove stale vip %s from LB %s", vip, oLbName)
				if err = c.OVNNbClient.LoadBalancerDeleteVip(oLbName, vip, ignoreHealthCheck); err != nil {
					klog.Errorf("failed to delete vip %s from LB %s: %v", vip, oLbName, err)
					return err
				}
			}
		}
		return nil
	}

	if err = updateVip(tcpLb, oTCPLb, tcpVips); err != nil {
		klog.Error(err)
		return err
	}
	if err = updateVip(udpLb, oUDPLb, udpVips); err != nil {
		klog.Error(err)
		return err
	}
	if err = updateVip(sctpLb, oSctpLb, sctpVips); err != nil {
		klog.Error(err)
		return err
	}

	if err := c.checkServiceLBIPBelongToSubnet(svc); err != nil {
		klog.Error(err)
		return err
	}

	if needUpdateEndpointQueue {
		c.addOrUpdateEndpointSliceQueue.Add(key)
	}
	// add the svc key which has the same vip
	vip, ok := svc.Annotations[util.SwitchLBRuleVipsAnnotation]
	if ok {
		allSlrs, err := c.switchLBRuleLister.List(labels.Everything())
		if err != nil {
			klog.Error(err)
			return err
		}
		for _, slr := range allSlrs {
			if slr.Spec.Vip == vip {
				slrKey := fmt.Sprintf("%s/slr-%s", slr.Spec.Namespace, slr.Name)
				c.addOrUpdateEndpointSliceQueue.Add(slrKey)
			}
		}
	}

	if c.config.EnableLbSvc && svc.Spec.Type == v1.ServiceTypeLoadBalancer {
		changed, err := c.checkLbSvcDeployAnnotationChanged(svc)
		if err != nil {
			klog.Errorf("failed to check annotation change for lb svc %s: %v", key, err)
			return err
		}

		// only process svc.spec.ports update
		if !changed {
			klog.Infof("update loadbalancer service %s", key)
			pod, err := c.getLbSvcPod(name, namespace)
			if err != nil {
				klog.Errorf("failed to get pod for lb svc %s: %v", key, err)
				if strings.Contains(err.Error(), "not found") {
					return nil
				}
				return err
			}

			toDel := diffSvcPorts(svcObject.oldPorts, svcObject.newPorts)
			if err := c.delDnatRules(pod, toDel, svc); err != nil {
				klog.Errorf("failed to delete dnat rules, err: %v", err)
				return err
			}
			if err = c.updatePodAttachNets(pod, svc); err != nil {
				klog.Errorf("failed to update pod attachment network for lb svc %s: %v", key, err)
				return err
			}
		}
	}

	if c.config.EnableBgpLbVip && svc.Spec.Type == v1.ServiceTypeLoadBalancer {
		if c.needCleanupBgpLbVipServiceBinding(svc) {
			if err = c.cleanupBgpLbVipServiceBinding(svc); err != nil {
				klog.Errorf("failed to cleanup bgp-lb-vip binding for service %s: %v", key, err)
				return err
			}
			return nil
		}

		needReconcile, err := c.needReconcileBgpLbVipService(svc)
		if err != nil {
			klog.Errorf("failed to check bgp-lb-vip reconcile precondition for service %s: %v", key, err)
			return err
		}
		if needReconcile {
			if err = c.reconcileBgpLbVipServiceLocked(key, svc); err != nil {
				klog.Errorf("failed to reconcile bgp-lb-vip for service %s: %v", key, err)
				return err
			}
		}
	}

	return nil
}

// Parse key of map, [fd00:10:96::11c9]:10665 for example
func parseVipAddr(vip string) string {
	host, _, err := net.SplitHostPort(vip)
	if err != nil {
		klog.Errorf("failed to parse vip %q: %v", vip, err)
		return ""
	}
	return host
}

func (c *Controller) handleAddService(key string) error {
	if c.config.EnableBgpLbVip {
		// enable-bgp-lb-vip and enable-lb-svc are mutually exclusive modes.
		// When bgp-lb-vip is on, we handle the EIP binding and return; the LB-SVC
		// pod-based flow is intentionally skipped.
		klog.Infof("dispatch add service %s to bgp-lb-vip handler", key)
		return c.handleAddBgpLbVipService(key)
	}
	if !c.config.EnableLbSvc {
		return nil
	}

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		klog.Error(err)
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	c.svcKeyMutex.LockKey(key)
	defer func() { _ = c.svcKeyMutex.UnlockKey(key) }()
	klog.Infof("handle add service %s", key)

	svc, err := c.servicesLister.Services(namespace).Get(name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		klog.Error(err)
		return err
	}
	if svc.Spec.Type != v1.ServiceTypeLoadBalancer {
		return nil
	}
	// Skip non kube-ovn lb-svc.
	if _, ok := svc.Annotations[util.AttachmentProvider]; !ok {
		return nil
	}

	klog.Infof("handle add loadbalancer service %s", key)

	if err = c.validateSvc(svc); err != nil {
		c.recorder.Event(svc, v1.EventTypeWarning, "ValidateSvcFailed", err.Error())
		klog.Errorf("failed to validate lb svc %s: %v", key, err)
		return err
	}

	nad, err := c.getAttachNetworkForService(svc)
	if err != nil {
		c.recorder.Event(svc, v1.EventTypeWarning, "GetNADFailed", err.Error())
		klog.Errorf("failed to check attachment network of lb svc %s: %v", key, err)
		return err
	}

	if err = c.createLbSvcPod(svc, nad); err != nil {
		klog.Errorf("failed to create lb svc pod for %s: %v", key, err)
		return err
	}

	var pod *v1.Pod
	for {
		pod, err = c.getLbSvcPod(name, namespace)
		if err != nil {
			klog.Warningf("pod for lb svc %s is not running: %v", key, err)
			time.Sleep(time.Second)
		}
		if pod != nil {
			break
		}

		// It's important here to check existing of svc, used to break the loop.
		_, err = c.servicesLister.Services(namespace).Get(name)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return nil
			}
			klog.Error(err)
			return err
		}
	}

	loadBalancerIP, err := c.getPodAttachIP(pod, svc)
	if err != nil {
		klog.Errorf("failed to get loadBalancerIP: %v", err)
		return err
	}

	svc, err = c.servicesLister.Services(namespace).Get(name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		klog.Error(err)
		return err
	}
	targetSvc := svc.DeepCopy()
	if err = c.updatePodAttachNets(pod, targetSvc); err != nil {
		klog.Errorf("failed to update pod attachment network for service %s/%s: %v", namespace, name, err)
		return err
	}

	// compatible with IPv4 and IPv6 dual stack subnet.
	var ingress []v1.LoadBalancerIngress
	for ip := range strings.SplitSeq(loadBalancerIP, ",") {
		if ip != "" && net.ParseIP(ip) != nil {
			ingress = append(ingress, v1.LoadBalancerIngress{IP: ip})
		}
	}
	targetSvc.Status.LoadBalancer.Ingress = ingress
	if !equality.Semantic.DeepEqual(svc.Status, targetSvc.Status) {
		if _, err = c.config.KubeClient.CoreV1().Services(namespace).
			UpdateStatus(context.Background(), targetSvc, metav1.UpdateOptions{}); err != nil {
			klog.Errorf("failed to update status of service %s/%s: %v", namespace, name, err)
			return err
		}
	}

	return nil
}

func getVipIps(svc *v1.Service) []string {
	var ips []string
	if vip, ok := svc.Annotations[util.SwitchLBRuleVipsAnnotation]; ok {
		for _, ip := range strings.Split(vip, ",") {
			if ip != "" {
				ips = append(ips, ip)
			}
		}
	} else {
		ips = util.ServiceClusterIPs(*svc)
		if svc.Annotations[util.ServiceExternalIPFromSubnetAnnotation] != "" {
			for _, ingress := range svc.Status.LoadBalancer.Ingress {
				if ingress.IP != "" {
					ips = append(ips, ingress.IP)
				}
			}
		}
	}
	return ips
}

func diffSvcPorts(oldPorts, newPorts []v1.ServicePort) (toDel []v1.ServicePort) {
	for _, oldPort := range oldPorts {
		found := false
		for _, newPort := range newPorts {
			if reflect.DeepEqual(oldPort, newPort) {
				found = true
				break
			}
		}
		if !found {
			toDel = append(toDel, oldPort)
		}
	}

	return toDel
}

func (c *Controller) checkServiceLBIPBelongToSubnet(svc *v1.Service) error {
	if svc.Spec.Type != v1.ServiceTypeLoadBalancer {
		return nil
	}
	if len(svc.Status.LoadBalancer.Ingress) == 0 {
		return nil
	}

	svc = svc.DeepCopy()
	if svc.Annotations == nil {
		svc.Annotations = map[string]string{}
	}

	origAnnotation := svc.Annotations[util.ServiceExternalIPFromSubnetAnnotation]

	subnets, err := c.subnetsLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("failed to list subnets: %v", err)
		return err
	}

	isServiceExternalIPFromSubnet := false
outer:
	for _, subnet := range subnets {
		for _, ingress := range svc.Status.LoadBalancer.Ingress {
			if util.CIDRContainIP(subnet.Spec.CIDRBlock, ingress.IP) {
				svc.Annotations[util.ServiceExternalIPFromSubnetAnnotation] = subnet.Name
				isServiceExternalIPFromSubnet = true
				break outer
			}
		}
	}

	if !isServiceExternalIPFromSubnet {
		delete(svc.Annotations, util.ServiceExternalIPFromSubnetAnnotation)
	}

	newAnnotation := svc.Annotations[util.ServiceExternalIPFromSubnetAnnotation]
	if newAnnotation == origAnnotation {
		return nil
	}

	klog.Infof("Service %s/%s external IP belongs to subnet: %v", svc.Namespace, svc.Name, isServiceExternalIPFromSubnet)
	if _, err = c.config.KubeClient.CoreV1().Services(svc.Namespace).Update(context.Background(), svc, metav1.UpdateOptions{}); err != nil {
		klog.Errorf("failed to update service %s/%s: %v", svc.Namespace, svc.Name, err)
		return err
	}

	return nil
}

// handleAddBgpLbVipService binds a pre-allocated VIP (type=bgp_lb_vip) to a
// LoadBalancer Service. The VIP is identified by the ovn.kubernetes.io/bgp-vip annotation.
// Once bound:
//   - svc.spec.externalIPs is set so kube-proxy installs DNAT rules on every node.
//   - svc.status.loadBalancer.ingress is set so the BGP speaker announces the IP.
//   - svc.annotations[ovn.kubernetes.io/bgp] is set to "true" so the speaker picks it up.
func (c *Controller) handleAddBgpLbVipService(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		klog.Error(err)
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	c.svcKeyMutex.LockKey(key)
	defer func() { _ = c.svcKeyMutex.UnlockKey(key) }()

	svc, err := c.servicesLister.Services(namespace).Get(name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		klog.Error(err)
		return err
	}
	if svc.Spec.Type != v1.ServiceTypeLoadBalancer {
		return nil
	}

	return c.reconcileBgpLbVipServiceLocked(key, svc)
}

func (c *Controller) reconcileBgpLbVipServiceLocked(key string, svc *v1.Service) error {
	if svc.Spec.Type != v1.ServiceTypeLoadBalancer {
		return nil
	}

	vipName := svc.Annotations[util.BgpVipAnnotation]
	if vipName == "" {
		// Service does not request a BGP LB VIP; nothing to do.
		return nil
	}

	namespace, name := svc.Namespace, svc.Name

	klog.Infof("handle add bgp-lb-vip service %s, vip %s", key, vipName)
	klog.Infof("bgp-lb-vip service %s current state: externalIPs=%v ingress=%v bgp=%q", key, svc.Spec.ExternalIPs, svc.Status.LoadBalancer.Ingress, svc.Annotations[util.BgpAnnotation])

	vip, err := c.virtualIpsLister.Get(vipName)
	if err != nil {
		klog.Errorf("failed to get vip %s for service %s: %v", vipName, key, err)
		return err
	}
	// Only bgp_lb_vip is supported here because this path expects an IPAM-only VIP
	// that is written into Service externalIPs/ingress for speaker announcement.
	if vip.Spec.Type != util.BgpLbVip {
		return fmt.Errorf("vip %s has type %q, expected %q", vipName, vip.Spec.Type, util.BgpLbVip)
	}
	if vip.Status.V4ip == "" {
		// IP not yet allocated. enqueueUpdateVirtualIP in vip.go will re-enqueue this
		// Service once the VIP receives its IP, so return nil to avoid retry noise.
		klog.Infof("bgp-lb-vip service %s: vip %s has no IP yet, skip", key, vipName)
		return nil
	}

	vipIP := vip.Status.V4ip
	klog.Infof("bgp-lb-vip service %s resolved vip %s to ip %s", key, vipName, vipIP)

	targetSvc := svc.DeepCopy()
	if targetSvc.Annotations == nil {
		targetSvc.Annotations = make(map[string]string)
	}

	// Set externalIPs so kube-proxy installs DNAT rules on every node.
	// Use replace semantics: desired state is exactly [vipIP].
	//
	// NOTE: spec (externalIPs + bgp annotation) and status (loadBalancer.ingress) are written
	// in two separate API calls because the Kubernetes API server treats them as independent
	// sub-resources. This means a bootstrap reconcile where both are unset will produce two
	// watch events and trigger one extra (idempotent) reconcile iteration. This is intentional:
	// the second iteration hits the `!equality.Semantic.DeepEqual` guards and returns early.
	desiredExternalIPs := []string{vipIP}
	// Also ensure the BGP speaker annotation is present so collectSvcBgpPrefixes announces the IP.
	changed := !equality.Semantic.DeepEqual(svc.Spec.ExternalIPs, desiredExternalIPs) ||
		svc.Annotations[util.BgpAnnotation] != "true"
	baseSvc := svc
	if changed {
		klog.Infof("bgp-lb-vip service %s updating spec: externalIPs %v -> %v, bgp %q -> %q", key, svc.Spec.ExternalIPs, desiredExternalIPs, svc.Annotations[util.BgpAnnotation], "true")
		targetSvc.Annotations[util.BgpAnnotation] = "true"
		targetSvc.Spec.ExternalIPs = desiredExternalIPs
		updatedSvc, updateErr := c.config.KubeClient.CoreV1().Services(namespace).Update(
			context.Background(), targetSvc, metav1.UpdateOptions{},
		)
		if updateErr != nil {
			klog.Errorf("failed to update service %s/%s: %v", namespace, name, updateErr)
			return updateErr
		}
		klog.Infof("bgp-lb-vip service %s spec updated successfully", key)
		baseSvc = updatedSvc
		targetSvc = updatedSvc.DeepCopy()
	}

	// Set status.loadBalancer.ingress so the BGP speaker discovers the IP.
	ingress := []v1.LoadBalancerIngress{{IP: vipIP}}
	if !equality.Semantic.DeepEqual(baseSvc.Status.LoadBalancer.Ingress, ingress) {
		klog.Infof("bgp-lb-vip service %s updating status ingress: %v -> %v", key, baseSvc.Status.LoadBalancer.Ingress, ingress)
		targetSvc.Status.LoadBalancer.Ingress = ingress
		if _, err = c.config.KubeClient.CoreV1().Services(namespace).UpdateStatus(
			context.Background(), targetSvc, metav1.UpdateOptions{},
		); err != nil {
			klog.Errorf("failed to update status for service %s/%s: %v", namespace, name, err)
			return err
		}
		klog.Infof("bgp-lb-vip service %s status updated successfully", key)
	}

	klog.Infof("bgp-lb-vip service %s bound to vip %s (%s)", key, vipName, vipIP)
	return nil
}

func (c *Controller) needReconcileBgpLbVipService(svc *v1.Service) (bool, error) {
	if svc.Spec.Type != v1.ServiceTypeLoadBalancer {
		return false, nil
	}
	vipName := svc.Annotations[util.BgpVipAnnotation]
	if vipName == "" {
		return false, nil
	}
	key := cache.MetaObjectToName(svc).String()

	vip, err := c.virtualIpsLister.Get(vipName)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			// VIP does not exist yet (or has already been deleted). There is nothing to
			// reconcile now. When the VIP is eventually created and receives its IP, the
			// VIP update event will re-enqueue this Service via enqueueUpdateVirtualIP's
			// indexer logic (Issue 1 fix). Returning (true, nil) here would only trigger
			// a pointless reconcile that fails with NotFound and causes retry log noise.
			klog.Infof("bgp-lb-vip service %s: vip %s not found, skip reconcile", key, vipName)
			return false, nil
		}
		return false, err
	}

	if vip.Spec.Type != util.BgpLbVip {
		klog.Infof("bgp-lb-vip service %s needs reconcile: vip %s has unexpected type=%q", key, vipName, vip.Spec.Type)
		return true, nil
	}
	if vip.Status.V4ip == "" {
		// IP not yet allocated; enqueueUpdateVirtualIP in vip.go will re-enqueue
		// this Service once the VIP receives its IP, so skip here to avoid
		// a reconcile that fails immediately and adds log noise.
		klog.Infof("bgp-lb-vip service %s: vip %s has no IP yet, skip reconcile", key, vipName)
		return false, nil
	}

	desiredExternalIPs := []string{vip.Status.V4ip}
	desiredIngress := []v1.LoadBalancerIngress{{IP: vip.Status.V4ip}}

	if !equality.Semantic.DeepEqual(svc.Spec.ExternalIPs, desiredExternalIPs) {
		klog.Infof("bgp-lb-vip service %s needs reconcile: externalIPs=%v desired=%v", key, svc.Spec.ExternalIPs, desiredExternalIPs)
		return true, nil
	}
	if svc.Annotations[util.BgpAnnotation] != "true" {
		klog.Infof("bgp-lb-vip service %s needs reconcile: bgp annotation=%q desired=%q", key, svc.Annotations[util.BgpAnnotation], "true")
		return true, nil
	}
	if !equality.Semantic.DeepEqual(svc.Status.LoadBalancer.Ingress, desiredIngress) {
		klog.Infof("bgp-lb-vip service %s needs reconcile: ingress=%v desired=%v", key, svc.Status.LoadBalancer.Ingress, desiredIngress)
		return true, nil
	}

	klog.Infof("bgp-lb-vip service %s does not need reconcile", key)
	return false, nil
}

func (c *Controller) needCleanupBgpLbVipServiceBinding(svc *v1.Service) bool {
	if svc.Spec.Type != v1.ServiceTypeLoadBalancer {
		return false
	}
	if svc.Annotations[util.BgpVipAnnotation] != "" {
		return false
	}
	if svc.Annotations[util.BgpAnnotation] == "true" {
		return true
	}
	return len(svc.Status.LoadBalancer.Ingress) != 0
}

func (c *Controller) cleanupBgpLbVipServiceBindingByVip(vipName string) error {
	objs, err := c.svcByBgpVipIndexer.ByIndex(bgpVipIndexName, vipName)
	if err != nil {
		klog.Errorf("failed to index services for bgp-lb-vip %s cleanup: %v", vipName, err)
		return err
	}

	for _, obj := range objs {
		svc, ok := obj.(*v1.Service)
		if !ok {
			continue
		}
		if err := c.cleanupBgpLbVipServiceBinding(svc); err != nil {
			return err
		}
	}

	return nil
}

func (c *Controller) cleanupBgpLbVipServiceBinding(svc *v1.Service) error {
	targetSvc := svc.DeepCopy()
	if targetSvc.Annotations == nil {
		targetSvc.Annotations = make(map[string]string)
	}

	// Only clear fields owned by the controller: spec.externalIPs and the BGP
	// speaker annotation. Do NOT remove BgpVipAnnotation — that is a user-managed
	// field. If the VIP is re-created with the same name the Service must be able
	// to auto-rebind via the still-present annotation.
	specChanged := len(svc.Spec.ExternalIPs) != 0 ||
		svc.Annotations[util.BgpAnnotation] != ""

	if specChanged {
		targetSvc.Spec.ExternalIPs = nil
		delete(targetSvc.Annotations, util.BgpAnnotation)

		updatedSvc, err := c.config.KubeClient.CoreV1().Services(targetSvc.Namespace).Update(
			context.Background(), targetSvc, metav1.UpdateOptions{},
		)
		if err != nil {
			klog.Errorf("failed to cleanup service %s/%s spec for bgp-lb-vip: %v", svc.Namespace, svc.Name, err)
			return err
		}
		targetSvc = updatedSvc.DeepCopy()
	}

	if len(svc.Status.LoadBalancer.Ingress) != 0 {
		targetSvc.Status.LoadBalancer.Ingress = nil
		if _, err := c.config.KubeClient.CoreV1().Services(targetSvc.Namespace).UpdateStatus(
			context.Background(), targetSvc, metav1.UpdateOptions{},
		); err != nil {
			klog.Errorf("failed to cleanup service %s/%s status for bgp-lb-vip: %v", svc.Namespace, svc.Name, err)
			return err
		}
	}

	return nil
}

// cleanBgpLbVipService is called on Service deletion.
// The VIP CR lifecycle is managed independently by the user; no action needed here.
func (c *Controller) cleanBgpLbVipService(svc *v1.Service) error {
	if len(svc.Spec.ExternalIPs) == 0 && len(svc.Status.LoadBalancer.Ingress) == 0 {
		return nil
	}
	klog.Infof("clean bgp-lb-vip for deleted service %s/%s", svc.Namespace, svc.Name)
	// The Service object is being deleted, so kube-proxy will remove Service-related
	// forwarding state from the deleted Service spec/status. The VIP CR is managed
	// separately and remains reusable by another Service.
	return nil
}
