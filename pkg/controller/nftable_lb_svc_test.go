package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"

	kubeovnv1 "github.com/kubeovn/kube-ovn/pkg/apis/kubeovn/v1"
	"github.com/kubeovn/kube-ovn/pkg/util"
)

func Test_nftableLbSvcQualifies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svc      *v1.Service
		expected bool
	}{
		{name: "loadbalancer with gateway and eip annotations", svc: &v1.Service{
			ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{util.VpcNatGatewayAnnotation: "gw", util.EipAnnotation: "eip0"}},
			Spec:       v1.ServiceSpec{Type: v1.ServiceTypeLoadBalancer},
		}, expected: true},
		{name: "loadbalancer without gateway annotation", svc: &v1.Service{
			ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{util.EipAnnotation: "eip0"}},
			Spec:       v1.ServiceSpec{Type: v1.ServiceTypeLoadBalancer},
		}, expected: false},
		{name: "loadbalancer without eip annotation", svc: &v1.Service{
			ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{util.VpcNatGatewayAnnotation: "gw"}},
			Spec:       v1.ServiceSpec{Type: v1.ServiceTypeLoadBalancer},
		}, expected: false},
		{name: "clusterip with gateway annotation", svc: &v1.Service{
			ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{util.VpcNatGatewayAnnotation: "gw"}},
			Spec:       v1.ServiceSpec{Type: v1.ServiceTypeClusterIP},
		}, expected: true},
		{name: "clusterip without gateway annotation", svc: &v1.Service{
			Spec: v1.ServiceSpec{Type: v1.ServiceTypeClusterIP},
		}, expected: false},
		{name: "nodeport with gateway annotation", svc: &v1.Service{
			ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{util.VpcNatGatewayAnnotation: "gw"}},
			Spec:       v1.ServiceSpec{Type: v1.ServiceTypeNodePort},
		}, expected: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.expected, nftableLbSvcQualifies(tt.svc))
		})
	}
}

func TestNftableLbEventHelpersRespectFeatureGate(t *testing.T) {
	t.Parallel()

	c := &Controller{config: &Configuration{EnableLb: true}}
	require.NotPanics(t, func() {
		c.enqueueNftableLbServicesForPod(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod"}})
		c.enqueueNftableLbServicesForEIP("eip0")
		c.enqueueNftableLbServicesForNatGw("gw")
	})
}

func Test_nftableLbDnatRuleName(t *testing.T) {
	t.Parallel()

	a := nftableLbDnatRuleName("ns", "svc", "tcp", "80", "10.0.0.1", "8080")
	b := nftableLbDnatRuleName("ns", "svc", "tcp", "80", "10.0.0.1", "8080")
	require.Equal(t, a, b)
	require.NotEqual(t, a, nftableLbDnatRuleName("ns", "svc", "tcp", "80", "10.0.0.2", "8080"))

	longName := "this-is-a-very-long-service-name-that-exceeds-the-kubernetes-limit-for-sure"
	got := nftableLbDnatRuleName("ns", longName, "tcp", "80", "10.0.0.1", "8080")
	require.LessOrEqual(t, len(got), 63)
	require.Empty(t, validation.IsDNS1123Subdomain(got))
}

func Test_nftableLbBackendResolver_missingTargetPod(t *testing.T) {
	t.Parallel()

	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	c := &Controller{podsLister: corelisters.NewPodLister(indexer)}
	resolver := c.nftableLbBackendResolver(&v1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "ns"}}, "gw-vpc")

	ep := discoveryv1.Endpoint{
		Addresses: []string{"10.0.0.1"},
		TargetRef: &v1.ObjectReference{Kind: "Pod", Namespace: "ns", Name: "gone"},
	}
	ip, ok := resolver(ep)
	require.False(t, ok)
	require.Empty(t, ip)
	ip, ok = resolver(discoveryv1.Endpoint{Addresses: []string{"10.0.0.2"}})
	require.True(t, ok)
	require.Equal(t, "10.0.0.2", ip)
}

func testNftableLbBackendIP(ep discoveryv1.Endpoint) (string, bool) {
	ip := firstIPv4(ep.Addresses)
	return ip, ip != ""
}

func Test_selectNftableLbBackendIPv4(t *testing.T) {
	t.Parallel()

	ip, ok := selectNftableLbBackendIPv4([]nftableLbNicCandidate{{ipv4: "10.0.0.1", vpc: "vpc1"}}, "vpc1")
	require.True(t, ok)
	require.Equal(t, "10.0.0.1", ip)
	ip, ok = selectNftableLbBackendIPv4([]nftableLbNicCandidate{
		{ipv4: "10.16.0.5", vpc: "ovn-cluster"}, {ipv4: "192.168.0.5", vpc: "vpc1"},
	}, "vpc1")
	require.True(t, ok)
	require.Equal(t, "192.168.0.5", ip)
	ip, ok = selectNftableLbBackendIPv4([]nftableLbNicCandidate{
		{ipv4: "192.168.0.9", vpc: "vpc1"}, {ipv4: "192.168.0.3", vpc: "vpc1"},
	}, "vpc1")
	require.True(t, ok)
	require.Equal(t, "192.168.0.3", ip)
	_, ok = selectNftableLbBackendIPv4([]nftableLbNicCandidate{{ipv4: "10.16.0.5", vpc: "ovn-cluster"}}, "vpc1")
	require.False(t, ok)
	_, ok = selectNftableLbBackendIPv4([]nftableLbNicCandidate{{ipv4: "", vpc: "vpc1"}}, "vpc1")
	require.False(t, ok)
	_, ok = selectNftableLbBackendIPv4(nil, "vpc1")
	require.False(t, ok)
}

func Test_firstIPv4(t *testing.T) {
	t.Parallel()

	require.Equal(t, "10.0.0.1", firstIPv4([]string{"fd00::1", "10.0.0.1"}))
	require.Equal(t, "10.0.0.1", firstIPv4([]string{"10.0.0.1"}))
	require.Empty(t, firstIPv4([]string{"fd00::1"}))
	require.Empty(t, firstIPv4(nil))
}

func Test_buildNftableLbBackends(t *testing.T) {
	t.Parallel()

	svc := &v1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "web"}, Spec: v1.ServiceSpec{
		Ports: []v1.ServicePort{{Name: "http", Port: 80, Protocol: v1.ProtocolTCP}, {Name: "sctp", Port: 90, Protocol: v1.ProtocolSCTP}},
	}}
	endpointSlices := []*discoveryv1.EndpointSlice{{
		Ports: []discoveryv1.EndpointPort{{Name: new("http"), Port: new(int32(8080))}},
		Endpoints: []discoveryv1.Endpoint{
			{Addresses: []string{"10.0.0.1"}, Conditions: discoveryv1.EndpointConditions{Ready: new(true)}},
			{Addresses: []string{"10.0.0.2"}, Conditions: discoveryv1.EndpointConditions{Ready: new(true)}},
			{Addresses: []string{"10.0.0.3"}, Conditions: discoveryv1.EndpointConditions{Ready: new(false)}},
			{Addresses: []string{"fd00::1"}, Conditions: discoveryv1.EndpointConditions{Ready: new(true)}},
		},
	}}

	desired := buildNftableLbBackends(svc, endpointSlices, testNftableLbBackendIP)
	// SCTP port and not-ready/IPv6 backends are dropped
	require.Equal(t, map[nftableLbIdentity][]string{
		{protocol: "tcp", externalPort: "80"}: {"10.0.0.1:8080", "10.0.0.2:8080"},
	}, desired)
}

func Test_buildNftableLbBackends_ports(t *testing.T) {
	t.Parallel()

	svc := &v1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "web"}, Spec: v1.ServiceSpec{
		Ports: []v1.ServicePort{{Port: 80, Protocol: v1.ProtocolTCP}},
	}}
	unnamed := []*discoveryv1.EndpointSlice{{
		Ports:     []discoveryv1.EndpointPort{{Port: new(int32(8080))}},
		Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"10.0.0.1"}, Conditions: discoveryv1.EndpointConditions{Ready: new(true)}}},
	}}
	require.Len(t, buildNftableLbBackends(svc, unnamed, testNftableLbBackendIP), 1)

	svc.Spec.Ports[0].Name = "http"
	mismatched := []*discoveryv1.EndpointSlice{{
		Ports:     []discoveryv1.EndpointPort{{Name: new("other"), Port: new(int32(8080))}},
		Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"10.0.0.1"}, Conditions: discoveryv1.EndpointConditions{Ready: new(true)}}},
	}}
	require.Empty(t, buildNftableLbBackends(svc, mismatched, testNftableLbBackendIP))
}

func Test_nftableLbSvcSessionAffinity(t *testing.T) {
	t.Parallel()

	svc := &v1.Service{Spec: v1.ServiceSpec{
		SessionAffinity:       v1.ServiceAffinityClientIP,
		SessionAffinityConfig: &v1.SessionAffinityConfig{ClientIP: &v1.ClientIPConfig{TimeoutSeconds: new(int32(600))}},
	}}
	affinity, timeout := nftableLbSvcSessionAffinity(svc)
	require.Equal(t, kubeovnv1.DnatSessionAffinityClientIP, affinity)
	require.Equal(t, int32(600), timeout)

	svc.Spec.SessionAffinity, svc.Spec.SessionAffinityConfig = v1.ServiceAffinityNone, nil
	affinity, timeout = nftableLbSvcSessionAffinity(svc)
	require.Equal(t, kubeovnv1.DnatSessionAffinityNone, affinity)
	require.Zero(t, timeout)
}

func Test_buildNftableLbSvcRecords(t *testing.T) {
	t.Parallel()

	svc := &v1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "web"}}
	desired := map[nftableLbIdentity][]string{{protocol: "tcp", externalPort: "80"}: {"10.0.0.1:8080"}}

	// ClusterIP scenario: no EIP, external address is the ClusterIP
	records := buildNftableLbSvcRecords(svc, &nftableLbSvcTarget{gw: "gw1", externalIP: "10.96.0.10"}, desired)
	require.Len(t, records, 1)
	for _, record := range records {
		require.Empty(t, record.Spec.EIP)
		require.Equal(t, kubeovnv1.DnatRuleTypeShare, record.Spec.Type)
		require.Equal(t, "10.0.0.1", record.Spec.InternalIP)
		require.Equal(t, "8080", record.Spec.InternalPort)
		require.Equal(t, "true", record.Labels[util.NftableLbSvcRecordLabel])
		require.Equal(t, "default", record.Labels[util.NftableLbSvcNsLabel])
		require.Equal(t, "web", record.Labels[util.NftableLbSvcNameLabel])
		require.Equal(t, "10.96.0.10", record.Labels[util.EipV4IpLabel])
		require.Equal(t, "gw1", record.Labels[util.VpcNatGatewayNameLabel])
		require.NotContains(t, record.Labels, util.EipUIDLabel)
	}

	// LoadBalancer scenario: the record claims the EIP so it stays in use
	eip := &kubeovnv1.IptablesEIP{ObjectMeta: metav1.ObjectMeta{Name: "eip0", UID: "uid0"}}
	records = buildNftableLbSvcRecords(svc, &nftableLbSvcTarget{gw: "gw1", externalIP: "192.168.0.10", eip: eip}, desired)
	require.Len(t, records, 1)
	for _, record := range records {
		require.Equal(t, "eip0", record.Spec.EIP)
		require.Equal(t, "uid0", record.Labels[util.EipUIDLabel])
		require.Equal(t, "eip0", record.Annotations[util.VpcEipAnnotation])
	}
}

func Test_isNftableLbSvcRecord(t *testing.T) {
	t.Parallel()

	require.True(t, isNftableLbSvcRecord(&kubeovnv1.IptablesDnatRule{ObjectMeta: metav1.ObjectMeta{
		Labels: map[string]string{util.NftableLbSvcRecordLabel: "true"},
	}}))
	require.False(t, isNftableLbSvcRecord(&kubeovnv1.IptablesDnatRule{}))
}

func TestCleanupNftableLbServiceSkipsUnclaimedService(t *testing.T) {
	t.Parallel()

	// A LoadBalancer handled by another provider must keep its ingress IP: cleanup returns
	// before touching status when this mode never claimed it (no finalizer).
	c := &Controller{}
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "other-provider"},
		Spec:       v1.ServiceSpec{Type: v1.ServiceTypeLoadBalancer},
		Status:     v1.ServiceStatus{LoadBalancer: v1.LoadBalancerStatus{Ingress: []v1.LoadBalancerIngress{{IP: "1.2.3.4"}}}},
	}
	require.NoError(t, c.cleanupNftableLbService(svc))
	require.Equal(t, "1.2.3.4", svc.Status.LoadBalancer.Ingress[0].IP)
}

func Test_nftableLbSvcIdentities(t *testing.T) {
	t.Parallel()

	svc := &v1.Service{Spec: v1.ServiceSpec{Ports: []v1.ServicePort{
		{Port: 80, Protocol: v1.ProtocolTCP}, {Port: 53, Protocol: v1.ProtocolUDP}, {Port: 90, Protocol: v1.ProtocolSCTP},
	}}}
	require.ElementsMatch(t, []nftableLbIdentity{
		{protocol: "tcp", externalPort: "80"}, {protocol: "udp", externalPort: "53"},
	}, nftableLbSvcIdentities(svc))
}

func TestDropNftableLbConflictingIdentities(t *testing.T) {
	t.Parallel()

	newSvc := func(name string, ports ...int32) *v1.Service {
		svc := &v1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, Annotations: map[string]string{
				util.VpcNatGatewayAnnotation: "gw1", util.EipAnnotation: "eip0",
			}},
			Spec: v1.ServiceSpec{Type: v1.ServiceTypeLoadBalancer},
		}
		for _, port := range ports {
			svc.Spec.Ports = append(svc.Spec.Ports, v1.ServicePort{Port: port, Protocol: v1.ProtocolTCP})
		}
		return svc
	}
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{IndexServiceByNftableLbEip: func(obj any) ([]string, error) {
		svc, ok := obj.(*v1.Service)
		if !ok || svc.Annotations[util.EipAnnotation] == "" {
			return nil, nil
		}
		return []string{svc.Annotations[util.EipAnnotation]}, nil
	}})
	require.NoError(t, indexer.Add(newSvc("a-first", 80)))
	svc := newSvc("b-second", 80, 443)
	require.NoError(t, indexer.Add(svc))

	queue := newTypedRateLimitingQueue[string]("nftable-lb-conflict-test", nil)
	t.Cleanup(queue.ShutDown)
	c := &Controller{svcIndexer: indexer, recorder: record.NewFakeRecorder(10), addOrUpdateNftableLbSvcQueue: queue}
	target := &nftableLbSvcTarget{
		gw: "gw1", externalIP: "192.168.0.10",
		eip: &kubeovnv1.IptablesEIP{ObjectMeta: metav1.ObjectMeta{Name: "eip0"}},
	}
	desired := map[nftableLbIdentity][]string{
		{protocol: "tcp", externalPort: "80"}:  {"10.0.0.1:8080"},
		{protocol: "tcp", externalPort: "443"}: {"10.0.0.1:8443"},
	}
	require.NoError(t, c.dropNftableLbConflictingIdentities(svc, target, desired))
	// port 80 is owned by the smaller service key, port 443 stays with this service
	require.Equal(t, map[nftableLbIdentity][]string{
		{protocol: "tcp", externalPort: "443"}: {"10.0.0.1:8443"},
	}, desired)

	// the winner keeps everything it declares
	winner := newSvc("a-first", 80)
	desired = map[nftableLbIdentity][]string{{protocol: "tcp", externalPort: "80"}: {"10.0.0.2:8080"}}
	require.NoError(t, c.dropNftableLbConflictingIdentities(winner, target, desired))
	require.Len(t, desired, 1)

	// ClusterIP scenario: no EIP, nothing is contested
	desired = map[nftableLbIdentity][]string{{protocol: "tcp", externalPort: "80"}: {"10.0.0.1:8080"}}
	require.NoError(t, c.dropNftableLbConflictingIdentities(svc, &nftableLbSvcTarget{gw: "gw1", externalIP: "10.96.0.10"}, desired))
	require.Len(t, desired, 1)
}

func Test_nftableLbPodAddressesChanged(t *testing.T) {
	t.Parallel()

	pod := func(annotations map[string]string) *v1.Pod {
		return &v1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: annotations}}
	}
	base := map[string]string{"ovn.kubernetes.io/ip_address": "10.16.0.5", "ovn.kubernetes.io/allocated": "true"}
	// unrelated churn (status, other annotations) must not trigger reprogramming
	require.False(t, nftableLbPodAddressesChanged(pod(base), pod(map[string]string{
		"ovn.kubernetes.io/ip_address": "10.16.0.5", "ovn.kubernetes.io/allocated": "false",
	})))
	require.True(t, nftableLbPodAddressesChanged(pod(base), pod(map[string]string{"ovn.kubernetes.io/ip_address": "10.16.0.6"})))
	// an attached NIC appearing or disappearing counts too
	require.True(t, nftableLbPodAddressesChanged(pod(base), pod(map[string]string{
		"ovn.kubernetes.io/ip_address": "10.16.0.5", "net1.kubernetes.io/ip_address": "192.168.0.5",
	})))
	require.False(t, nftableLbPodAddressesChanged(pod(nil), pod(nil)))
}

func Test_nftableLbSvcChanged(t *testing.T) {
	t.Parallel()

	base := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{util.VpcNatGatewayAnnotation: "gw1"}},
		Spec: v1.ServiceSpec{
			Type: v1.ServiceTypeClusterIP, ClusterIPs: []string{"10.96.0.10"},
			Ports: []v1.ServicePort{{Port: 80, Protocol: v1.ProtocolTCP}},
		},
	}
	// our own status write must not trigger another reconcile
	statusOnly := base.DeepCopy()
	statusOnly.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{IP: "192.168.0.10"}}
	require.False(t, nftableLbSvcChanged(base, statusOnly))

	for _, mutate := range []func(*v1.Service){
		func(s *v1.Service) { s.Annotations[util.VpcNatGatewayAnnotation] = "gw2" },
		func(s *v1.Service) { s.Annotations[util.EipAnnotation] = "eip0" },
		func(s *v1.Service) { s.Spec.Type = v1.ServiceTypeLoadBalancer },
		func(s *v1.Service) { s.Spec.Ports[0].Port = 8080 },
		func(s *v1.Service) { s.Spec.SessionAffinity = v1.ServiceAffinityClientIP },
		func(s *v1.Service) { s.Finalizers = []string{util.NftableLbSvcFinalizer} },
		func(s *v1.Service) { s.DeletionTimestamp = &metav1.Time{Time: time.Now()} },
	} {
		changed := base.DeepCopy()
		mutate(changed)
		require.True(t, nftableLbSvcChanged(base, changed))
	}
}
