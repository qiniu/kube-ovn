package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	kubeovnv1 "github.com/kubeovn/kube-ovn/pkg/apis/kubeovn/v1"
	"github.com/kubeovn/kube-ovn/pkg/util"

	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/set"

	kubeovnlister "github.com/kubeovn/kube-ovn/pkg/client/listers/kubeovn/v1"
)

// Test_enqueueDelIptablesDnatRule_wakesOwnerService pins the record-loss wake-up: deleting a
// Service accounting record out from under its owner re-enqueues that Service so its reconcile
// restores the claim, while non-accounting share records and exclusive rules keep their existing
// behavior.
func Test_enqueueDelIptablesDnatRule_wakesOwnerService(t *testing.T) {
	t.Parallel()

	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	t.Cleanup(queue.ShutDown)
	delQueue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	t.Cleanup(delQueue.ShutDown)
	c := &Controller{
		config:                         &Configuration{EnableGwNftableLbSvc: true, EnableGwNftableSvcClusterIP: true},
		addOrUpdateGwNftableLbSvcQueue: queue,
		delIptablesDnatRuleQueue:       delQueue,
	}

	c.enqueueDelIptablesDnatRule(&kubeovnv1.IptablesDnatRule{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc-record",
			Labels: map[string]string{
				util.NftableLbSvcNsLabel: "default", util.NftableLbSvcNameLabel: "web", util.NftableLbSvcRecordLabel: "true",
			},
		}, Spec: kubeovnv1.IptablesDnatRuleSpec{Type: kubeovnv1.DnatRuleTypeShare, EIP: "eip0"},
	})
	require.Equal(t, 1, queue.Len())
	key, _ := queue.Get()
	require.Equal(t, "default/web", key)
	queue.Done(key)

	// a hand-managed share record (no Service owner) only re-checks the EIP
	c.enqueueDelIptablesDnatRule(&kubeovnv1.IptablesDnatRule{
		ObjectMeta: metav1.ObjectMeta{Name: "manual"},
		Spec:       kubeovnv1.IptablesDnatRuleSpec{Type: kubeovnv1.DnatRuleTypeShare, EIP: "eip0"},
	})
	// an exclusive rule goes to its own delete queue, never to Services
	c.enqueueDelIptablesDnatRule(&kubeovnv1.IptablesDnatRule{
		ObjectMeta: metav1.ObjectMeta{Name: "exclusive"},
		Spec:       kubeovnv1.IptablesDnatRuleSpec{Type: kubeovnv1.DnatRuleTypeExclusive, EIP: "eip0"},
	})
	require.Equal(t, 0, queue.Len(), "only Service accounting records wake their owner Service")
}

// Test_handleAddOrUpdateGwNftableLbService_restoresDeletedRecords pins the recovery half of the
// accounting rule: after every accounting record of a Service is deleted out from under it, the
// next reconcile re-claims the records before it touches anything else, so claims never go
// missing for longer than one reconcile.
func Test_handleAddOrUpdateGwNftableLbService_restoresDeletedRecords(t *testing.T) {
	f := newNftableLbSvcOwnershipFixture()
	f.svc.Finalizers = []string{util.KubeOVNControllerFinalizer}

	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		Vpcs:           []*kubeovnv1.Vpc{f.vpc},
		VpcNatGateways: []*kubeovnv1.VpcNatGateway{f.gw},
		Subnets:        []*kubeovnv1.Subnet{f.subnet},
		Services:       []*v1.Service{f.svc},
		EndpointSlices: []*discoveryv1.EndpointSlice{f.slice},
		IptablesEIPs:   []*kubeovnv1.IptablesEIP{f.eip},
		Pods:           gatewayPods(f.gw.Name, "10.0.7.254"),
	})
	require.NoError(t, err)
	c := fc.fakeController
	c.config.EnableGwNftableLbSvc = true
	c.config.EnableGwNftableSvcClusterIP = true
	c.addOrUpdateGwNftableLbSvcQueue = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	t.Cleanup(c.addOrUpdateGwNftableLbSvcQueue.ShutDown)
	c.resetIptablesEipQueue = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	t.Cleanup(c.resetIptablesEipQueue.ShutDown)
	c.execRulesInPod = func(_ *v1.Pod, _ string, _ []string) error { return nil }
	fc.mockOvnClient.EXPECT().ListLogicalRouterPolicies(util.DefaultVpc, util.NatGatewayVipPolicyPriority,
		natGwVipRouteExternalIDs(f.gw.Name), false).Return(nil, nil).AnyTimes()
	fc.mockOvnClient.EXPECT().AddLogicalRouterPolicy(util.DefaultVpc, util.NatGatewayVipPolicyPriority, gomock.Any(),
		string(kubeovnv1.PolicyRouteActionReroute), []string{"10.0.7.254"}, nil, gomock.Any()).Return(nil).AnyTimes()

	owner := f.namespace + "/" + f.svc.Name
	require.NoError(t, c.handleAddOrUpdateGwNftableLbService(owner))
	claimed, err := c.config.KubeOvnClient.KubeovnV1().IptablesDnatRules().List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, claimed.Items, "the service claims its records on reconcile")

	// Delete every account record of the service, as an accident or a manual cleanup would, and
	// wait until the informer observes the loss.
	for _, item := range claimed.Items {
		require.NoError(t, c.config.KubeOvnClient.KubeovnV1().IptablesDnatRules().Delete(context.Background(), item.Name, metav1.DeleteOptions{}))
	}
	require.Eventually(t, func() bool {
		rules, err := c.iptablesDnatRulesLister.List(labels.Everything())
		return err == nil && len(rules) == 0
	}, 5*time.Second, 10*time.Millisecond, "the informer must observe the record deletion")

	require.NoError(t, c.handleAddOrUpdateGwNftableLbService(owner))
	restored, err := c.config.KubeOvnClient.KubeovnV1().IptablesDnatRules().List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, restored.Items, len(claimed.Items), "the reconcile restores the deleted claims")
	seen := set.New[string]()
	for _, item := range restored.Items {
		seen.Insert(item.Name)
	}
	for _, item := range claimed.Items {
		require.True(t, seen.Has(item.Name), "record %s must be re-claimed", item.Name)
	}
}

// Test_nftableLbSvcIntentClaimsEip pins the EIP-release hold: while a live Service still declares
// share DNAT on a wired EIP (and could restore its deleted records), the EIP's finalizer must not
// be released; the hold lifts for unready EIPs, terminating Services and missing gateways, where
// no claim can exist.
func Test_nftableLbSvcIntentClaimsEip(t *testing.T) {
	t.Parallel()

	const gwName = "gw0"
	newSvcIndexer := func(objs ...*v1.Service) cache.Indexer {
		indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
			IndexGwNftableLbServiceByEip: indexGwNftableLbServiceByEip,
		})
		for _, obj := range objs {
			require.NoError(t, indexer.Add(obj))
		}
		return indexer
	}
	newGwLister := func(gws ...*kubeovnv1.VpcNatGateway) kubeovnlister.VpcNatGatewayLister {
		indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
		for _, gw := range gws {
			require.NoError(t, indexer.Add(gw))
		}
		return kubeovnlister.NewVpcNatGatewayLister(indexer)
	}
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "web",
			Annotations: map[string]string{util.EipAnnotation: "eip0", util.VpcNatGatewayAnnotation: gwName},
		}, Spec: v1.ServiceSpec{
			Type:       v1.ServiceTypeLoadBalancer,
			ClusterIP:  "10.96.1.5",
			ClusterIPs: []string{"10.96.1.5"},
			Ports:      []v1.ServicePort{{Name: "http", Port: 80, Protocol: v1.ProtocolTCP}},
		},
	}
	gw := &kubeovnv1.VpcNatGateway{ObjectMeta: metav1.ObjectMeta{Name: gwName}}
	newController := func(svcIndexer cache.Indexer, gwLister kubeovnlister.VpcNatGatewayLister) *Controller {
		return &Controller{
			config:              &Configuration{EnableGwNftableLbSvc: true, EnableGwNftableSvcClusterIP: true},
			svcIndexer:          svcIndexer,
			vpcNatGatewayLister: gwLister,
		}
	}
	readyEip := &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{Name: "eip0"},
		Status:     kubeovnv1.IptablesEIPStatus{IP: "203.0.113.10", Ready: true},
	}

	t.Run("holds while a live service declares the wired eip", func(t *testing.T) {
		c := newController(newSvcIndexer(svc), newGwLister(gw))
		require.True(t, c.nftableLbSvcIntentClaimsEip(readyEip))
	})
	t.Run("unready eip holds no claim", func(t *testing.T) {
		unready := readyEip.DeepCopy()
		unready.Status.Ready = false
		c := newController(newSvcIndexer(svc), newGwLister(gw))
		require.False(t, c.nftableLbSvcIntentClaimsEip(unready))
	})
	t.Run("no referencing service", func(t *testing.T) {
		c := newController(newSvcIndexer(), newGwLister(gw))
		require.False(t, c.nftableLbSvcIntentClaimsEip(readyEip))
	})
	t.Run("terminating service", func(t *testing.T) {
		dying := svc.DeepCopy()
		dying.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		c := newController(newSvcIndexer(dying), newGwLister(gw))
		require.False(t, c.nftableLbSvcIntentClaimsEip(readyEip))
	})
	t.Run("missing gateway claims nothing", func(t *testing.T) {
		c := newController(newSvcIndexer(svc), newGwLister())
		require.False(t, c.nftableLbSvcIntentClaimsEip(readyEip))
	})
}

// Test_handleUpdateIptablesEip_holdsWhileServiceClaims pins the release-path wiring of the hold:
// with every accounting record of the claiming Service deleted, the EIP reconcile still keeps its
// finalizer until no live Service declares the EIP any more.
func Test_handleUpdateIptablesEip_holdsWhileServiceClaims(t *testing.T) {
	f := newNftableLbSvcOwnershipFixture()
	terminating := metav1.Time{Time: time.Now()}
	eip := &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{
			Name: "owned-eip", UID: "owned-eip-uid",
			Finalizers:        []string{util.KubeOVNControllerFinalizer},
			DeletionTimestamp: &terminating,
		},
		Spec:   kubeovnv1.IptablesEIPSpec{NatGwDp: f.gw.Name, V4ip: "172.20.0.5"},
		Status: kubeovnv1.IptablesEIPStatus{IP: "172.20.0.5", Ready: true},
	}

	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		Vpcs:           []*kubeovnv1.Vpc{f.vpc},
		VpcNatGateways: []*kubeovnv1.VpcNatGateway{f.gw},
		Subnets:        []*kubeovnv1.Subnet{f.subnet},
		Services:       []*v1.Service{f.svc},
		IptablesEIPs:   []*kubeovnv1.IptablesEIP{eip},
	})
	require.NoError(t, err)
	c := fc.fakeController
	c.config.EnableGwNftableLbSvc = true
	c.config.EnableGwNftableSvcClusterIP = true
	c.updateIptablesEipQueue = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	t.Cleanup(c.updateIptablesEipQueue.ShutDown)

	// The Service's records are gone (deleted out from under it), so the API read finds no rule.
	require.NoError(t, c.handleUpdateIptablesEip("owned-eip"))
	kept, err := c.config.KubeOvnClient.KubeovnV1().IptablesEIPs().Get(context.Background(), "owned-eip", metav1.GetOptions{})
	require.NoError(t, err)
	require.Contains(t, kept.Finalizers, util.KubeOVNControllerFinalizer,
		"the eip finalizer must hold while a live service still declares the eip")
}
