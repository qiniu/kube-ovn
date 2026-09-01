package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kubeovnv1 "github.com/kubeovn/kube-ovn/pkg/apis/kubeovn/v1"
	kubeovnfake "github.com/kubeovn/kube-ovn/pkg/client/clientset/versioned/fake"
	kubeovninformerfactory "github.com/kubeovn/kube-ovn/pkg/client/informers/externalversions"
	"github.com/kubeovn/kube-ovn/pkg/util"
)

// fakeGw returns a minimal VpcNatGateway CRD object for use in tests.
func fakeGw(name string) *kubeovnv1.VpcNatGateway {
	return &kubeovnv1.VpcNatGateway{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       kubeovnv1.VpcNatGatewaySpec{},
	}
}

func TestNatGwDeleted(t *testing.T) {
	t.Parallel()

	t.Run("gateway CRD exists returns false", func(t *testing.T) {
		fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
			VpcNatGateways: []*kubeovnv1.VpcNatGateway{fakeGw("test-gw")},
		})
		require.NoError(t, err)
		deleted, err := fc.fakeController.natGwDeleted("test-gw")
		require.NoError(t, err)
		require.False(t, deleted)
	})

	t.Run("gateway CRD missing returns true", func(t *testing.T) {
		fc, err := newFakeControllerWithOptions(t, nil)
		require.NoError(t, err)
		deleted, err := fc.fakeController.natGwDeleted("missing-gw")
		require.NoError(t, err)
		require.True(t, deleted)
	})

	t.Run("terminating gateway CRD returns true", func(t *testing.T) {
		now := metav1.Now()
		gw := fakeGw("dying-gw")
		gw.DeletionTimestamp = &now
		gw.Finalizers = []string{util.KubeOVNControllerFinalizer}
		fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
			VpcNatGateways: []*kubeovnv1.VpcNatGateway{gw},
		})
		require.NoError(t, err)
		deleted, err := fc.fakeController.natGwDeleted("dying-gw")
		require.NoError(t, err)
		require.True(t, deleted)
	})
}

// TestDeleteEipInPod_NatGwGone verifies that deleteEipInPod returns nil (skips
// cleanup) when the VpcNatGateway CRD no longer exists, allowing the EIP to
// be finalized without an infinite retry loop.
func TestDeleteEipInPod_NatGwGone(t *testing.T) {
	t.Parallel()
	fc, err := newFakeControllerWithOptions(t, nil) // no VpcNatGateway
	require.NoError(t, err)
	err = fc.fakeController.deleteEipInPod("missing-gw", "10.0.0.1/24", "kube-system")
	require.NoError(t, err, "should skip cleanup when gateway CRD is gone")
}

// TestDeleteEipInPod_NatGwExistsPodMissing verifies that deleteEipInPod returns
// an error (triggering a reconcile retry) when the VpcNatGateway CRD exists but
// its pod is not yet available (e.g., being recreated).
func TestDeleteEipInPod_NatGwExistsPodMissing(t *testing.T) {
	t.Parallel()
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		VpcNatGateways: []*kubeovnv1.VpcNatGateway{fakeGw("test-gw")},
	})
	require.NoError(t, err)
	err = fc.fakeController.deleteEipInPod("test-gw", "10.0.0.1/24", "kube-system")
	require.Error(t, err, "should return error to retry when pod is temporarily absent")
}

// TestDelEipQoSInPod_NatGwGone verifies cleanup is skipped when gateway is gone.
func TestDelEipQoSInPod_NatGwGone(t *testing.T) {
	t.Parallel()
	fc, err := newFakeControllerWithOptions(t, nil)
	require.NoError(t, err)
	err = fc.fakeController.delEipQoSInPod("missing-gw", "10.0.0.1", "kube-system", kubeovnv1.QoSDirectionIngress)
	require.NoError(t, err, "should skip cleanup when gateway CRD is gone")
}

// TestDelEipQoSInPod_NatGwExistsPodMissing verifies that an error is returned
// when the gateway CRD exists but the pod is not ready.
func TestDelEipQoSInPod_NatGwExistsPodMissing(t *testing.T) {
	t.Parallel()
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		VpcNatGateways: []*kubeovnv1.VpcNatGateway{fakeGw("test-gw")},
	})
	require.NoError(t, err)
	err = fc.fakeController.delEipQoSInPod("test-gw", "10.0.0.1", "kube-system", kubeovnv1.QoSDirectionEgress)
	require.Error(t, err, "should return error to retry when pod is temporarily absent")
}

// TestEnqueueAddIptablesEip verifies that on the add path a terminating EIP is routed to the
// update queue (which runs deletion cleanup) instead of the add queue. This is what lets a
// stuck-terminating EIP be finalized after a controller restart, where the informer re-lists
// existing objects and fires only AddFunc.
func TestEnqueueAddIptablesEip(t *testing.T) {
	t.Parallel()
	c := &Controller{
		addIptablesEipQueue:    newTypedRateLimitingQueue[string]("AddIptablesEip", nil),
		updateIptablesEipQueue: newTypedRateLimitingQueue[string]("UpdateIptablesEip", nil),
	}
	t.Cleanup(c.addIptablesEipQueue.ShutDown)
	t.Cleanup(c.updateIptablesEipQueue.ShutDown)
	now := metav1.Now()
	assertEnqueueAddRouting(t, c.addIptablesEipQueue, c.updateIptablesEipQueue, c.enqueueAddIptablesEip,
		&kubeovnv1.IptablesEIP{ObjectMeta: metav1.ObjectMeta{Name: "live-eip"}},
		&kubeovnv1.IptablesEIP{ObjectMeta: metav1.ObjectMeta{Name: "terminating-eip", DeletionTimestamp: &now}},
	)
}

func TestCheckQoSPolicyNotTerminating(t *testing.T) {
	t.Parallel()

	newController := func(t *testing.T, qosPolicies ...*kubeovnv1.QoSPolicy) *Controller {
		t.Helper()
		factory := kubeovninformerfactory.NewSharedInformerFactory(kubeovnfake.NewSimpleClientset(), 0)
		informer := factory.Kubeovn().V1().QoSPolicies()
		for _, qos := range qosPolicies {
			require.NoError(t, informer.Informer().GetStore().Add(qos))
		}
		return &Controller{qosPoliciesLister: informer.Lister()}
	}

	t.Run("empty qos policy name is allowed", func(t *testing.T) {
		require.NoError(t, newController(t).checkQoSPolicyNotTerminating(""))
	})

	t.Run("missing qos policy is rejected", func(t *testing.T) {
		err := newController(t).checkQoSPolicyNotTerminating("missing-qos")
		require.Error(t, err)
	})

	t.Run("live qos policy is allowed", func(t *testing.T) {
		qos := &kubeovnv1.QoSPolicy{ObjectMeta: metav1.ObjectMeta{Name: "live-qos"}}
		require.NoError(t, newController(t, qos).checkQoSPolicyNotTerminating("live-qos"))
	})

	t.Run("terminating qos policy is rejected", func(t *testing.T) {
		now := metav1.Now()
		qos := &kubeovnv1.QoSPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "dying-qos", DeletionTimestamp: &now},
		}
		err := newController(t, qos).checkQoSPolicyNotTerminating("dying-qos")
		require.ErrorContains(t, err, "qos policy dying-qos is terminating")
	})
}

func TestGetBindableEip(t *testing.T) {
	t.Parallel()

	newController := func(t *testing.T, gws []*kubeovnv1.VpcNatGateway, eips ...*kubeovnv1.IptablesEIP) *Controller {
		t.Helper()
		factory := kubeovninformerfactory.NewSharedInformerFactory(kubeovnfake.NewSimpleClientset(), 0)
		informer := factory.Kubeovn().V1().IptablesEIPs()
		for _, eip := range eips {
			require.NoError(t, informer.Informer().GetStore().Add(eip))
		}
		gwInformer := factory.Kubeovn().V1().VpcNatGateways()
		for _, gw := range gws {
			require.NoError(t, gwInformer.Informer().GetStore().Add(gw))
		}
		return &Controller{
			iptablesEipsLister:  informer.Lister(),
			vpcNatGatewayLister: gwInformer.Lister(),
		}
	}

	readyEip := func(name string, deleting bool) *kubeovnv1.IptablesEIP {
		eip := &kubeovnv1.IptablesEIP{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       kubeovnv1.IptablesEIPSpec{NatGwDp: "gw"},
			Status:     kubeovnv1.IptablesEIPStatus{IP: "1.1.1.1"},
		}
		if deleting {
			now := metav1.Now()
			eip.DeletionTimestamp = &now
		}
		return eip
	}

	liveGw := []*kubeovnv1.VpcNatGateway{fakeGw("gw")}

	t.Run("live eip is bindable", func(t *testing.T) {
		eip, err := newController(t, liveGw, readyEip("live-eip", false)).getBindableEip("live-eip")
		require.NoError(t, err)
		require.Equal(t, "live-eip", eip.Name)
	})

	t.Run("terminating eip is rejected", func(t *testing.T) {
		_, err := newController(t, liveGw, readyEip("dying-eip", true)).getBindableEip("dying-eip")
		require.ErrorContains(t, err, "eip dying-eip is terminating")
	})

	t.Run("missing eip is rejected", func(t *testing.T) {
		_, err := newController(t, liveGw).getBindableEip("missing-eip")
		require.Error(t, err)
	})

	// The rules would be written into a gateway pod that is about to disappear.
	t.Run("terminating gateway is rejected", func(t *testing.T) {
		now := metav1.Now()
		dyingGw := fakeGw("gw")
		dyingGw.DeletionTimestamp = &now
		dyingGw.Finalizers = []string{util.KubeOVNControllerFinalizer}
		_, err := newController(t, []*kubeovnv1.VpcNatGateway{dyingGw}, readyEip("live-eip", false)).getBindableEip("live-eip")
		require.ErrorContains(t, err, "vpc nat gw gw is terminating")
	})
}

func TestCheckNatGwNotTerminating(t *testing.T) {
	t.Parallel()

	t.Run("live gateway is allowed", func(t *testing.T) {
		fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
			VpcNatGateways: []*kubeovnv1.VpcNatGateway{fakeGw("live-gw")},
		})
		require.NoError(t, err)
		require.NoError(t, fc.fakeController.checkNatGwNotTerminating("live-gw"))
	})

	t.Run("terminating gateway is rejected", func(t *testing.T) {
		now := metav1.Now()
		gw := fakeGw("dying-gw")
		gw.DeletionTimestamp = &now
		gw.Finalizers = []string{util.KubeOVNControllerFinalizer}
		fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
			VpcNatGateways: []*kubeovnv1.VpcNatGateway{gw},
		})
		require.NoError(t, err)
		err = fc.fakeController.checkNatGwNotTerminating("dying-gw")
		require.ErrorContains(t, err, "vpc nat gw dying-gw is terminating")
	})
}

func TestGetIptablesEipNatUsesUID(t *testing.T) {
	t.Parallel()

	oldEip := &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{Name: "old-eip", UID: "old-eip-uid"},
		Spec:       kubeovnv1.IptablesEIPSpec{V4ip: "1.1.1.1"},
		Status:     kubeovnv1.IptablesEIPStatus{IP: "1.1.1.1"},
	}
	newEip := &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{Name: "new-eip", UID: "new-eip-uid"},
		Spec:       kubeovnv1.IptablesEIPSpec{V4ip: "1.1.1.1"},
		Status:     kubeovnv1.IptablesEIPStatus{IP: "1.1.1.1"},
	}
	fip := &kubeovnv1.IptablesFIPRule{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "old-fip",
			Labels: map[string]string{util.EipV4IpLabel: "1.1.1.1", util.EipUIDLabel: "old-eip-uid"},
		},
		Spec: kubeovnv1.IptablesFIPRuleSpec{EIP: "old-eip"},
	}
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		IptablesEIPs: []*kubeovnv1.IptablesEIP{oldEip, newEip},
		IptablesFIPs: []*kubeovnv1.IptablesFIPRule{fip},
	})
	require.NoError(t, err)

	nat, err := fc.fakeController.getIptablesEipNat(newEip)
	require.NoError(t, err)
	require.Empty(t, nat)

	nat, err = fc.fakeController.getIptablesEipNat(oldEip)
	require.NoError(t, err)
	require.Equal(t, util.FipUsingEip, nat)
}

// TestGetIptablesEipNatFromAPISeesUncachedRule pins the release-side read to the API server: a rule
// that claimed the EIP before the informer caught up must still block the finalizer.
func TestGetIptablesEipNatFromAPISeesUncachedRule(t *testing.T) {
	t.Parallel()

	eip := &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{Name: "eip", UID: "eip-uid"},
		Spec:       kubeovnv1.IptablesEIPSpec{V4ip: "1.1.1.1"},
		Status:     kubeovnv1.IptablesEIPStatus{IP: "1.1.1.1"},
	}
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		IptablesEIPs: []*kubeovnv1.IptablesEIP{eip},
	})
	require.NoError(t, err)
	c := fc.fakeController

	// Created straight through the API, so the informer cache has not observed it.
	_, err = c.config.KubeOvnClient.KubeovnV1().IptablesFIPRules().Create(t.Context(), &kubeovnv1.IptablesFIPRule{
		ObjectMeta: metav1.ObjectMeta{Name: "fip", Labels: map[string]string{util.EipUIDLabel: "eip-uid"}},
		Spec:       kubeovnv1.IptablesFIPRuleSpec{EIP: "eip"},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	cached, err := c.getIptablesEipNat(eip)
	require.NoError(t, err)
	require.Empty(t, cached, "the informer cache has not observed the claim yet")

	fresh, err := c.getIptablesEipNatFromAPI(eip)
	require.NoError(t, err)
	require.Equal(t, util.FipUsingEip, fresh)
}

func TestQoSPolicyInUseUsesUID(t *testing.T) {
	old := vpcNatEnabled
	vpcNatEnabled = "true"
	t.Cleanup(func() { vpcNatEnabled = old })

	const name = "10.0.0.1"
	now := metav1.Now()
	qos := &kubeovnv1.QoSPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			UID:               "old-qos-uid",
			DeletionTimestamp: &now,
			Finalizers:        []string{util.KubeOVNControllerFinalizer},
		},
		Spec: kubeovnv1.QoSPolicySpec{BindingType: kubeovnv1.QoSBindingTypeEIP},
	}
	eip := &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			UID:    "new-eip-uid",
			Labels: map[string]string{util.QoSLabel: name, util.QoSPolicyUIDLabel: "new-qos-uid"},
		},
		Spec: kubeovnv1.IptablesEIPSpec{QoSPolicy: name},
	}
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		QoSPolicies:  []*kubeovnv1.QoSPolicy{qos},
		IptablesEIPs: []*kubeovnv1.IptablesEIP{eip},
	})
	require.NoError(t, err)

	require.NoError(t, fc.fakeController.handleUpdateQoSPolicy(name))
	updatedQos, err := fc.fakeController.config.KubeOvnClient.KubeovnV1().QoSPolicies().Get(t.Context(), name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, updatedQos.Finalizers)
}

func TestSyncNatUIDLabels(t *testing.T) {
	t.Parallel()

	qos := &kubeovnv1.QoSPolicy{ObjectMeta: metav1.ObjectMeta{Name: "qos", UID: "qos-uid"}}
	eip := &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{Name: "eip", UID: "eip-uid"},
		Spec:       kubeovnv1.IptablesEIPSpec{QoSPolicy: "qos"},
	}
	fip := &kubeovnv1.IptablesFIPRule{ObjectMeta: metav1.ObjectMeta{Name: "fip"}, Spec: kubeovnv1.IptablesFIPRuleSpec{EIP: "eip"}}
	dnat := &kubeovnv1.IptablesDnatRule{ObjectMeta: metav1.ObjectMeta{Name: "dnat"}, Spec: kubeovnv1.IptablesDnatRuleSpec{EIP: "eip"}}
	snat := &kubeovnv1.IptablesSnatRule{ObjectMeta: metav1.ObjectMeta{Name: "snat"}, Spec: kubeovnv1.IptablesSnatRuleSpec{EIP: "eip"}}
	gw := fakeGw("gw")
	gw.Spec.QoSPolicy = "qos"
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		QoSPolicies:       []*kubeovnv1.QoSPolicy{qos},
		IptablesEIPs:      []*kubeovnv1.IptablesEIP{eip},
		IptablesFIPs:      []*kubeovnv1.IptablesFIPRule{fip},
		IptablesDnatRules: []*kubeovnv1.IptablesDnatRule{dnat},
		IptablesSnatRules: []*kubeovnv1.IptablesSnatRule{snat},
		VpcNatGateways:    []*kubeovnv1.VpcNatGateway{gw},
	})
	require.NoError(t, err)

	require.NoError(t, fc.fakeController.syncNatUIDLabels(t.Context()))
	client := fc.fakeController.config.KubeOvnClient.KubeovnV1()

	updatedFip, err := client.IptablesFIPRules().Get(t.Context(), "fip", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "eip-uid", updatedFip.Labels[util.EipUIDLabel])
	updatedDnat, err := client.IptablesDnatRules().Get(t.Context(), "dnat", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "eip-uid", updatedDnat.Labels[util.EipUIDLabel])
	updatedSnat, err := client.IptablesSnatRules().Get(t.Context(), "snat", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "eip-uid", updatedSnat.Labels[util.EipUIDLabel])
	updatedEip, err := client.IptablesEIPs().Get(t.Context(), "eip", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "qos", updatedEip.Labels[util.QoSLabel])
	require.Equal(t, "qos-uid", updatedEip.Labels[util.QoSPolicyUIDLabel])
	updatedGw, err := client.VpcNatGateways().Get(t.Context(), "gw", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "qos", updatedGw.Labels[util.QoSLabel])
	require.Equal(t, "qos-uid", updatedGw.Labels[util.QoSPolicyUIDLabel])
}

func TestSyncNatUIDLabelsAlreadySynced(t *testing.T) {
	t.Parallel()

	qos := &kubeovnv1.QoSPolicy{ObjectMeta: metav1.ObjectMeta{Name: "qos", UID: "qos-uid"}}
	eip := &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "eip",
			UID:    "eip-uid",
			Labels: map[string]string{util.QoSLabel: "qos", util.QoSPolicyUIDLabel: "qos-uid"},
		},
		Spec: kubeovnv1.IptablesEIPSpec{QoSPolicy: "qos"},
	}
	fip := &kubeovnv1.IptablesFIPRule{
		ObjectMeta: metav1.ObjectMeta{Name: "fip", Labels: map[string]string{util.EipUIDLabel: "eip-uid"}},
		Spec:       kubeovnv1.IptablesFIPRuleSpec{EIP: "eip"},
	}
	gw := fakeGw("gw")
	gw.Spec.QoSPolicy = "qos"
	gw.Labels = map[string]string{util.QoSLabel: "qos", util.QoSPolicyUIDLabel: "qos-uid"}
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		QoSPolicies:    []*kubeovnv1.QoSPolicy{qos},
		IptablesEIPs:   []*kubeovnv1.IptablesEIP{eip},
		IptablesFIPs:   []*kubeovnv1.IptablesFIPRule{fip},
		VpcNatGateways: []*kubeovnv1.VpcNatGateway{gw},
	})
	require.NoError(t, err)
	require.NoError(t, fc.fakeController.syncNatUIDLabels(t.Context()))

	client := fc.fakeController.config.KubeOvnClient.KubeovnV1()
	updatedFip, err := client.IptablesFIPRules().Get(t.Context(), "fip", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, map[string]string{util.EipUIDLabel: "eip-uid"}, updatedFip.Labels)
	updatedEip, err := client.IptablesEIPs().Get(t.Context(), "eip", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, map[string]string{util.QoSLabel: "qos", util.QoSPolicyUIDLabel: "qos-uid"}, updatedEip.Labels)
	updatedGw, err := client.VpcNatGateways().Get(t.Context(), "gw", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, map[string]string{util.QoSLabel: "qos", util.QoSPolicyUIDLabel: "qos-uid"}, updatedGw.Labels)
}

func TestSyncNatUIDLabelsMissingReferences(t *testing.T) {
	t.Parallel()

	eip := &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{Name: "eip"},
		Spec:       kubeovnv1.IptablesEIPSpec{QoSPolicy: "missing-qos"},
	}
	fip := &kubeovnv1.IptablesFIPRule{ObjectMeta: metav1.ObjectMeta{Name: "fip"}, Spec: kubeovnv1.IptablesFIPRuleSpec{EIP: "missing-eip"}}
	gw := fakeGw("gw")
	gw.Spec.QoSPolicy = "missing-qos"
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		IptablesEIPs:   []*kubeovnv1.IptablesEIP{eip},
		IptablesFIPs:   []*kubeovnv1.IptablesFIPRule{fip},
		VpcNatGateways: []*kubeovnv1.VpcNatGateway{gw},
	})
	require.NoError(t, err)
	require.NoError(t, fc.fakeController.syncNatUIDLabels(t.Context()))

	client := fc.fakeController.config.KubeOvnClient.KubeovnV1()
	updatedFip, err := client.IptablesFIPRules().Get(t.Context(), "fip", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotContains(t, updatedFip.Labels, util.EipUIDLabel)
	updatedEip, err := client.IptablesEIPs().Get(t.Context(), "eip", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotContains(t, updatedEip.Labels, util.QoSPolicyUIDLabel)
	updatedGw, err := client.VpcNatGateways().Get(t.Context(), "gw", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotContains(t, updatedGw.Labels, util.QoSPolicyUIDLabel)
}

// TestPatchEipLabelSkipsTerminatingQoS pins the guard down in patchEipLabel, which writes the
// util.QoSLabel/util.QoSPolicyUIDLabel pair on the update path. handleResetIptablesEip reaches it
// from a delayed queue keyed by the EIP address, so without the guard a recreated EIP can be
// stamped with a previous instance's terminating QoS policy.
func TestPatchEipLabelSkipsTerminatingQoS(t *testing.T) {
	t.Parallel()

	now := metav1.Now()
	qos := &kubeovnv1.QoSPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "dying-qos", UID: "dying-qos-uid", DeletionTimestamp: &now},
	}
	eip := &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{Name: "reused-eip"},
		Spec:       kubeovnv1.IptablesEIPSpec{V4ip: "1.1.1.1", QoSPolicy: "dying-qos"},
	}

	// A terminating EIP is exempt: handleResetIptablesEip must still reach the status patch that
	// unblocks the EIP's own deletion, even when the EIP and its QoS policy are deleted together.
	dyingEip := eip.DeepCopy()
	dyingEip.Name = "dying-eip"
	dyingEip.DeletionTimestamp = &now

	// An EIP already carrying the reference is exempt: rewriting the same label adds nothing to the
	// in-use count, while failing here would abort handleUpdateVpcFloatingIP's redo loop for every
	// other FIP on the same gateway.
	boundEip := eip.DeepCopy()
	boundEip.Name = "bound-eip"
	boundEip.Labels = map[string]string{util.QoSLabel: "dying-qos", util.QoSPolicyUIDLabel: "dying-qos-uid"}

	client := kubeovnfake.NewSimpleClientset()
	for _, obj := range []*kubeovnv1.IptablesEIP{eip, dyingEip, boundEip} {
		_, err := client.KubeovnV1().IptablesEIPs().Create(t.Context(), obj, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	factory := kubeovninformerfactory.NewSharedInformerFactory(client, 0)
	eipInformer := factory.Kubeovn().V1().IptablesEIPs()
	qosInformer := factory.Kubeovn().V1().QoSPolicies()
	for _, obj := range []*kubeovnv1.IptablesEIP{eip, dyingEip, boundEip} {
		require.NoError(t, eipInformer.Informer().GetStore().Add(obj))
	}
	require.NoError(t, qosInformer.Informer().GetStore().Add(qos))

	c := &Controller{
		iptablesEipsLister: eipInformer.Lister(),
		qosPoliciesLister:  qosInformer.Lister(),
		config:             &Configuration{KubeOvnClient: client},
	}
	require.ErrorContains(t, c.patchEipLabel("reused-eip"), "qos policy dying-qos is terminating")
	require.NoError(t, c.patchEipLabel("dying-eip"))
	require.NoError(t, c.patchEipLabel("bound-eip"))

	// The exempt rewrite must keep the credential the in-use check counts, not blank it.
	bound, err := client.KubeovnV1().IptablesEIPs().Get(t.Context(), "bound-eip", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "dying-qos", bound.Labels[util.QoSLabel])
	require.Equal(t, "dying-qos-uid", bound.Labels[util.QoSPolicyUIDLabel])
}

// TestUpdateCrdNatGwLabelsSkipsTerminatingQoS covers the VpcNatGateway side of the same rule:
// updateCrdNatGwLabels writes the util.QoSLabel/util.QoSPolicyUIDLabel pair on gateways, which the
// QoSBindingTypeNatGw in-use check lists on.
func TestUpdateCrdNatGwLabelsSkipsTerminatingQoS(t *testing.T) {
	t.Parallel()

	now := metav1.Now()
	qos := &kubeovnv1.QoSPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "dying-qos", UID: "dying-qos-uid", DeletionTimestamp: &now},
		Spec:       kubeovnv1.QoSPolicySpec{BindingType: kubeovnv1.QoSBindingTypeNatGw},
	}
	gw := fakeGw("live-gw")
	dyingGw := fakeGw("dying-gw")
	dyingGw.DeletionTimestamp = &now
	boundGw := fakeGw("bound-gw")
	boundGw.Labels = map[string]string{util.QoSLabel: "dying-qos", util.QoSPolicyUIDLabel: "dying-qos-uid"}

	client := kubeovnfake.NewSimpleClientset()
	for _, obj := range []*kubeovnv1.VpcNatGateway{gw, dyingGw, boundGw} {
		_, err := client.KubeovnV1().VpcNatGateways().Create(t.Context(), obj, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	factory := kubeovninformerfactory.NewSharedInformerFactory(client, 0)
	gwInformer := factory.Kubeovn().V1().VpcNatGateways()
	qosInformer := factory.Kubeovn().V1().QoSPolicies()
	for _, obj := range []*kubeovnv1.VpcNatGateway{gw, dyingGw, boundGw} {
		require.NoError(t, gwInformer.Informer().GetStore().Add(obj))
	}
	require.NoError(t, qosInformer.Informer().GetStore().Add(qos))

	c := &Controller{
		vpcNatGatewayLister: gwInformer.Lister(),
		qosPoliciesLister:   qosInformer.Lister(),
		config:              &Configuration{KubeOvnClient: client},
	}
	require.ErrorContains(t, c.updateCrdNatGwLabels("live-gw", "dying-qos"), "qos policy dying-qos is terminating")
	// Already referencing it: rewriting the same label must not fail.
	require.NoError(t, c.updateCrdNatGwLabels("bound-gw", "dying-qos"))
	// Clearing the QoS reference must always be allowed, it is what releases the policy.
	require.NoError(t, c.updateCrdNatGwLabels("live-gw", ""))
	// A terminating gateway establishes no new binding and must not be blocked.
	require.NoError(t, c.updateCrdNatGwLabels("dying-gw", "dying-qos"))

	// The exempt rewrite must keep the credential the in-use check counts, not blank it.
	bound, err := client.KubeovnV1().VpcNatGateways().Get(t.Context(), "bound-gw", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "dying-qos", bound.Labels[util.QoSLabel])
	require.Equal(t, "dying-qos-uid", bound.Labels[util.QoSPolicyUIDLabel])
}

// TestTerminatingQoSPolicyDeadlock replays the production lifecycle with fake clients: a QoSPolicy is
// marked for deletion while an EIP of the same name (they are both named after the address) is
// recreated for the next instance. The policy waits for its referencing EIPs to disappear before
// dropping its finalizer, so before the fix the recreated EIP took the tombstone's label and both
// objects stayed forever, blocking the address from ever being freed.
//
// This is not covered by the guard unit tests: they only prove the guard returns an error, not
// that the loop actually closes.
func TestTerminatingQoSPolicyDeadlock(t *testing.T) {
	// vpcNatEnabled is a package level variable, so this test cannot run in parallel.
	old := vpcNatEnabled
	vpcNatEnabled = "true"
	t.Cleanup(func() { vpcNatEnabled = old })

	const name = "10.0.0.1"
	now := metav1.Now()
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		QoSPolicies: []*kubeovnv1.QoSPolicy{{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				DeletionTimestamp: &now,
				Finalizers:        []string{util.KubeOVNControllerFinalizer},
			},
			Spec: kubeovnv1.QoSPolicySpec{BindingType: kubeovnv1.QoSBindingTypeEIP},
		}},
		IptablesEIPs: []*kubeovnv1.IptablesEIP{{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       kubeovnv1.IptablesEIPSpec{V4ip: name, QoSPolicy: name},
		}},
	})
	require.NoError(t, err)
	c := fc.fakeController

	// The recreated EIP must refuse to take the tombstone, before allocating anything.
	require.ErrorContains(t, c.handleAddIptablesEip(name), "qos policy "+name+" is terminating")
	eip, err := c.iptablesEipsLister.Get(name)
	require.NoError(t, err)
	require.NotContains(t, eip.Labels, util.QoSLabel, "the tombstone must not be referenced")

	// With no referencing EIP left, the policy drops its finalizer and is finally reclaimed.
	require.NoError(t, c.handleUpdateQoSPolicy(name))
	qos, err := c.config.KubeOvnClient.KubeovnV1().QoSPolicies().Get(t.Context(), name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, qos.Finalizers, "the terminating qos policy must be released")
}
