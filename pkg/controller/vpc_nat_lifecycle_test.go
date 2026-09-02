package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	kubeovnv1 "github.com/kubeovn/kube-ovn/pkg/apis/kubeovn/v1"
	"github.com/kubeovn/kube-ovn/pkg/util"
)

// TestHandleAddSkipsTerminating covers keys that were queued while the object was still live and
// only reached the worker after deletion started. The enqueue-side routing cannot catch those, so
// the add handlers must return early instead of allocating IPs or writing rules for a dying object.
func TestHandleAddSkipsTerminating(t *testing.T) {
	old := vpcNatEnabled
	vpcNatEnabled = "true"
	t.Cleanup(func() { vpcNatEnabled = old })

	now := metav1.Now()
	final := []string{util.KubeOVNControllerFinalizer}

	eip := &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{Name: "dying-eip", DeletionTimestamp: &now, Finalizers: final},
		Spec:       kubeovnv1.IptablesEIPSpec{NatGwDp: "gw", QoSPolicy: "missing-qos"},
	}
	fip := &kubeovnv1.IptablesFIPRule{
		ObjectMeta: metav1.ObjectMeta{Name: "dying-fip", DeletionTimestamp: &now, Finalizers: final},
		Spec:       kubeovnv1.IptablesFIPRuleSpec{EIP: "dying-eip", InternalIP: "10.0.0.5"},
	}
	dnat := &kubeovnv1.IptablesDnatRule{
		ObjectMeta: metav1.ObjectMeta{Name: "dying-dnat", DeletionTimestamp: &now, Finalizers: final},
		Spec: kubeovnv1.IptablesDnatRuleSpec{
			EIP: "dying-eip", ExternalPort: "80", InternalPort: "8080",
			InternalIP: "10.0.0.5", Protocol: "tcp",
		},
	}
	snat := &kubeovnv1.IptablesSnatRule{
		ObjectMeta: metav1.ObjectMeta{Name: "dying-snat", DeletionTimestamp: &now, Finalizers: final},
		Spec:       kubeovnv1.IptablesSnatRuleSpec{EIP: "dying-eip", InternalCIDR: "10.0.0.0/24"},
	}
	qos := &kubeovnv1.QoSPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "dying-qos", DeletionTimestamp: &now, Finalizers: final},
		Spec:       kubeovnv1.QoSPolicySpec{BindingType: kubeovnv1.QoSBindingTypeEIP},
	}

	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		IptablesEIPs:      []*kubeovnv1.IptablesEIP{eip},
		IptablesFIPs:      []*kubeovnv1.IptablesFIPRule{fip},
		IptablesDnatRules: []*kubeovnv1.IptablesDnatRule{dnat},
		IptablesSnatRules: []*kubeovnv1.IptablesSnatRule{snat},
		QoSPolicies:       []*kubeovnv1.QoSPolicy{qos},
	})
	require.NoError(t, err)
	c := fc.fakeController

	// None of these may report an error: an error would requeue the key forever, and any work done
	// here would be work against an object that is already being torn down.
	require.NoError(t, c.handleAddIptablesEip("dying-eip"))
	require.NoError(t, c.handleAddIptablesFip("dying-fip"))
	require.NoError(t, c.handleAddIptablesDnatRule("dying-dnat"))
	require.NoError(t, c.handleAddIptablesSnatRule("dying-snat"))
	require.NoError(t, c.handleAddQoSPolicy("dying-qos"))

	// The EIP must not have been allocated an address or stamped with a QoS credential.
	stored, err := c.config.KubeOvnClient.KubeovnV1().IptablesEIPs().Get(t.Context(), "dying-eip", metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, stored.Status.IP)
	require.NotContains(t, stored.Labels, util.QoSPolicyUIDLabel)
}

// TestSyncVpcNatGatewayCRKeepsQoSLabels pins the startup ordering: syncNatUIDLabels runs before the
// workers and initResourceOnce runs after, so syncVpcNatGatewayCR must not blank the credential
// that the QoS in-use check counts.
func TestSyncVpcNatGatewayCRKeepsQoSLabels(t *testing.T) {
	old := vpcNatEnabled
	vpcNatEnabled = "true"
	t.Cleanup(func() { vpcNatEnabled = old })

	qos := &kubeovnv1.QoSPolicy{ObjectMeta: metav1.ObjectMeta{Name: "qos", UID: "qos-uid"}}
	gw := fakeGw("gw")
	gw.Spec.QoSPolicy = "qos"

	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		QoSPolicies:    []*kubeovnv1.QoSPolicy{qos},
		VpcNatGateways: []*kubeovnv1.VpcNatGateway{gw},
		ConfigMaps: []*corev1.ConfigMap{
			{
				ObjectMeta: metav1.ObjectMeta{Name: util.VpcNatGatewayConfig, Namespace: "kube-system"},
				Data:       map[string]string{"enable-vpc-nat-gw": "true"},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: util.VpcNatConfig, Namespace: "kube-system"},
				Data:       map[string]string{"image": "kubeovn/vpc-nat-gateway:test"},
			},
		},
	})
	require.NoError(t, err)
	c := fc.fakeController

	require.NoError(t, c.syncNatUIDLabels(t.Context()))
	// initResourceOnce runs this after the workers start; it must preserve the backfilled credential.
	require.NoError(t, c.syncVpcNatGatewayCR())

	stored, err := c.config.KubeOvnClient.KubeovnV1().VpcNatGateways().Get(t.Context(), "gw", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "qos", stored.Labels[util.QoSLabel])
	require.Equal(t, "qos-uid", stored.Labels[util.QoSPolicyUIDLabel])
}

// TestCleanupShareDnatInPodNatGwGone verifies the rebuild branch is skipped once the gateway is
// gone. A sibling share backend is left behind on purpose so the cleanup takes the rebuild path,
// which talks to the gateway pod; without the guard it retries forever and never releases the
// finalizer.
func TestCleanupShareDnatInPodNatGwGone(t *testing.T) {
	t.Parallel()

	sibling := func(gwName string) *kubeovnv1.IptablesDnatRule {
		return &kubeovnv1.IptablesDnatRule{
			ObjectMeta: metav1.ObjectMeta{
				Name: "sibling-dnat",
				Labels: map[string]string{
					util.VpcNatGatewayNameLabel: gwName,
					util.VpcDnatEPortLabel:      "80",
				},
			},
			Spec: kubeovnv1.IptablesDnatRuleSpec{
				EIP: "eip", ExternalPort: "80", InternalPort: "8080",
				InternalIP: "10.0.0.9", Protocol: "tcp",
				Type: kubeovnv1.DnatRuleTypeShare,
			},
		}
	}

	t.Run("gateway missing", func(t *testing.T) {
		fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
			IptablesDnatRules: []*kubeovnv1.IptablesDnatRule{sibling("missing-gw")},
		})
		require.NoError(t, err)
		backends, err := fc.fakeController.getShareBackends("missing-gw", "eip", "80", "tcp", "dnat")
		require.NoError(t, err)
		require.NotEmpty(t, backends, "the rebuild branch must be reachable")

		require.NoError(t, fc.fakeController.cleanupShareDnatInPod(
			"dnat", "missing-gw", "eip", "tcp", "10.0.0.1", "80", "dnat",
		))
	})

	t.Run("gateway terminating", func(t *testing.T) {
		now := metav1.Now()
		gw := fakeGw("dying-gw")
		gw.DeletionTimestamp = &now
		gw.Finalizers = []string{util.KubeOVNControllerFinalizer}
		fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
			VpcNatGateways:    []*kubeovnv1.VpcNatGateway{gw},
			IptablesDnatRules: []*kubeovnv1.IptablesDnatRule{sibling("dying-gw")},
		})
		require.NoError(t, err)
		backends, err := fc.fakeController.getShareBackends("dying-gw", "eip", "80", "tcp", "dnat")
		require.NoError(t, err)
		require.NotEmpty(t, backends, "the rebuild branch must be reachable")

		require.NoError(t, fc.fakeController.cleanupShareDnatInPod(
			"dnat", "dying-gw", "eip", "tcp", "10.0.0.1", "80", "dnat",
		))
	})
}

// TestSyncVpcNatGatewayCRDanglingQoSDoesNotBlockStartup covers a gateway left pointing at a policy
// that no longer exists. syncVpcNatGatewayCR runs from initResourceOnce, whose errors are fatal, so
// a dangling reference must not keep the controller from starting; the backfill skips the same case.
func TestSyncVpcNatGatewayCRDanglingQoSDoesNotBlockStartup(t *testing.T) {
	old := vpcNatEnabled
	vpcNatEnabled = "true"
	t.Cleanup(func() { vpcNatEnabled = old })

	gw := fakeGw("gw")
	gw.Spec.QoSPolicy = "gone-qos"

	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		VpcNatGateways: []*kubeovnv1.VpcNatGateway{gw},
		ConfigMaps: []*corev1.ConfigMap{
			{
				ObjectMeta: metav1.ObjectMeta{Name: util.VpcNatGatewayConfig, Namespace: "kube-system"},
				Data:       map[string]string{"enable-vpc-nat-gw": "true"},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: util.VpcNatConfig, Namespace: "kube-system"},
				Data:       map[string]string{"image": "kubeovn/vpc-nat-gateway:test"},
			},
		},
	})
	require.NoError(t, err)
	c := fc.fakeController

	require.NoError(t, c.syncNatUIDLabels(t.Context()), "backfill skips the dangling reference")
	require.NoError(t, c.syncVpcNatGatewayCR(), "startup must not fail on a dangling reference")
}

// TestQoSPolicyReleaseReadsThroughAPI mirrors the EIP side: the release decision must not run on an
// informer cache that has not observed a referrer written moments earlier, or the policy drops its
// finalizer while still in use.
func TestQoSPolicyReleaseReadsThroughAPI(t *testing.T) {
	old := vpcNatEnabled
	vpcNatEnabled = "true"
	t.Cleanup(func() { vpcNatEnabled = old })

	now := metav1.Now()
	qos := &kubeovnv1.QoSPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "qos",
			UID:               "qos-uid",
			DeletionTimestamp: &now,
			Finalizers:        []string{util.KubeOVNControllerFinalizer},
		},
		Spec: kubeovnv1.QoSPolicySpec{BindingType: kubeovnv1.QoSBindingTypeEIP},
	}
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		QoSPolicies: []*kubeovnv1.QoSPolicy{qos},
	})
	require.NoError(t, err)
	c := fc.fakeController

	// Created straight through the API, so the informer cache does not have it.
	_, err = c.config.KubeOvnClient.KubeovnV1().IptablesEIPs().Create(t.Context(), &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "eip",
			Labels: map[string]string{util.QoSLabel: "qos", util.QoSPolicyUIDLabel: "qos-uid"},
		},
		Spec: kubeovnv1.IptablesEIPSpec{QoSPolicy: "qos"},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	cached, err := c.iptablesEipsLister.List(labels.SelectorFromSet(labels.Set{util.QoSPolicyUIDLabel: "qos-uid"}))
	require.NoError(t, err)
	require.Empty(t, cached, "the informer cache has not observed the referrer yet")

	require.NoError(t, c.handleUpdateQoSPolicy("qos"))
	stored, err := c.config.KubeOvnClient.KubeovnV1().QoSPolicies().Get(t.Context(), "qos", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, stored.Finalizers, "the policy is still referenced and must keep its finalizer")
}

// TestFipRebindSwapsClaimBetweenPodOperations pins the claim ordering on the rebind path. The EIP
// in-use check counts the UID label, so the old EIP has to stay claimed until its rule is gone and
// the new one has to be claimed before its rule exists; either edge lets a concurrent release drop
// a finalizer with a live rule behind it.
func TestFipRebindSwapsClaimBetweenPodOperations(t *testing.T) {
	old := vpcNatEnabled
	vpcNatEnabled = "true"
	t.Cleanup(func() { vpcNatEnabled = old })

	newFip := func(statusNatGwDp string) *kubeovnv1.IptablesFIPRule {
		return &kubeovnv1.IptablesFIPRule{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "fip",
				Labels: map[string]string{util.VpcNatGatewayNameLabel: "gw", util.EipV4IpLabel: "1.1.1.1", util.EipUIDLabel: "old-uid"},
			},
			Spec: kubeovnv1.IptablesFIPRuleSpec{EIP: "new-eip", InternalIP: "10.0.0.1"},
			Status: kubeovnv1.IptablesFIPRuleStatus{
				V4ip: "1.1.1.1", NatGwDp: statusNatGwDp, InternalIP: "10.0.0.1", Ready: true,
			},
		}
	}
	setup := func(t *testing.T, fip *kubeovnv1.IptablesFIPRule) *Controller {
		t.Helper()
		eip := &kubeovnv1.IptablesEIP{
			ObjectMeta: metav1.ObjectMeta{Name: "new-eip", UID: "new-uid"},
			Spec:       kubeovnv1.IptablesEIPSpec{V4ip: "2.2.2.2", NatGwDp: "gw"},
			Status:     kubeovnv1.IptablesEIPStatus{IP: "2.2.2.2"},
		}
		fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
			VpcNatGateways: []*kubeovnv1.VpcNatGateway{fakeGw("gw")},
			IptablesEIPs:   []*kubeovnv1.IptablesEIP{eip},
			IptablesFIPs:   []*kubeovnv1.IptablesFIPRule{fip},
		})
		require.NoError(t, err)
		return fc.fakeController
	}
	storedLabel := func(t *testing.T, c *Controller) string {
		t.Helper()
		stored, err := c.config.KubeOvnClient.KubeovnV1().IptablesFIPRules().Get(t.Context(), "fip", metav1.GetOptions{})
		require.NoError(t, err)
		return stored.Labels[util.EipUIDLabel]
	}

	t.Run("old claim is kept while the old rule removal fails", func(t *testing.T) {
		// Status points at the live gateway, whose pod is absent, so the removal errors out.
		c := setup(t, newFip("gw"))
		require.Error(t, c.handleUpdateIptablesFip("fip"))
		require.Equal(t, "old-uid", storedLabel(t, c), "releasing the old EIP here would orphan its rule")
	})

	t.Run("new claim is written before the new rule", func(t *testing.T) {
		// Status points at a gateway that is already gone, so the removal is a no-op and the run
		// gets as far as creating the new rule, which fails on the missing pod.
		c := setup(t, newFip("retired-gw"))
		require.Error(t, c.handleUpdateIptablesFip("fip"))
		require.Equal(t, "new-uid", storedLabel(t, c), "the claim must land before the rule it covers")
	})
}

// TestCreateOrUpdateEipCRClearsQoSLabels pins the update branch to the create branch: writing the
// QoS credential only when the reference is non-empty leaves the previous UID behind, and the
// in-use check counts that label.
func TestCreateOrUpdateEipCRClearsQoSLabels(t *testing.T) {
	eip := &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "eip",
			Labels: map[string]string{util.QoSLabel: "gone-qos", util.QoSPolicyUIDLabel: "gone-uid"},
		},
		Spec: kubeovnv1.IptablesEIPSpec{V4ip: "1.1.1.1", NatGwDp: "gw"},
	}
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		IptablesEIPs: []*kubeovnv1.IptablesEIP{eip},
	})
	require.NoError(t, err)
	c := fc.fakeController

	require.NoError(t, c.createOrUpdateEipCR("eip", "1.1.1.1", "", "", "gw", "", "external", ""))

	stored, err := c.config.KubeOvnClient.KubeovnV1().IptablesEIPs().Get(t.Context(), "eip", metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, stored.Labels[util.QoSLabel])
	require.Empty(t, stored.Labels[util.QoSPolicyUIDLabel], "a dropped reference must not keep counting")
}

// TestWaitNatLabelClaimsSyncedNamesStragglers covers the diagnostics on a path whose caller aborts
// startup, where a bare deadline error says nothing about what failed to converge.
func TestWaitNatLabelClaimsSyncedNamesStragglers(t *testing.T) {
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		IptablesFIPs: []*kubeovnv1.IptablesFIPRule{
			{ObjectMeta: metav1.ObjectMeta{Name: "fip"}},
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	err = fc.fakeController.waitNatLabelClaimsSynced(ctx, []natLabelClaim{
		{natType: util.FipUsingEip, name: "fip", labelKey: util.EipUIDLabel, value: "never-written"},
	})
	require.ErrorContains(t, err, "fip")
	require.ErrorContains(t, err, "1 of 1 claims still unsynced")
}

// TestPatchIptableInfoRejectsUnknownType keeps an unroutable type from reporting success, which
// would leave the startup backfill waiting for a label nobody wrote.
func TestPatchIptableInfoRejectsUnknownType(t *testing.T) {
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{})
	require.NoError(t, err)
	require.ErrorContains(t, fc.fakeController.patchIptableInfo("name", "bogus", "[]"), "unknown nat type")
}

// TestSyncNatUIDLabelsSkipsTerminating keeps the startup backfill from writing credentials the
// normal writers would refuse. Stamping a terminating policy or EIP revives a reference the
// release path was about to stop counting, which is the deadlock this series removes.
func TestSyncNatUIDLabelsSkipsTerminating(t *testing.T) {
	now := metav1.Now()
	fin := []string{util.KubeOVNControllerFinalizer}

	dyingQoS := &kubeovnv1.QoSPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "dying-qos", UID: "dying-qos-uid", DeletionTimestamp: &now, Finalizers: fin},
	}
	liveQoS := &kubeovnv1.QoSPolicy{ObjectMeta: metav1.ObjectMeta{Name: "live-qos", UID: "live-qos-uid"}}
	dyingEip := &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{Name: "dying-eip", UID: "dying-eip-uid", DeletionTimestamp: &now, Finalizers: fin},
	}
	// Live referrer pointing at a terminating policy: the policy is free to go, so nothing may
	// hand it a fresh claim.
	liveEip := &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{Name: "live-eip", UID: "live-eip-uid"},
		Spec:       kubeovnv1.IptablesEIPSpec{QoSPolicy: "dying-qos"},
	}
	// Terminating referrer pointing at a live policy: it establishes no new binding.
	dyingFip := &kubeovnv1.IptablesFIPRule{
		ObjectMeta: metav1.ObjectMeta{Name: "dying-fip", DeletionTimestamp: &now, Finalizers: fin},
		Spec:       kubeovnv1.IptablesFIPRuleSpec{EIP: "live-eip"},
	}
	// Live referrer pointing at a terminating EIP.
	liveSnat := &kubeovnv1.IptablesSnatRule{
		ObjectMeta: metav1.ObjectMeta{Name: "live-snat"},
		Spec:       kubeovnv1.IptablesSnatRuleSpec{EIP: "dying-eip"},
	}
	dyingGw := fakeGw("dying-gw")
	dyingGw.DeletionTimestamp = &now
	dyingGw.Finalizers = fin
	dyingGw.Spec.QoSPolicy = "live-qos"

	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		QoSPolicies:       []*kubeovnv1.QoSPolicy{dyingQoS, liveQoS},
		IptablesEIPs:      []*kubeovnv1.IptablesEIP{dyingEip, liveEip},
		IptablesFIPs:      []*kubeovnv1.IptablesFIPRule{dyingFip},
		IptablesSnatRules: []*kubeovnv1.IptablesSnatRule{liveSnat},
		VpcNatGateways:    []*kubeovnv1.VpcNatGateway{dyingGw},
	})
	require.NoError(t, err)
	c := fc.fakeController

	require.NoError(t, c.syncNatUIDLabels(t.Context()))

	kc := c.config.KubeOvnClient.KubeovnV1()
	eip, err := kc.IptablesEIPs().Get(t.Context(), "live-eip", metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, eip.Labels[util.QoSPolicyUIDLabel], "a terminating policy must not gain a referrer")

	fip, err := kc.IptablesFIPRules().Get(t.Context(), "dying-fip", metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, fip.Labels[util.EipUIDLabel], "a terminating referrer establishes no binding")

	snat, err := kc.IptablesSnatRules().Get(t.Context(), "live-snat", metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, snat.Labels[util.EipUIDLabel], "a terminating eip must not gain a referrer")

	gw, err := kc.VpcNatGateways().Get(t.Context(), "dying-gw", metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, gw.Labels[util.QoSPolicyUIDLabel], "a terminating gateway establishes no binding")
}

// TestEipCleanupSurvivesMissingSubnet keeps a gone external subnet from holding an EIP's
// finalizer. The cleanup used to resolve the subnet before it even looked at DeletionTimestamp,
// so an EIP outliving its subnet could never finish deleting.
func TestEipCleanupSurvivesMissingSubnet(t *testing.T) {
	old := vpcNatEnabled
	vpcNatEnabled = "true"
	t.Cleanup(func() { vpcNatEnabled = old })

	now := metav1.Now()
	eip := &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "eip",
			UID:               "eip-uid",
			DeletionTimestamp: &now,
			Finalizers:        []string{util.KubeOVNControllerFinalizer},
		},
		// The subnet this points at is deliberately absent from the fixture.
		Spec:   kubeovnv1.IptablesEIPSpec{ExternalSubnet: "gone-subnet", NatGwDp: "gw"},
		Status: kubeovnv1.IptablesEIPStatus{IP: "1.1.1.1"},
	}
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		IptablesEIPs: []*kubeovnv1.IptablesEIP{eip},
	})
	require.NoError(t, err)
	c := fc.fakeController

	require.NoError(t, c.handleUpdateIptablesEip("eip"))

	stored, err := c.config.KubeOvnClient.KubeovnV1().IptablesEIPs().Get(t.Context(), "eip", metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, stored.Finalizers, "an unreferenced eip must release even without its subnet")
}

// TestPatchEipLabelClearsDroppedQoS pins the other half of the exempt-path rule: the recorded UID
// is preserved only while the EIP still points at the policy. Dropping the reference must clear
// both labels, or the policy keeps counting a referrer that no longer refers to it.
func TestPatchEipLabelClearsDroppedQoS(t *testing.T) {
	now := metav1.Now()
	dying := &kubeovnv1.QoSPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "qos",
			UID:               "qos-uid",
			DeletionTimestamp: &now,
			Finalizers:        []string{util.KubeOVNControllerFinalizer},
		},
	}
	eip := &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "eip",
			Labels: map[string]string{util.QoSLabel: "qos", util.QoSPolicyUIDLabel: "qos-uid"},
		},
		// The reference is gone from the spec while the policy it named is still terminating.
		Spec: kubeovnv1.IptablesEIPSpec{QoSPolicy: "", NatGwDp: "gw"},
	}
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		QoSPolicies:  []*kubeovnv1.QoSPolicy{dying},
		IptablesEIPs: []*kubeovnv1.IptablesEIP{eip},
	})
	require.NoError(t, err)
	c := fc.fakeController

	require.NoError(t, c.patchEipLabel("eip"))

	stored, err := c.config.KubeOvnClient.KubeovnV1().IptablesEIPs().Get(t.Context(), "eip", metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, stored.Labels[util.QoSLabel])
	require.Empty(t, stored.Labels[util.QoSPolicyUIDLabel], "a dropped reference must stop counting")
}

// TestSyncVpcNatGatewayCRSkipsTerminating closes the same hole as the backfill on the other
// startup path: updateCrdNatGwLabels resolves a live policy and stamps its UID, so a gateway
// already being deleted would hand the policy a referrer that outlives the reconcile.
func TestSyncVpcNatGatewayCRSkipsTerminating(t *testing.T) {
	old := vpcNatEnabled
	vpcNatEnabled = "true"
	t.Cleanup(func() { vpcNatEnabled = old })

	now := metav1.Now()
	gw := fakeGw("dying-gw")
	gw.DeletionTimestamp = &now
	gw.Finalizers = []string{util.KubeOVNControllerFinalizer}
	gw.Spec.QoSPolicy = "live-qos"

	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		QoSPolicies:    []*kubeovnv1.QoSPolicy{{ObjectMeta: metav1.ObjectMeta{Name: "live-qos", UID: "live-qos-uid"}}},
		VpcNatGateways: []*kubeovnv1.VpcNatGateway{gw},
		ConfigMaps: []*corev1.ConfigMap{
			{
				ObjectMeta: metav1.ObjectMeta{Name: util.VpcNatGatewayConfig, Namespace: "kube-system"},
				Data:       map[string]string{"enable-vpc-nat-gw": "true"},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: util.VpcNatConfig, Namespace: "kube-system"},
				Data:       map[string]string{"image": "kubeovn/vpc-nat-gateway:test"},
			},
		},
	})
	require.NoError(t, err)
	c := fc.fakeController

	require.NoError(t, c.syncVpcNatGatewayCR())

	stored, err := c.config.KubeOvnClient.KubeovnV1().VpcNatGateways().Get(t.Context(), "dying-gw", metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, stored.Labels[util.QoSPolicyUIDLabel], "a terminating gateway establishes no binding")
}
