package webhook

import (
	"testing"

	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ovnv1 "github.com/kubeovn/kube-ovn/pkg/apis/kubeovn/v1"
	"github.com/kubeovn/kube-ovn/pkg/util"
)

// TestEipUIDSelectorIsolatesGenerations pins admission to the same credential the controller's
// in-use check counts. Two EIPs can carry the same address, so selecting by address made the
// webhook block a deletion because of a rule belonging to a different EIP.
func TestEipUIDSelectorIsolatesGenerations(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, ovnv1.AddToScheme(scheme))

	mine := &ovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{Name: "mine", UID: "mine-uid"},
		Status:     ovnv1.IptablesEIPStatus{IP: "1.1.1.1"},
	}
	other := &ovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{Name: "other", UID: "other-uid"},
		Status:     ovnv1.IptablesEIPStatus{IP: "1.1.1.1"},
	}
	// Belongs to "other" but shares the address, which is exactly what used to be miscounted.
	fip := &ovnv1.IptablesFIPRule{
		ObjectMeta: metav1.ObjectMeta{
			Name: "other-fip",
			Labels: map[string]string{
				util.EipV4IpLabel: "1.1.1.1",
				util.EipUIDLabel:  "other-uid",
			},
		},
		Spec: ovnv1.IptablesFIPRuleSpec{EIP: "other"},
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mine, other, fip).Build()

	list := &ovnv1.IptablesFIPRuleList{}
	require.NoError(t, reader.List(t.Context(), list, eipUIDSelector(mine)))
	require.Empty(t, list.Items, "a rule owned by another EIP must not count")

	list = &ovnv1.IptablesFIPRuleList{}
	require.NoError(t, reader.List(t.Context(), list, eipUIDSelector(other)))
	require.Len(t, list.Items, 1)
}

// TestValidateQoSPolicyRef covers the admission guard shared by IptablesEIP and VpcNatGateway.
// The controller keeps a referrer pointing at a missing or terminating policy out of Ready, so
// admission has to reject it up front instead of leaving the user with a silent retry loop.
func TestValidateQoSPolicyRef(t *testing.T) {
	t.Parallel()

	now := metav1.Now()
	scheme := runtime.NewScheme()
	require.NoError(t, ovnv1.AddToScheme(scheme))
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&ovnv1.QoSPolicy{ObjectMeta: metav1.ObjectMeta{
			Name:       "live-qos",
			Finalizers: []string{util.KubeOVNControllerFinalizer},
		}},
		&ovnv1.QoSPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "pending-qos", Finalizers: []string{util.KubeOVNControllerFinalizer}},
			Spec:       ovnv1.QoSPolicySpec{BindingType: ovnv1.QoSBindingTypeEIP},
		},
		&ovnv1.QoSPolicy{ObjectMeta: metav1.ObjectMeta{Name: "new-qos"}},
		&ovnv1.QoSPolicy{ObjectMeta: metav1.ObjectMeta{
			Name:              "dying-qos",
			DeletionTimestamp: &now,
			Finalizers:        []string{util.KubeOVNControllerFinalizer},
		}},
	).Build()

	t.Run("empty reference is allowed", func(t *testing.T) {
		require.NoError(t, validateQoSPolicyRef(t.Context(), reader, ""))
	})

	t.Run("existing policy is allowed", func(t *testing.T) {
		require.NoError(t, validateQoSPolicyRef(t.Context(), reader, "live-qos"))
	})

	t.Run("policy without controller reconcile is rejected", func(t *testing.T) {
		require.ErrorContains(t, validateQoSPolicyRef(t.Context(), reader, "new-qos"), "not ready")
	})

	t.Run("policy with stale status is rejected", func(t *testing.T) {
		require.ErrorContains(t, validateQoSPolicyRef(t.Context(), reader, "pending-qos"), "not ready")
	})

	t.Run("missing policy is rejected", func(t *testing.T) {
		err := validateQoSPolicyRef(t.Context(), reader, "missing-qos")
		require.Error(t, err)
		require.True(t, k8serrors.IsNotFound(err))
		require.ErrorContains(t, err, "create it before referencing it")
	})

	t.Run("terminating policy is rejected", func(t *testing.T) {
		err := validateQoSPolicyRef(t.Context(), reader, "dying-qos")
		require.ErrorContains(t, err, "terminating")
		require.ErrorContains(t, err, "wait for its deletion to complete")
	})
}
