package controller

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"

	kubeovnv1 "github.com/kubeovn/kube-ovn/pkg/apis/kubeovn/v1"
	"github.com/kubeovn/kube-ovn/pkg/util"
)

type failingEipLister struct{}

func (failingEipLister) List(labels.Selector) ([]*kubeovnv1.IptablesEIP, error) {
	return nil, errors.New("eip lister failed")
}

func (failingEipLister) Get(string) (*kubeovnv1.IptablesEIP, error) {
	return nil, errors.New("eip lister failed")
}

func TestQoSPolicyReferrersWaitForStatusInformerUpdate(t *testing.T) {
	qos := &kubeovnv1.QoSPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "late-qos"},
		Spec:       kubeovnv1.QoSPolicySpec{BindingType: kubeovnv1.QoSBindingTypeEIP},
	}
	eip := &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{Name: "pending-eip"},
		Spec:       kubeovnv1.IptablesEIPSpec{QoSPolicy: "late-qos"},
	}
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		QoSPolicies:  []*kubeovnv1.QoSPolicy{qos},
		IptablesEIPs: []*kubeovnv1.IptablesEIP{eip},
	})
	require.NoError(t, err)
	c := fc.fakeController
	c.addIptablesEipQueue = newTypedRateLimitingQueue[string]("AddIptablesEip", nil)
	c.updateIptablesEipQueue = newTypedRateLimitingQueue[string]("UpdateIptablesEip", nil)
	c.addOrUpdateVpcNatGatewayQueue = newTypedRateLimitingQueue[string]("AddOrUpdateVpcNatGw", nil)
	t.Cleanup(c.addIptablesEipQueue.ShutDown)
	t.Cleanup(c.updateIptablesEipQueue.ShutDown)
	t.Cleanup(c.addOrUpdateVpcNatGatewayQueue.ShutDown)

	require.NoError(t, c.handleAddQoSPolicy("late-qos"))
	require.Zero(t, c.addIptablesEipQueue.Len(), "the lister still exposes the previous qos status")

	updated, err := c.config.KubeOvnClient.KubeovnV1().QoSPolicies().Get(t.Context(), "late-qos", metav1.GetOptions{})
	require.NoError(t, err)
	require.True(t, qosPolicyStatusMatchesSpec(updated))
	c.enqueueUpdateQoSPolicy(qos, updated)
	require.Equal(t, 1, c.addIptablesEipQueue.Len(), "the reconciled status update wakes the referrer")
}

func TestQoSPolicyInvalidationEnqueuesEstablishedReferrers(t *testing.T) {
	oldQos := &kubeovnv1.QoSPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "qos", Finalizers: []string{util.KubeOVNControllerFinalizer}},
		Spec:       kubeovnv1.QoSPolicySpec{BindingType: kubeovnv1.QoSBindingTypeEIP},
		Status:     kubeovnv1.QoSPolicyStatus{BindingType: kubeovnv1.QoSBindingTypeEIP},
	}
	eip := &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{Name: "eip"},
		Spec:       kubeovnv1.IptablesEIPSpec{QoSPolicy: "qos"},
		Status:     kubeovnv1.IptablesEIPStatus{Ready: true, IP: "1.1.1.1", QoSPolicy: "qos"},
	}
	gw := &kubeovnv1.VpcNatGateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw"},
		Spec:       kubeovnv1.VpcNatGatewaySpec{QoSPolicy: "qos"},
		Status:     kubeovnv1.VpcNatGatewayStatus{QoSPolicy: "qos"},
	}
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		IptablesEIPs:   []*kubeovnv1.IptablesEIP{eip},
		VpcNatGateways: []*kubeovnv1.VpcNatGateway{gw},
	})
	require.NoError(t, err)
	c := fc.fakeController
	c.updateQoSPolicyQueue = newTypedRateLimitingQueue[string]("UpdateQoSPolicy", nil)
	c.updateIptablesEipQueue = newTypedRateLimitingQueue[string]("UpdateIptablesEip", nil)
	c.addOrUpdateVpcNatGatewayQueue = newTypedRateLimitingQueue[string]("AddOrUpdateVpcNatGw", nil)
	t.Cleanup(c.updateQoSPolicyQueue.ShutDown)
	t.Cleanup(c.updateIptablesEipQueue.ShutDown)
	t.Cleanup(c.addOrUpdateVpcNatGatewayQueue.ShutDown)

	t.Run("terminating is authoritative invalidation", func(t *testing.T) {
		newQos := oldQos.DeepCopy()
		now := metav1.Now()
		newQos.DeletionTimestamp = &now
		c.enqueueUpdateQoSPolicy(oldQos, newQos)
		require.Equal(t, 1, c.updateQoSPolicyQueue.Len())
		require.Equal(t, 1, c.updateIptablesEipQueue.Len())
		require.Equal(t, 1, c.addOrUpdateVpcNatGatewayQueue.Len())
	})

	t.Run("terminating add replay notifies referrers", func(t *testing.T) {
		c.updateQoSPolicyQueue = newTypedRateLimitingQueue[string]("TerminatingAddQoS", nil)
		c.updateIptablesEipQueue = newTypedRateLimitingQueue[string]("TerminatingAddEip", nil)
		c.addOrUpdateVpcNatGatewayQueue = newTypedRateLimitingQueue[string]("TerminatingAddGw", nil)
		t.Cleanup(c.updateQoSPolicyQueue.ShutDown)
		t.Cleanup(c.updateIptablesEipQueue.ShutDown)
		t.Cleanup(c.addOrUpdateVpcNatGatewayQueue.ShutDown)
		qos := oldQos.DeepCopy()
		now := metav1.Now()
		qos.DeletionTimestamp = &now
		c.enqueueAddQoSPolicy(qos)
		require.Equal(t, 1, c.updateQoSPolicyQueue.Len())
		require.Equal(t, 1, c.updateIptablesEipQueue.Len())
		require.Equal(t, 1, c.addOrUpdateVpcNatGatewayQueue.Len())
	})

	t.Run("finalizer removal invalidates referrers", func(t *testing.T) {
		c.updateQoSPolicyQueue = newTypedRateLimitingQueue[string]("FinalizerQoS", nil)
		c.updateIptablesEipQueue = newTypedRateLimitingQueue[string]("FinalizerEip", nil)
		c.addOrUpdateVpcNatGatewayQueue = newTypedRateLimitingQueue[string]("FinalizerGw", nil)
		t.Cleanup(c.updateQoSPolicyQueue.ShutDown)
		t.Cleanup(c.updateIptablesEipQueue.ShutDown)
		t.Cleanup(c.addOrUpdateVpcNatGatewayQueue.ShutDown)
		newQos := oldQos.DeepCopy()
		newQos.Finalizers = nil
		c.enqueueUpdateQoSPolicy(oldQos, newQos)
		require.Equal(t, 1, c.updateQoSPolicyQueue.Len())
		require.Equal(t, 1, c.updateIptablesEipQueue.Len())
		require.Equal(t, 1, c.addOrUpdateVpcNatGatewayQueue.Len())
	})

	t.Run("eip list failure does not block gateway notification", func(t *testing.T) {
		originalLister := c.iptablesEipsLister
		t.Cleanup(func() { c.iptablesEipsLister = originalLister })
		c.iptablesEipsLister = failingEipLister{}
		c.addOrUpdateVpcNatGatewayQueue = newTypedRateLimitingQueue[string]("ListFailureGw", nil)
		t.Cleanup(c.addOrUpdateVpcNatGatewayQueue.ShutDown)
		err := c.enqueueQoSPolicyReferrers(oldQos, false)
		require.ErrorContains(t, err, "eip lister failed")
		require.Equal(t, 1, c.addOrUpdateVpcNatGatewayQueue.Len())
	})

	t.Run("pending spec keeps the established data plane valid", func(t *testing.T) {
		c.updateIptablesEipQueue = newTypedRateLimitingQueue[string]("PendingSpecEip", nil)
		c.addOrUpdateVpcNatGatewayQueue = newTypedRateLimitingQueue[string]("PendingSpecGw", nil)
		t.Cleanup(c.updateIptablesEipQueue.ShutDown)
		t.Cleanup(c.addOrUpdateVpcNatGatewayQueue.ShutDown)
		newQos := oldQos.DeepCopy()
		newQos.Spec.BandwidthLimitRules = kubeovnv1.QoSPolicyBandwidthLimitRules{{Name: "new-rule"}}
		c.enqueueUpdateQoSPolicy(oldQos, newQos)
		require.Zero(t, c.updateIptablesEipQueue.Len())
		require.Zero(t, c.addOrUpdateVpcNatGatewayQueue.Len())
	})

	t.Run("terminating after a pending spec still invalidates", func(t *testing.T) {
		c.updateIptablesEipQueue = newTypedRateLimitingQueue[string]("PendingDeleteEip", nil)
		c.addOrUpdateVpcNatGatewayQueue = newTypedRateLimitingQueue[string]("PendingDeleteGw", nil)
		t.Cleanup(c.updateIptablesEipQueue.ShutDown)
		t.Cleanup(c.addOrUpdateVpcNatGatewayQueue.ShutDown)
		pendingQos := oldQos.DeepCopy()
		pendingQos.Spec.BandwidthLimitRules = kubeovnv1.QoSPolicyBandwidthLimitRules{{Name: "new-rule"}}
		deletingQos := pendingQos.DeepCopy()
		now := metav1.Now()
		deletingQos.DeletionTimestamp = &now
		c.enqueueUpdateQoSPolicy(pendingQos, deletingQos)
		require.Equal(t, 1, c.updateIptablesEipQueue.Len())
		require.Equal(t, 1, c.addOrUpdateVpcNatGatewayQueue.Len())
	})

	t.Run("delete event notifies referrers", func(t *testing.T) {
		c.delQoSPolicyQueue = newTypedRateLimitingQueue[string]("DeleteQoSPolicy", nil)
		c.updateIptablesEipQueue = newTypedRateLimitingQueue[string]("DeletedQoSEip", nil)
		c.addOrUpdateVpcNatGatewayQueue = newTypedRateLimitingQueue[string]("DeletedQoSGw", nil)
		t.Cleanup(c.delQoSPolicyQueue.ShutDown)
		t.Cleanup(c.updateIptablesEipQueue.ShutDown)
		t.Cleanup(c.addOrUpdateVpcNatGatewayQueue.ShutDown)
		c.enqueueDelQoSPolicy(oldQos)
		require.Equal(t, 1, c.delQoSPolicyQueue.Len())
		require.Equal(t, 1, c.updateIptablesEipQueue.Len())
		require.Equal(t, 1, c.addOrUpdateVpcNatGatewayQueue.Len())
	})

	t.Run("same-name generations are isolated by uid", func(t *testing.T) {
		oldGeneration := oldQos.DeepCopy()
		oldGeneration.UID = types.UID("old-uid")
		newGeneration := oldQos.DeepCopy()
		newGeneration.UID = types.UID("new-uid")
		for _, tc := range []struct {
			name       string
			boundUID   string
			eventQos   *kubeovnv1.QoSPolicy
			usable     bool
			wantQueued int
		}{
			{name: "old delete ignores new binding", boundUID: "new-uid", eventQos: oldGeneration, wantQueued: 0},
			{name: "new usable wakes old binding", boundUID: "old-uid", eventQos: newGeneration, usable: true, wantQueued: 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				eip := eip.DeepCopy()
				eip.Labels = map[string]string{util.QoSPolicyUIDLabel: tc.boundUID}
				gw := gw.DeepCopy()
				gw.Labels = map[string]string{util.QoSPolicyUIDLabel: tc.boundUID}
				fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
					IptablesEIPs:   []*kubeovnv1.IptablesEIP{eip},
					VpcNatGateways: []*kubeovnv1.VpcNatGateway{gw},
				})
				require.NoError(t, err)
				c := fc.fakeController
				c.updateIptablesEipQueue = newTypedRateLimitingQueue[string]("QoSGenerationEip", nil)
				c.addOrUpdateVpcNatGatewayQueue = newTypedRateLimitingQueue[string]("QoSGenerationGw", nil)
				t.Cleanup(c.updateIptablesEipQueue.ShutDown)
				t.Cleanup(c.addOrUpdateVpcNatGatewayQueue.ShutDown)

				require.NoError(t, c.enqueueQoSPolicyReferrers(tc.eventQos, tc.usable))
				require.Equal(t, tc.wantQueued, c.updateIptablesEipQueue.Len())
				require.Equal(t, tc.wantQueued, c.addOrUpdateVpcNatGatewayQueue.Len())
			})
		}
	})
}

func TestUpdateQoSPolicyRestoresMissingControllerFinalizer(t *testing.T) {
	qos := &kubeovnv1.QoSPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "qos"},
		Spec:       kubeovnv1.QoSPolicySpec{BindingType: kubeovnv1.QoSBindingTypeEIP},
		Status:     kubeovnv1.QoSPolicyStatus{BindingType: kubeovnv1.QoSBindingTypeEIP},
	}
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{QoSPolicies: []*kubeovnv1.QoSPolicy{qos}})
	require.NoError(t, err)

	require.NoError(t, fc.fakeController.handleUpdateQoSPolicy("qos"))
	stored, err := fc.fakeController.config.KubeOvnClient.KubeovnV1().QoSPolicies().Get(t.Context(), "qos", metav1.GetOptions{})
	require.NoError(t, err)
	require.Contains(t, stored.Finalizers, util.KubeOVNControllerFinalizer)
}

func TestQoSPolicyFinalizerLossEnqueuesAndRestores(t *testing.T) {
	oldQos := &kubeovnv1.QoSPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "qos", Finalizers: []string{util.KubeOVNControllerFinalizer}},
		Spec:       kubeovnv1.QoSPolicySpec{BindingType: kubeovnv1.QoSBindingTypeEIP},
		Status:     kubeovnv1.QoSPolicyStatus{BindingType: kubeovnv1.QoSBindingTypeEIP},
	}
	newQos := oldQos.DeepCopy()
	newQos.Finalizers = nil
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{QoSPolicies: []*kubeovnv1.QoSPolicy{newQos}})
	require.NoError(t, err)
	c := fc.fakeController
	c.updateQoSPolicyQueue = newTypedRateLimitingQueue[string]("QoSFinalizerRecovery", nil)
	t.Cleanup(c.updateQoSPolicyQueue.ShutDown)

	c.enqueueUpdateQoSPolicy(oldQos, newQos)
	require.Equal(t, 1, c.updateQoSPolicyQueue.Len())
	require.NoError(t, c.handleUpdateQoSPolicy("qos"))
	stored, err := c.config.KubeOvnClient.KubeovnV1().QoSPolicies().Get(t.Context(), "qos", metav1.GetOptions{})
	require.NoError(t, err)
	require.Contains(t, stored.Finalizers, util.KubeOVNControllerFinalizer)
}

func TestValidateRateValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		value     string
		fieldName string
		wantErr   bool
		errMsg    string
	}{
		{
			name:      "valid numeric value",
			value:     "100",
			fieldName: "rateMax",
			wantErr:   false,
		},
		{
			name:      "valid large numeric value",
			value:     "10000",
			fieldName: "rateMax",
			wantErr:   false,
		},
		{
			name:      "valid zero value",
			value:     "0",
			fieldName: "rateMax",
			wantErr:   false,
		},
		{
			name:      "valid decimal value",
			value:     "100.5",
			fieldName: "rateMax",
			wantErr:   false,
		},
		{
			name:      "valid small decimal value",
			value:     "0.5",
			fieldName: "rateMax",
			wantErr:   false,
		},
		{
			name:      "valid very small decimal value 0.01",
			value:     "0.01",
			fieldName: "rateMax",
			wantErr:   false,
		},
		{
			name:      "valid very small decimal value 0.001",
			value:     "0.001",
			fieldName: "rateMax",
			wantErr:   false,
		},
		{
			name:      "valid decimal burst value",
			value:     "1.25",
			fieldName: "burstMax",
			wantErr:   false,
		},
		{
			name:      "valid small decimal burst value 0.01",
			value:     "0.01",
			fieldName: "burstMax",
			wantErr:   false,
		},
		{
			name:      "empty value allowed",
			value:     "",
			fieldName: "rateMax",
			wantErr:   false,
		},
		{
			name:      "invalid - contains unit suffix",
			value:     "100Mbit",
			fieldName: "rateMax",
			wantErr:   true,
			errMsg:    "must be a positive number",
		},
		{
			name:      "invalid - contains unit suffix Mbps",
			value:     "100Mbps",
			fieldName: "rateMax",
			wantErr:   true,
			errMsg:    "must be a positive number",
		},
		{
			name:      "invalid - command injection attempt semicolon",
			value:     "100;rm -rf /",
			fieldName: "rateMax",
			wantErr:   true,
			errMsg:    "must be a positive number",
		},
		{
			name:      "invalid - command injection attempt backtick",
			value:     "100`whoami`",
			fieldName: "rateMax",
			wantErr:   true,
			errMsg:    "must be a positive number",
		},
		{
			name:      "invalid - command injection attempt $(...)",
			value:     "$(cat /etc/passwd)",
			fieldName: "rateMax",
			wantErr:   true,
			errMsg:    "must be a positive number",
		},
		{
			name:      "invalid - negative number",
			value:     "-100",
			fieldName: "rateMax",
			wantErr:   true,
			errMsg:    "must be a positive number",
		},
		{
			name:      "invalid - multiple decimal points",
			value:     "100.5.5",
			fieldName: "rateMax",
			wantErr:   true,
			errMsg:    "must be a positive number",
		},
		{
			name:      "invalid - spaces",
			value:     "100 200",
			fieldName: "rateMax",
			wantErr:   true,
			errMsg:    "must be a positive number",
		},
		{
			name:      "invalid - hex format",
			value:     "0x64",
			fieldName: "burstMax",
			wantErr:   true,
			errMsg:    "must be a positive number",
		},
		{
			name:      "invalid - trailing decimal point",
			value:     "100.",
			fieldName: "rateMax",
			wantErr:   true,
			errMsg:    "must be a positive number",
		},
		{
			name:      "invalid - leading decimal point",
			value:     ".5",
			fieldName: "rateMax",
			wantErr:   true,
			errMsg:    "must be a positive number",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateRateValue(tt.value, tt.fieldName)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				assert.Contains(t, err.Error(), tt.fieldName)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateIPMatchValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		matchValue string
		want       bool
	}{
		{
			name:       "valid src with IPv4 CIDR /32",
			matchValue: "src 192.168.1.1/32",
			want:       true,
		},
		{
			name:       "valid dst with IPv4 CIDR /32",
			matchValue: "dst 10.0.0.1/32",
			want:       true,
		},
		{
			name:       "valid src with IPv4 subnet",
			matchValue: "src 192.168.0.0/24",
			want:       true,
		},
		{
			name:       "valid dst with IPv4 subnet",
			matchValue: "dst 10.0.0.0/8",
			want:       true,
		},
		{
			name:       "valid src with IPv6 CIDR",
			matchValue: "src 2001:db8::1/128",
			want:       true,
		},
		{
			name:       "valid dst with IPv6 subnet",
			matchValue: "dst 2001:db8::/32",
			want:       true,
		},
		{
			name:       "invalid - missing direction",
			matchValue: "192.168.1.1/32",
			want:       false,
		},
		{
			name:       "invalid - wrong direction",
			matchValue: "in 192.168.1.1/32",
			want:       false,
		},
		{
			name:       "invalid - missing CIDR prefix",
			matchValue: "src 192.168.1.1",
			want:       false,
		},
		{
			name:       "invalid - malformed IP",
			matchValue: "src 192.168.1.256/32",
			want:       false,
		},
		{
			name:       "invalid - empty string",
			matchValue: "",
			want:       false,
		},
		{
			name:       "invalid - only direction",
			matchValue: "src",
			want:       false,
		},
		{
			name:       "invalid - extra parts",
			matchValue: "src 192.168.1.1/32 extra",
			want:       false,
		},
		{
			name:       "invalid - command injection in direction",
			matchValue: "src;rm 192.168.1.1/32",
			want:       false,
		},
		{
			name:       "invalid - command injection in CIDR",
			matchValue: "src 192.168.1.1/32;whoami",
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := validateIPMatchValue(tt.matchValue)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDiffQoSPolicyBandwidthLimitRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		oldList     kubeovnv1.QoSPolicyBandwidthLimitRules
		newList     kubeovnv1.QoSPolicyBandwidthLimitRules
		wantAdded   kubeovnv1.QoSPolicyBandwidthLimitRules
		wantDeleted kubeovnv1.QoSPolicyBandwidthLimitRules
		wantUpdated kubeovnv1.QoSPolicyBandwidthLimitRules
	}{
		{
			name:        "both empty lists",
			oldList:     kubeovnv1.QoSPolicyBandwidthLimitRules{},
			newList:     kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantAdded:   kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantDeleted: kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantUpdated: kubeovnv1.QoSPolicyBandwidthLimitRules{},
		},
		{
			name:    "add new rule to empty list",
			oldList: kubeovnv1.QoSPolicyBandwidthLimitRules{},
			newList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", BurstMax: "10", Direction: "egress"},
			},
			wantAdded: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", BurstMax: "10", Direction: "egress"},
			},
			wantDeleted: kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantUpdated: kubeovnv1.QoSPolicyBandwidthLimitRules{},
		},
		{
			name: "delete all rules",
			oldList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", BurstMax: "10", Direction: "egress"},
			},
			newList:   kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantAdded: kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantDeleted: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", BurstMax: "10", Direction: "egress"},
			},
			wantUpdated: kubeovnv1.QoSPolicyBandwidthLimitRules{},
		},
		{
			name: "no changes - identical rules",
			oldList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", BurstMax: "10", Direction: "egress"},
			},
			newList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", BurstMax: "10", Direction: "egress"},
			},
			wantAdded:   kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantDeleted: kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantUpdated: kubeovnv1.QoSPolicyBandwidthLimitRules{},
		},
		{
			name: "update rule - change RateMax",
			oldList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", BurstMax: "10", Direction: "egress"},
			},
			newList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "200", BurstMax: "10", Direction: "egress"},
			},
			wantAdded:   kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantDeleted: kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantUpdated: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "200", BurstMax: "10", Direction: "egress"},
			},
		},
		{
			name: "update rule - change BurstMax",
			oldList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", BurstMax: "10", Direction: "egress"},
			},
			newList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", BurstMax: "20", Direction: "egress"},
			},
			wantAdded:   kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantDeleted: kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantUpdated: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", BurstMax: "20", Direction: "egress"},
			},
		},
		{
			name: "update rule - change Direction",
			oldList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", BurstMax: "10", Direction: "egress"},
			},
			newList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", BurstMax: "10", Direction: "ingress"},
			},
			wantAdded:   kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantDeleted: kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantUpdated: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", BurstMax: "10", Direction: "ingress"},
			},
		},
		{
			name: "complex scenario - add, delete, update",
			oldList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", BurstMax: "10", Direction: "egress"},
				{Name: "rule2", RateMax: "200", BurstMax: "20", Direction: "ingress"},
				{Name: "rule3", RateMax: "300", BurstMax: "30", Direction: "egress"},
			},
			newList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "150", BurstMax: "10", Direction: "egress"},  // updated
				{Name: "rule3", RateMax: "300", BurstMax: "30", Direction: "egress"},  // unchanged
				{Name: "rule4", RateMax: "400", BurstMax: "40", Direction: "ingress"}, // added
			},
			wantAdded: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule4", RateMax: "400", BurstMax: "40", Direction: "ingress"},
			},
			wantDeleted: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule2", RateMax: "200", BurstMax: "20", Direction: "ingress"},
			},
			wantUpdated: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "150", BurstMax: "10", Direction: "egress"},
			},
		},
		{
			name: "update rule with MatchType and MatchValue",
			oldList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", MatchType: "ip", MatchValue: "src 192.168.1.0/24"},
			},
			newList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", MatchType: "ip", MatchValue: "dst 10.0.0.0/8"},
			},
			wantAdded:   kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantDeleted: kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantUpdated: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", MatchType: "ip", MatchValue: "dst 10.0.0.0/8"},
			},
		},
		{
			name: "update rule with Interface change",
			oldList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", Interface: "eth0"},
			},
			newList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", Interface: "net1"},
			},
			wantAdded:   kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantDeleted: kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantUpdated: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", Interface: "net1"},
			},
		},
		{
			name: "update rule with Priority change",
			oldList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", Priority: 1},
			},
			newList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", Priority: 2},
			},
			wantAdded:   kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantDeleted: kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantUpdated: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100", Priority: 2},
			},
		},
		{
			name:    "multiple adds",
			oldList: kubeovnv1.QoSPolicyBandwidthLimitRules{},
			newList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100"},
				{Name: "rule2", RateMax: "200"},
				{Name: "rule3", RateMax: "300"},
			},
			wantAdded: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "100"},
				{Name: "rule2", RateMax: "200"},
				{Name: "rule3", RateMax: "300"},
			},
			wantDeleted: kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantUpdated: kubeovnv1.QoSPolicyBandwidthLimitRules{},
		},
		{
			name: "decimal rate values - verify reflect.DeepEqual works correctly",
			oldList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "0.5", BurstMax: "0.1"},
			},
			newList: kubeovnv1.QoSPolicyBandwidthLimitRules{
				{Name: "rule1", RateMax: "0.5", BurstMax: "0.1"},
			},
			wantAdded:   kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantDeleted: kubeovnv1.QoSPolicyBandwidthLimitRules{},
			wantUpdated: kubeovnv1.QoSPolicyBandwidthLimitRules{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotAdded, gotDeleted, gotUpdated := diffQoSPolicyBandwidthLimitRules(tt.oldList, tt.newList)

			// For added and updated, order matters as they come from newList iteration
			assert.ElementsMatch(t, tt.wantAdded, gotAdded, "added rules mismatch")
			assert.ElementsMatch(t, tt.wantUpdated, gotUpdated, "updated rules mismatch")
			// For deleted, order may vary as it comes from map iteration
			assert.ElementsMatch(t, tt.wantDeleted, gotDeleted, "deleted rules mismatch")
		})
	}
}

func TestValidateInterfaceName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		iface   string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid interface eth0",
			iface:   "eth0",
			wantErr: false,
		},
		{
			name:    "valid interface net1",
			iface:   "net1",
			wantErr: false,
		},
		{
			name:    "valid interface with underscore",
			iface:   "bond_0",
			wantErr: false,
		},
		{
			name:    "valid interface with hyphen",
			iface:   "veth-abc",
			wantErr: false,
		},
		{
			name:    "valid max length interface (15 chars)",
			iface:   "abcdefghijklmno",
			wantErr: false,
		},
		{
			name:    "empty interface allowed",
			iface:   "",
			wantErr: false,
		},
		{
			name:    "invalid - too long (16 chars)",
			iface:   "abcdefghijklmnop",
			wantErr: true,
			errMsg:  "must be 1-15 alphanumeric",
		},
		{
			name:    "invalid - command injection with semicolon",
			iface:   "eth0;rm -rf /",
			wantErr: true,
			errMsg:  "must be 1-15 alphanumeric",
		},
		{
			name:    "invalid - command injection with backtick",
			iface:   "eth0`whoami`",
			wantErr: true,
			errMsg:  "must be 1-15 alphanumeric",
		},
		{
			name:    "invalid - command injection with $(...)",
			iface:   "$(cat /etc/passwd)",
			wantErr: true,
			errMsg:  "must be 1-15 alphanumeric",
		},
		{
			name:    "invalid - contains space",
			iface:   "eth 0",
			wantErr: true,
			errMsg:  "must be 1-15 alphanumeric",
		},
		{
			name:    "invalid - contains dot",
			iface:   "eth0.1",
			wantErr: true,
			errMsg:  "must be 1-15 alphanumeric",
		},
		{
			name:    "invalid - contains slash",
			iface:   "eth/0",
			wantErr: true,
			errMsg:  "must be 1-15 alphanumeric",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateInterfaceName(tt.iface)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateDirection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		direction kubeovnv1.QoSPolicyRuleDirection
		wantErr   bool
		errMsg    string
	}{
		{
			name:      "valid ingress",
			direction: kubeovnv1.QoSDirectionIngress,
			wantErr:   false,
		},
		{
			name:      "valid egress",
			direction: kubeovnv1.QoSDirectionEgress,
			wantErr:   false,
		},
		{
			name:      "empty direction allowed",
			direction: "",
			wantErr:   false,
		},
		{
			name:      "invalid - arbitrary string",
			direction: "invalid",
			wantErr:   true,
			errMsg:    "must be 'ingress' or 'egress'",
		},
		{
			name:      "invalid - command injection attempt",
			direction: "ingress;rm -rf /",
			wantErr:   true,
			errMsg:    "must be 'ingress' or 'egress'",
		},
		{
			name:      "invalid - case sensitive (INGRESS)",
			direction: "INGRESS",
			wantErr:   true,
			errMsg:    "must be 'ingress' or 'egress'",
		},
		{
			name:      "invalid - typo",
			direction: "ingresss",
			wantErr:   true,
			errMsg:    "must be 'ingress' or 'egress'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateDirection(tt.direction)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestQoSPolicyStatusMatchesSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		oldObj kubeovnv1.QoSPolicyBandwidthLimitRules
		newObj kubeovnv1.QoSPolicyBandwidthLimitRules
		want   bool
	}{
		{
			name:   "both nil",
			oldObj: nil,
			newObj: nil,
			want:   true,
		},
		{
			name:   "both empty",
			oldObj: kubeovnv1.QoSPolicyBandwidthLimitRules{},
			newObj: kubeovnv1.QoSPolicyBandwidthLimitRules{},
			want:   true,
		},
		{
			name:   "identical single rule",
			oldObj: kubeovnv1.QoSPolicyBandwidthLimitRules{{Name: "r1", RateMax: "100"}},
			newObj: kubeovnv1.QoSPolicyBandwidthLimitRules{{Name: "r1", RateMax: "100"}},
			want:   true,
		},
		{
			name:   "different RateMax",
			oldObj: kubeovnv1.QoSPolicyBandwidthLimitRules{{Name: "r1", RateMax: "100"}},
			newObj: kubeovnv1.QoSPolicyBandwidthLimitRules{{Name: "r1", RateMax: "200"}},
			want:   false,
		},
		{
			name:   "different length",
			oldObj: kubeovnv1.QoSPolicyBandwidthLimitRules{{Name: "r1"}},
			newObj: kubeovnv1.QoSPolicyBandwidthLimitRules{{Name: "r1"}, {Name: "r2"}},
			want:   false,
		},
		{
			name:   "same rules different order",
			oldObj: kubeovnv1.QoSPolicyBandwidthLimitRules{{Name: "r1"}, {Name: "r2"}},
			newObj: kubeovnv1.QoSPolicyBandwidthLimitRules{{Name: "r2"}, {Name: "r1"}},
			want:   true, // order-independent comparison after sorting by Name
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := (&kubeovnv1.QoSPolicy{
				Spec:   kubeovnv1.QoSPolicySpec{BandwidthLimitRules: tt.newObj},
				Status: kubeovnv1.QoSPolicyStatus{BandwidthLimitRules: tt.oldObj},
			}).StatusMatchesSpec()
			assert.Equal(t, tt.want, got)
		})
	}
}
