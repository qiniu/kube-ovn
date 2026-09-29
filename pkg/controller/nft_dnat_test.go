package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kubeovnv1 "github.com/kubeovn/kube-ovn/pkg/apis/kubeovn/v1"
	"github.com/kubeovn/kube-ovn/pkg/util"
)

func TestNftDnatMapAddRule(t *testing.T) {
	t.Parallel()

	t.Run("stateless rule sorts and dedups backends", func(t *testing.T) {
		rule, err := nftDnatMapAddRule("tcp", "10.0.0.1", "80",
			[]string{"10.0.0.6:8080", "10.0.0.5:8080", "10.0.0.6:8080"}, "", 0)
		require.NoError(t, err)
		require.Equal(t, "10.0.0.1,80,tcp,none,0,10.0.0.5:8080@10.0.0.6:8080", rule)
	})

	t.Run("client ip affinity carries its timeout", func(t *testing.T) {
		rule, err := nftDnatMapAddRule("udp", "10.0.0.2", "53",
			[]string{"10.0.0.5:5353"}, kubeovnv1.DnatSessionAffinityClientIP, 30)
		require.NoError(t, err)
		require.Equal(t, "10.0.0.2,53,udp,clientip,30,10.0.0.5:5353", rule)
	})

	t.Run("client ip affinity falls back to the default timeout", func(t *testing.T) {
		rule, err := nftDnatMapAddRule("tcp", "10.0.0.3", "443",
			[]string{"10.0.0.5:8443"}, kubeovnv1.DnatSessionAffinityClientIP, 0)
		require.NoError(t, err)
		require.Equal(t, "10.0.0.3,443,tcp,clientip,"+
			"10800,10.0.0.5:8443", rule)
	})

	t.Run("empty vip is rejected", func(t *testing.T) {
		_, err := nftDnatMapAddRule("tcp", "", "80", []string{"10.0.0.5:8080"}, "", 0)
		require.ErrorContains(t, err, "empty IPv4 EIP")
	})

	t.Run("backend less rule is rejected", func(t *testing.T) {
		_, err := nftDnatMapAddRule("tcp", "10.0.0.1", "80", nil, "", 0)
		require.ErrorContains(t, err, "no backends")
	})
}

func TestNftDnatMapDelRule(t *testing.T) {
	t.Parallel()
	require.Equal(t, "10.0.0.1,80,tcp", nftDnatMapDelRule("tcp", "10.0.0.1", "80"))
}

// TestProgramNftableLbServiceIdentitiesReprogramsUnchangedRecords pins the instance-recovery
// contract: the accounting records describe the Service's desired state, not the state of the
// current gateway instance. When a gateway Pod is replaced the records still match the desired
// state, and the Service is re-enqueued precisely so the fresh, empty instance is programmed
// again; a record-based skip would leave it without maps or hairpin rules.
func TestProgramNftableLbServiceIdentitiesReprogramsUnchangedRecords(t *testing.T) {
	t.Parallel()

	record := &kubeovnv1.IptablesDnatRule{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc.80.tcp.10.0.0.5.8080",
			Labels: map[string]string{
				util.VpcNatGatewayNameLabel: "gw",
				util.VpcDnatEPortLabel:      "80",
			},
		},
		Spec: kubeovnv1.IptablesDnatRuleSpec{
			ClusterIP:    "10.96.0.10",
			ExternalPort: "80",
			InternalIP:   "10.0.0.5",
			InternalPort: "8080",
			Protocol:     "tcp",
			Type:         kubeovnv1.DnatRuleTypeShare,
		},
	}

	programmed := map[string][]string{}
	c := &Controller{
		config: &Configuration{},
		execRulesInPod: func(_ *v1.Pod, operation string, rules []string) error {
			programmed[operation] = append(programmed[operation], rules...)
			return nil
		},
	}

	pods := []*v1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "gw-0", Namespace: metav1.NamespaceSystem}}}
	programs := buildNftableLbPrograms(map[string]*kubeovnv1.IptablesDnatRule{record.Name: record}, "")
	// The records already match the desired state, as they do right after a gateway instance is
	// replaced (the reconcile that would refresh them has not run yet).
	require.NoError(t, c.programNftableLbServiceIdentities(pods, programs, []*kubeovnv1.IptablesDnatRule{record}))

	require.Equal(t, []string{"10.96.0.10,80,tcp,none,0,10.0.0.5:8080"}, programmed[natGwNftDnatMapAdd],
		"the identity must be programmed again for the new gateway instance")
	require.Equal(t, []string{"10.96.0.10,80,tcp"}, programmed[natGwVipHairpinAdd])
	require.Empty(t, programmed[natGwNftDnatMapDel], "an identity the Service still wants must not be deleted")
	require.Empty(t, programmed[natGwVipHairpinDel])
}
