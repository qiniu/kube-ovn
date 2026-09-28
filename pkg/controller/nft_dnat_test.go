package controller

import (
	"testing"

	"github.com/stretchr/testify/require"

	kubeovnv1 "github.com/kubeovn/kube-ovn/pkg/apis/kubeovn/v1"
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
