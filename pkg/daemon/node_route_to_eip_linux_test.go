package daemon

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kubeovnv1 "github.com/kubeovn/kube-ovn/pkg/apis/kubeovn/v1"
	"github.com/kubeovn/kube-ovn/pkg/util"
)

func TestGenerateMacvlanName(t *testing.T) {
	tests := []struct {
		name     string
		cidr     string
		expected string
	}{
		{
			name:     "typical IPv4 CIDR",
			cidr:     "192.168.1.0/24",
			expected: "macc0a80100", // 192=0xc0, 168=0xa8, 1=0x01, 0=0x00
		},
		{
			name:     "class A network",
			cidr:     "10.0.0.0/8",
			expected: "mac0a000000", // 10=0x0a, 0=0x00, 0=0x00, 0=0x00
		},
		{
			name:     "class B network",
			cidr:     "172.16.0.0/16",
			expected: "macac100000", // 172=0xac, 16=0x10, 0=0x00, 0=0x00
		},
		{
			name:     "class C network",
			cidr:     "192.168.100.0/24",
			expected: "macc0a86400", // 192=0xc0, 168=0xa8, 100=0x64, 0=0x00
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := generateMacvlanName(tt.cidr)
			require.NoError(t, err)
			assert.LessOrEqual(t, len(result), 15, "interface name should not exceed 15 chars")
			assert.True(t, strings.HasPrefix(result, "mac"), "should have 'mac' prefix")
			assert.Equal(t, tt.expected, result, "should match expected name")
			// Verify consistent output
			result2, err := generateMacvlanName(tt.cidr)
			require.NoError(t, err)
			assert.Equal(t, result, result2, "same input should produce same output")
		})
	}

	// Verify uniqueness for different CIDRs - including cases that would collide with old implementation
	t.Run("different CIDRs produce different outputs", func(t *testing.T) {
		names := make(map[string]string)
		// Include CIDRs that would have collided with simple digit concatenation
		inputs := []string{
			"192.168.1.0/24",
			"192.168.2.0/24",
			"10.0.0.0/8",
			"172.16.0.0/16",
			"19.216.81.0/24", // Would collide with 192.168.1.0 using old method
			"1.92.168.10/24", // Would collide with 192.168.1.0 using old method
		}
		for _, input := range inputs {
			result, err := generateMacvlanName(input)
			require.NoError(t, err)
			if existing, ok := names[result]; ok {
				t.Errorf("collision detected: %q and %q both produce %q", existing, input, result)
			}
			names[result] = input
		}
	})

	// Verify error returned for invalid or IPv6 CIDRs
	t.Run("returns error for invalid CIDR", func(t *testing.T) {
		_, err := generateMacvlanName("invalid")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a valid IPv4")
	})

	t.Run("returns error for IPv6 CIDR", func(t *testing.T) {
		_, err := generateMacvlanName("fd00::/64")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a valid IPv4")
	})
}

func TestParseEIPDestination(t *testing.T) {
	tests := []struct {
		name        string
		eip         string
		wantMask    int
		wantErr     bool
		errContains string
	}{
		{
			name:     "valid IPv4",
			eip:      "192.168.1.100",
			wantMask: 32,
			wantErr:  false,
		},
		{
			name:     "valid IPv6",
			eip:      "2001:db8::1",
			wantMask: 128,
			wantErr:  false,
		},
		{
			name:        "invalid IP",
			eip:         "invalid",
			wantErr:     true,
			errContains: "invalid EIP address",
		},
		{
			name:        "empty string",
			eip:         "",
			wantErr:     true,
			errContains: "invalid EIP address",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst, err := parseEIPDestination(tt.eip)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, dst)
			ones, _ := dst.Mask.Size()
			assert.Equal(t, tt.wantMask, ones)
		})
	}
}

func TestShouldEnqueueIptablesEip(t *testing.T) {
	tests := []struct {
		name           string
		externalSubnet string
		ready          bool
		want           bool
	}{
		{
			name:           "ready with ExternalSubnet",
			externalSubnet: "external-subnet",
			ready:          true,
			want:           true,
		},
		{
			name:           "ready without ExternalSubnet",
			externalSubnet: "",
			ready:          true,
			want:           false,
		},
		{
			name:           "not ready with ExternalSubnet",
			externalSubnet: "external-subnet",
			ready:          false,
			want:           false,
		},
		{
			name:           "not ready without ExternalSubnet",
			externalSubnet: "",
			ready:          false,
			want:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eip := &kubeovnv1.IptablesEIP{
				Spec: kubeovnv1.IptablesEIPSpec{
					ExternalSubnet: tt.externalSubnet,
				},
				Status: kubeovnv1.IptablesEIPStatus{
					Ready: tt.ready,
				},
			}
			assert.Equal(t, tt.want, shouldEnqueueIptablesEip(eip))
		})
	}
}

func TestIsVpcNatGwPod(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{
			name:   "NAT GW pod",
			labels: map[string]string{util.VpcNatGatewayLabel: "true"},
			want:   true,
		},
		{
			name:   "NAT GW pod with extra labels",
			labels: map[string]string{util.VpcNatGatewayLabel: "true", "app": "test"},
			want:   true,
		},
		{
			name:   "not NAT GW pod - label false",
			labels: map[string]string{util.VpcNatGatewayLabel: "false"},
			want:   false,
		},
		{
			name:   "not NAT GW pod - no label",
			labels: map[string]string{"app": "test"},
			want:   false,
		},
		{
			name:   "not NAT GW pod - nil labels",
			labels: nil,
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Labels: tt.labels},
			}
			assert.Equal(t, tt.want, isVpcNatGwPod(pod))
		})
	}
}

func TestGetNatGwNameFromPod(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   string
	}{
		{
			name:   "has NAT GW name label",
			labels: map[string]string{util.VpcNatGatewayNameLabel: "my-nat-gw"},
			want:   "my-nat-gw",
		},
		{
			name:   "no NAT GW name label",
			labels: map[string]string{"app": "test"},
			want:   "",
		},
		{
			name:   "nil labels",
			labels: nil,
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Labels: tt.labels},
			}
			assert.Equal(t, tt.want, getNatGwNameFromPod(pod))
		})
	}
}

func TestGetIPv4CIDR(t *testing.T) {
	tests := []struct {
		name      string
		cidrBlock string
		want      string
	}{
		{
			name:      "IPv4 only",
			cidrBlock: "192.168.1.0/24",
			want:      "192.168.1.0/24",
		},
		{
			name:      "IPv6 only",
			cidrBlock: "2001:db8::/64",
			want:      "",
		},
		{
			name:      "dual-stack IPv4 first",
			cidrBlock: "192.168.1.0/24,2001:db8::/64",
			want:      "192.168.1.0/24",
		},
		{
			name:      "dual-stack IPv6 first",
			cidrBlock: "2001:db8::/64,192.168.1.0/24",
			want:      "192.168.1.0/24",
		},
		{
			name:      "empty CIDR",
			cidrBlock: "",
			want:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subnet := &kubeovnv1.Subnet{
				Spec: kubeovnv1.SubnetSpec{
					CIDRBlock: tt.cidrBlock,
				},
			}
			assert.Equal(t, tt.want, getIPv4CIDR(subnet))
		})
	}
}
