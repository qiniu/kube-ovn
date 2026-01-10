package speaker

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"

	kubeovnv1 "github.com/kubeovn/kube-ovn/pkg/apis/kubeovn/v1"
)

func TestValidateRequiredFlags(t *testing.T) {
	tests := []struct {
		name    string
		config  *Configuration
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config with PeerWithLocal disabled",
			config: &Configuration{
				NeighborAddresses: []net.IP{net.ParseIP("10.0.0.1")},
				ClusterAs:         65001,
				NeighborAs:        65002,
				NodeName:          "node1",
				NodeRouteEIPMode:  true,
				PeerWithLocal:     false,
			},
			wantErr: false,
		},
		{
			name: "valid config with PeerWithLocal enabled",
			config: &Configuration{
				NeighborAddresses: []net.IP{net.ParseIP("10.0.0.1")},
				ClusterAs:         65001,
				NeighborAs:        65002,
				NodeName:          "node1",
				NodeRouteEIPMode:  true,
				PeerWithLocal:     true,
			},
			wantErr: false,
		},
		{
			name: "mutually exclusive modes",
			config: &Configuration{
				NeighborAddresses: []net.IP{net.ParseIP("10.0.0.1")},
				ClusterAs:         65001,
				NeighborAs:        65002,
				NatGwMode:         true,
				NodeRouteEIPMode:  true,
			},
			wantErr: true,
			errMsg:  "--nat-gw-mode and --node-route-eip-mode are mutually exclusive",
		},
		{
			name: "missing neighbor address",
			config: &Configuration{
				ClusterAs:  65001,
				NeighborAs: 65002,
			},
			wantErr: true,
			errMsg:  "at least one of --neighbor-address or --neighbor-ipv6-address must be specified",
		},
		{
			name: "missing cluster-as",
			config: &Configuration{
				NeighborAddresses: []net.IP{net.ParseIP("10.0.0.1")},
				NeighborAs:        65002,
			},
			wantErr: true,
			errMsg:  "--cluster-as must be specified",
		},
		{
			name: "missing neighbor-as",
			config: &Configuration{
				NeighborAddresses: []net.IP{net.ParseIP("10.0.0.1")},
				ClusterAs:         65001,
			},
			wantErr: true,
			errMsg:  "--neighbor-as must be specified",
		},
		{
			name: "node-route-eip-mode without node-name",
			config: &Configuration{
				NeighborAddresses: []net.IP{net.ParseIP("10.0.0.1")},
				ClusterAs:         65001,
				NeighborAs:        65002,
				NodeRouteEIPMode:  true,
				NodeName:          "",
			},
			wantErr: true,
			errMsg:  "--node-route-eip-mode requires --node-name to be specified",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.validateRequiredFlags()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetBgpLocalAddress(t *testing.T) {
	tests := []struct {
		name          string
		peerWithLocal bool
		podIPv4       net.IP
		podIPv6       net.IP
		wantIPv4      string
		wantIPv6      string
	}{
		{
			name:          "disabled returns empty string (backward compatible)",
			peerWithLocal: false,
			podIPv4:       net.ParseIP("10.244.0.5"),
			podIPv6:       net.ParseIP("fd00::5"),
			wantIPv4:      "",
			wantIPv6:      "",
		},
		{
			name:          "enabled returns Pod IP",
			peerWithLocal: true,
			podIPv4:       net.ParseIP("10.244.0.5"),
			podIPv6:       net.ParseIP("fd00::5"),
			wantIPv4:      "10.244.0.5",
			wantIPv6:      "fd00::5",
		},
		{
			name:          "enabled with nil Pod IPv4",
			peerWithLocal: true,
			podIPv4:       nil,
			podIPv6:       net.ParseIP("fd00::5"),
			wantIPv4:      "",
			wantIPv6:      "fd00::5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Configuration{
				PeerWithLocal: tt.peerWithLocal,
				PodIPs: map[string]net.IP{
					kubeovnv1.ProtocolIPv4: tt.podIPv4,
					kubeovnv1.ProtocolIPv6: tt.podIPv6,
				},
			}

			gotIPv4 := config.getBgpLocalAddress(true)
			assert.Equal(t, tt.wantIPv4, gotIPv4)

			gotIPv6 := config.getBgpLocalAddress(false)
			assert.Equal(t, tt.wantIPv6, gotIPv6)
		})
	}
}
