package util

import (
	"strings"

	kubeovnv1 "github.com/kubeovn/kube-ovn/pkg/apis/kubeovn/v1"
)

// MaxTunnelKey is OVN's maximum 24-bit Datapath_Binding.tunnel_key value.
const MaxTunnelKey = 1<<24 - 1

func IsValidTunnelKey(key int) bool {
	return key > 0 && key <= MaxTunnelKey
}

// IsOvnVpcSubnet reports whether a subnet participates in the VPC tunnel-key
// guarantee. It includes default and custom VPCs; vlan is the only
// VPC/underlay discriminator after the OVN-provider check.
func IsOvnVpcSubnet(subnet *kubeovnv1.Subnet) bool {
	return subnet != nil && IsOvnProvider(subnet.Spec.Provider) && subnet.Spec.Vlan == ""
}

func IsOvnProvider(provider string) bool {
	if provider == "" || provider == OvnProvider {
		return true
	}
	if fields := strings.Split(provider, "."); len(fields) == 3 && fields[2] == OvnProvider {
		return true
	}
	return false
}

func GetNadBySubnetProvider(provider string) (nadName, nadNamespace string, existNad bool) {
	fields := strings.Split(provider, ".")
	switch {
	case len(fields) == 3 && fields[2] == OvnProvider:
		return fields[0], fields[1], true
	case len(fields) == 2:
		return fields[0], fields[1], true
	}
	return "", "", false
}
