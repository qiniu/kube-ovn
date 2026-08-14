package daemon

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	kubeovnv1 "github.com/kubeovn/kube-ovn/pkg/apis/kubeovn/v1"
	kubeovnlister "github.com/kubeovn/kube-ovn/pkg/client/listers/kubeovn/v1"
	"github.com/kubeovn/kube-ovn/pkg/util"
)

func TestPodTunnelKeyReady(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, subnet := range []*kubeovnv1.Subnet{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "vpc-subnet"},
			Spec:       kubeovnv1.SubnetSpec{Provider: util.OvnProvider},
			Status:     kubeovnv1.SubnetStatus{TunnelKey: 1234},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "vlan-subnet"},
			Spec:       kubeovnv1.SubnetSpec{Provider: util.OvnProvider, Vlan: "vlan-a"},
		},
	} {
		if err := indexer.Add(subnet); err != nil {
			t.Fatal(err)
		}
	}
	csh := cniServerHandler{Controller: &Controller{
		subnetsLister: kubeovnlister.NewSubnetLister(indexer),
	}}
	pod := func(subnet, tunnelKey string) *corev1.Pod {
		annotations := map[string]string{}
		if subnet != "" {
			annotations[util.LogicalSwitchAnnotation] = subnet
		}
		if tunnelKey != "" {
			annotations[util.TunnelKeyAnnotation] = tunnelKey
		}
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: annotations}}
	}

	tests := []struct {
		name     string
		pod      *corev1.Pod
		provider string
		want     bool
		wantErr  bool
	}{
		{name: "OVN provider without logical switch", pod: pod("", ""), provider: util.OvnProvider, want: false},
		{name: "non-OVN provider without logical switch", pod: pod("", ""), provider: "net1.default", want: true},
		{name: "VPC key matches subnet status", pod: pod("vpc-subnet", "1234"), provider: util.OvnProvider, want: true},
		{name: "VPC key missing", pod: pod("vpc-subnet", ""), provider: util.OvnProvider, want: false},
		{name: "VPC key zero", pod: pod("vpc-subnet", "0"), provider: util.OvnProvider, want: false},
		{name: "VPC key non-numeric", pod: pod("vpc-subnet", "invalid"), provider: util.OvnProvider, want: false},
		{name: "VPC key differs from subnet status", pod: pod("vpc-subnet", "999"), provider: util.OvnProvider, want: false},
		{name: "vlan subnet needs no key", pod: pod("vlan-subnet", ""), provider: util.OvnProvider, want: true},
		{name: "missing subnet", pod: pod("missing-subnet", "1234"), provider: util.OvnProvider, want: false, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := csh.podTunnelKeyReady(tt.pod, tt.provider)
			if (err != nil) != tt.wantErr {
				t.Fatalf("podTunnelKeyReady() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("podTunnelKeyReady() = %v, want %v", got, tt.want)
			}
		})
	}
}
