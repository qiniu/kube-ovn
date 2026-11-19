package controller

import (
	"context"
	"testing"

	"github.com/scylladb/go-set/strset"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kubeovnv1 "github.com/kubeovn/kube-ovn/pkg/apis/kubeovn/v1"
	"github.com/kubeovn/kube-ovn/pkg/ovs"
	"github.com/kubeovn/kube-ovn/pkg/ovsdb/ovnnb"
	"github.com/kubeovn/kube-ovn/pkg/util"
)

func newLogicalRouterPort(lrName, lrpName, mac string, networks []string) *ovnnb.LogicalRouterPort {
	return &ovnnb.LogicalRouterPort{
		Name:     lrpName,
		MAC:      mac,
		Networks: networks,
		ExternalIDs: map[string]string{
			"lr":     lrName,
			"vendor": util.CniTypeName,
		},
	}
}

func Test_logicalRouterPortFilter(t *testing.T) {
	t.Parallel()

	exceptPeerPorts := strset.New(
		"except-lrp-0",
		"except-lrp-1",
	)

	lrpNames := []string{"other-0", "other-1", "other-2", "except-lrp-0", "except-lrp-1"}
	lrps := make([]*ovnnb.LogicalRouterPort, 0)
	for _, lrpName := range lrpNames {
		lrp := newLogicalRouterPort("", lrpName, "", nil)
		peer := lrpName + "-peer"
		lrp.Peer = &peer
		lrps = append(lrps, lrp)
	}

	filterFunc := logicalRouterPortFilter(exceptPeerPorts)

	for _, lrp := range lrps {
		if exceptPeerPorts.Has(lrp.Name) {
			require.False(t, filterFunc(lrp))
		} else {
			require.True(t, filterFunc(lrp))
		}
	}
}

// gcIP is the backstop for IP CRs whose logical switch port is already gone, so markAndCleanLSP
// can never see them (qbox/las#5154). It must not touch reserved IPs: those are created by users
// to hold an address for a pod that does not exist yet.
func TestGcIP(t *testing.T) {
	const (
		namespace = "ns"
		subnet    = "ovn-default"
	)
	ipCR := func(name, podName, podType, reserved string) *kubeovnv1.IP {
		return &kubeovnv1.IP{
			ObjectMeta: metav1.ObjectMeta{
				Name:   name,
				Labels: map[string]string{util.IPReservedLabel: reserved},
			},
			Spec: kubeovnv1.IPSpec{
				PodName:   podName,
				Namespace: namespace,
				Subnet:    subnet,
				PodType:   podType,
			},
		}
	}
	orphan := ipCR(ovs.PodNameToPortName("sts-0", namespace, util.OvnProvider), "sts-0", util.KindStatefulSet, "false")
	reserved := ipCR(ovs.PodNameToPortName("future-0", namespace, util.OvnProvider), "future-0", util.KindStatefulSet, "true")
	live := ipCR(ovs.PodNameToPortName("alive-0", namespace, util.OvnProvider), "alive-0", util.KindStatefulSet, "false")

	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		Namespaces: []*corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: namespace}}},
		Subnets: []*kubeovnv1.Subnet{{
			ObjectMeta: metav1.ObjectMeta{Name: subnet},
			Spec:       kubeovnv1.SubnetSpec{Provider: util.OvnProvider, Vpc: "ovn-cluster"},
		}},
		StatefulSets: []*appsv1.StatefulSet{{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "alive"},
		}},
		IPs: []*kubeovnv1.IP{orphan, reserved, live},
	})
	require.NoError(t, err)
	c := fc.fakeController
	require.NoError(t, c.ipam.AddOrUpdateSubnet(subnet, "10.0.0.0/24", "10.0.0.1", nil))

	require.NoError(t, c.gcIP())

	_, err = c.config.KubeOvnClient.KubeovnV1().IPs().Get(context.Background(), orphan.Name, metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err), "an ip whose statefulset is gone must be collected")
	_, err = c.config.KubeOvnClient.KubeovnV1().IPs().Get(context.Background(), reserved.Name, metav1.GetOptions{})
	require.NoError(t, err, "a reserved ip must survive even without its owner")
	_, err = c.config.KubeOvnClient.KubeovnV1().IPs().Get(context.Background(), live.Name, metav1.GetOptions{})
	require.NoError(t, err, "an ip of a live statefulset must survive")
}
