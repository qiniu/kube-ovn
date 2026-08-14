package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// metricPodTunnelKeyRepairPatches counts successful annotation patches
	// that added, corrected or removed one or more tunnel_key (VNI) annotations.
	// It counts patch calls, not pods: a multi-NIC pod can be patched more than
	// once when another VPC subnet key is not ready yet.
	metricPodTunnelKeyRepairPatches = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pod_tunnel_key_repair_patch_total",
		Help: "Total number of annotation patches that added, corrected or removed one or more tunnel_key (VNI) annotations; a multi-NIC pod can be patched more than once.",
	})

	// metricPodTunnelKeySkipped counts provider repairs that could not
	// proceed because the subnet named by their logical_switch annotation no
	// longer exists. Providers without logical_switch (kube-ovn as IPAM only)
	// and OVN vlan/underlay subnets are not counted: they legitimately keep
	// the default tunnel key.
	metricPodTunnelKeySkipped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pod_tunnel_key_repair_skipped_total",
		Help: "Total number of tunnel_key repairs skipped because the logical_switch subnet no longer exists.",
	})
)

func registerTunnelKeyMetrics() {
	metrics.Registry.MustRegister(metricPodTunnelKeyRepairPatches)
	metrics.Registry.MustRegister(metricPodTunnelKeySkipped)
}
