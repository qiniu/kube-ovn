package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// metricPodTunnelKeyRepairPatches counts successful annotation patches
	// that wrote one or more tunnel_key (VNI) annotations. It counts patch
	// calls, not pods: a pod with several OVN NICs can be patched more than
	// once (partial progress when another NIC's subnet key is not synced yet).
	metricPodTunnelKeyRepairPatches = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pod_tunnel_key_repair_patch_total",
		Help: "Total number of annotation patches that wrote one or more tunnel_key (VNI) annotations; a pod with several OVN NICs can be patched more than once.",
	})

	// metricPodTunnelKeySkipped counts provider repairs that could not
	// proceed because the subnet named by their logical_switch annotation no
	// longer exists. Providers without a logical_switch annotation (subnets
	// whose provider is not ovn, i.e. kube-ovn as IPAM only) are not counted:
	// they legitimately never get a tunnel_key.
	metricPodTunnelKeySkipped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pod_tunnel_key_repair_skipped_total",
		Help: "Total number of tunnel_key repairs skipped because the logical_switch subnet no longer exists.",
	})
)

func registerTunnelKeyMetrics() {
	metrics.Registry.MustRegister(metricPodTunnelKeyRepairPatches)
	metrics.Registry.MustRegister(metricPodTunnelKeySkipped)
}
