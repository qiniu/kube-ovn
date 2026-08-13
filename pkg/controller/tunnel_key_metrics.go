package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// metricPodTunnelKeyMissing is the current number of allocated OVN pods
	// missing the tunnel_key (VNI) annotation, as measured by the periodic
	// resync (resyncPodTunnelKeyOnce) during the post-start backfill window.
	// A non-zero value means Cilium would fall back to the non-VPC endpoint
	// scheme for those pods until the repair worker patches them.
	metricPodTunnelKeyMissing = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pod_tunnel_key_missing",
		Help: "Number of allocated OVN pods missing the tunnel_key (VNI) annotation, measured by the periodic tunnel_key resync during the post-start backfill window.",
	})

	// metricPodTunnelKeyRepaired counts successful tunnel_key annotation
	// patches, so repair activity for the legacy-pod backfill is observable.
	metricPodTunnelKeyRepaired = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pod_tunnel_key_repair_total",
		Help: "Total number of pods whose missing tunnel_key (VNI) annotation was repaired.",
	})

	// metricPodTunnelKeySkipped counts pods the tunnel_key repair could not
	// process because their networks (NAD / default subnet) could not be
	// resolved. Such pods are otherwise silently skipped: InitIPAM continues,
	// the resync waits for the next tick.
	metricPodTunnelKeySkipped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pod_tunnel_key_repair_skipped_total",
		Help: "Total number of pods skipped by the tunnel_key repair because their networks could not be resolved.",
	})
)

func registerTunnelKeyMetrics() {
	metrics.Registry.MustRegister(metricPodTunnelKeyMissing)
	metrics.Registry.MustRegister(metricPodTunnelKeyRepaired)
	metrics.Registry.MustRegister(metricPodTunnelKeySkipped)
}
