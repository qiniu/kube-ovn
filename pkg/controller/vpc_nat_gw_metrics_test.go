package controller

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	kubeovnv1 "github.com/kubeovn/kube-ovn/pkg/apis/kubeovn/v1"
	kubeovnlisters "github.com/kubeovn/kube-ovn/pkg/client/listers/kubeovn/v1"
)

func TestParseEipTrafficMetrics(t *testing.T) {
	t.Parallel()
	eip := &kubeovnv1.IptablesEIP{
		ObjectMeta: metav1.ObjectMeta{Name: "public-ip", UID: types.UID("eip-uid")},
		Spec:       kubeovnv1.IptablesEIPSpec{NatGwDp: "gateway"},
		Status:     kubeovnv1.IptablesEIPStatus{IP: "192.0.2.10"},
	}
	for _, tc := range []struct {
		name, data string
		values     []float64
		wantErr    bool
	}{
		{"traffic", `{"nftables":[{"counter":{"name":"ingress_eip_uid","bytes":1048576}},{"counter":{"name":"egress_eip_uid","bytes":2097152}},{"counter":{"name":"ingress_old_uid","bytes":999}}]}`, []float64{1048576, 2097152}, false},
		{"reset", `{"nftables":[{"counter":{"name":"ingress_eip_uid","bytes":0}},{"counter":{"name":"egress_eip_uid","bytes":1}}]}`, []float64{0, 1}, false},
		{"other generation", `{"nftables":[{"counter":{"name":"ingress_old_uid","bytes":999}}]}`, nil, false},
		{"missing counters", `{"nftables":[]}`, nil, false},
		{"invalid JSON", `not JSON`, nil, true},
		{"missing bytes", `{"nftables":[{"counter":{"name":"ingress_eip_uid"}}]}`, nil, true},
		{"negative bytes", `{"nftables":[{"counter":{"name":"ingress_eip_uid","bytes":-1}}]}`, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			samples, err := parseEipTrafficMetrics([]byte(tc.data), []*kubeovnv1.IptablesEIP{eip})
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, samples, len(tc.values))
			for i, sample := range samples {
				var metric dto.Metric
				require.NoError(t, sample.Write(&metric))
				require.Equal(t, tc.values[i], metric.GetCounter().GetValue())
				labelValues := make(map[string]string)
				for _, label := range metric.GetLabel() {
					labelValues[label.GetName()] = label.GetValue()
				}
				require.Equal(t, "public-ip", labelValues["eip"])
				require.Equal(t, "192.0.2.10", labelValues["ip"])
				require.Equal(t, "gateway", labelValues["nat_gateway"])
				require.Equal(t, []string{"ingress", "egress"}[i], labelValues["direction"])
			}
			registry := prometheus.NewRegistry()
			registry.MustRegister(&eipTrafficCollector{samples: samples})
			families, err := registry.Gather()
			require.NoError(t, err)
			if len(samples) != 0 {
				require.Equal(t, dto.MetricType_COUNTER, families[0].GetType())
			}
		})
	}
}

func TestEipTrafficMetricsRemovesDeletedEips(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	c := &Controller{
		iptablesEipsLister:  kubeovnlisters.NewIptablesEIPLister(indexer),
		vpcNatGatewayLister: kubeovnlisters.NewVpcNatGatewayLister(indexer),
	}
	eipTrafficMetrics.Lock()
	eipTrafficMetrics.samples = []prometheus.Metric{prometheus.MustNewConstMetric(eipTrafficBytesDesc,
		prometheus.CounterValue, 100, "deleted", "192.0.2.10", "gateway", "ingress")}
	eipTrafficMetrics.Unlock()
	c.exportEipTrafficMetrics(t.Context())
	registry := prometheus.NewRegistry()
	registry.MustRegister(eipTrafficMetrics)
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Empty(t, families)
}

func TestEipTrafficMetricsRetriesFailedGateway(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, "Unknown command: eip-metrics-sync", http.StatusBadRequest)
	}))
	t.Cleanup(server.Close)
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		VpcNatGateways: []*kubeovnv1.VpcNatGateway{fakeGw("gateway")},
		Pods:           []*corev1.Pod{runningNatGwPod("gateway")},
	})
	require.NoError(t, err)
	c := fc.fakeController
	c.config.KubeRestConfig = &rest.Config{Host: server.URL}
	c.config.KubeClient, err = kubernetes.NewForConfig(c.config.KubeRestConfig)
	require.NoError(t, err)
	eipTrafficMetrics.Lock()
	eipTrafficMetrics.samples = []prometheus.Metric{prometheus.MustNewConstMetric(eipTrafficBytesDesc,
		prometheus.CounterValue, 100, "deleted", "192.0.2.10", "gateway", "ingress")}
	eipTrafficMetrics.Unlock()
	for i := range 2 {
		c.exportEipTrafficMetrics(t.Context())
		require.Equal(t, int32(i+1), requests.Load())
		registry := prometheus.NewRegistry()
		registry.MustRegister(eipTrafficMetrics)
		families, err := registry.Gather()
		require.NoError(t, err)
		require.Empty(t, families)
	}
}
