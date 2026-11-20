package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	kubeovnv1 "github.com/kubeovn/kube-ovn/pkg/apis/kubeovn/v1"
	"github.com/kubeovn/kube-ovn/pkg/util"
)

var (
	eipTrafficBytesDesc = prometheus.NewDesc("kube_ovn_eip_traffic_bytes_total",
		"Cumulative IPv4 bytes at the NAT gateway external interface, before DNAT for ingress and after SNAT for egress.",
		[]string{"eip", "ip", "nat_gateway", "direction"}, nil)
	eipTrafficMetrics         = &eipTrafficCollector{}
	registerEipTrafficMetrics = sync.OnceFunc(func() { metrics.Registry.MustRegister(eipTrafficMetrics) })
)

type eipTrafficCollector struct {
	sync.RWMutex
	samples []prometheus.Metric
}

func (*eipTrafficCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- eipTrafficBytesDesc
}

func (m *eipTrafficCollector) Collect(ch chan<- prometheus.Metric) {
	m.RLock()
	defer m.RUnlock()
	for _, sample := range m.samples {
		ch <- sample
	}
}

func (c *Controller) exportEipTrafficMetrics(ctx context.Context) {
	gateways, err := c.vpcNatGatewayLister.List(labels.Everything())
	var eips []*kubeovnv1.IptablesEIP
	if err == nil {
		eips, err = c.iptablesEipsLister.List(labels.Everything())
	}
	if err != nil {
		klog.Errorf("failed to list EIP traffic resources: %v", err)
		eipTrafficMetrics.Lock()
		eipTrafficMetrics.samples = nil
		eipTrafficMetrics.Unlock()
		return
	}
	groups := make(map[string][]*kubeovnv1.IptablesEIP)
	for _, eip := range eips {
		if eip.Status.IP != "" {
			key := c.natEipNamespace(eip) + "/" + eip.Spec.NatGwDp
			groups[key] = append(groups[key], eip)
		}
	}

	// Reconcile counters independently of EIP readiness/finalizers, including
	// empty gateways so deleted EIPs are cleaned up after controller restarts.
	results := make(chan []prometheus.Metric, len(gateways))
	workers := make(chan struct{}, 4)
	var wg sync.WaitGroup
	for _, gateway := range gateways {
		wg.Go(func() {
			select {
			case workers <- struct{}{}:
				defer func() { <-workers }()
			case <-ctx.Done():
				return
			}
			group := groups[c.natGwNamespace(gateway)+"/"+gateway.Name]
			samples, err := c.readEipTrafficMetrics(ctx, gateway, group)
			if err != nil {
				klog.Errorf("failed to collect EIP traffic for gateway %s: %v", gateway.Name, err)
				return
			}
			results <- samples
		})
	}
	wg.Wait()
	close(results)
	var samples []prometheus.Metric
	for result := range results {
		samples = append(samples, result...)
	}
	eipTrafficMetrics.Lock()
	eipTrafficMetrics.samples = samples
	eipTrafficMetrics.Unlock()
}

func (c *Controller) readEipTrafficMetrics(ctx context.Context, gateway *kubeovnv1.VpcNatGateway, eips []*kubeovnv1.IptablesEIP) ([]prometheus.Metric, error) {
	pod, err := c.getNatGwPod(gateway.Name, c.natGwNamespace(gateway))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	command := []string{"bash", vpcNatGwScriptPath, "eip-metrics-sync"}
	for _, eip := range eips {
		command = append(command, string(eip.UID)+","+eip.Status.IP)
	}
	stdout, stderr, err := util.ExecuteWithOptionsContext(ctx, c.config.KubeClient, c.config.KubeRestConfig, util.ExecOptions{
		Namespace: pod.Namespace, PodName: pod.Name, ContainerName: vpcNatGwContainerName,
		Command: command, CaptureStdout: true, CaptureStderr: true,
	})
	if err != nil {
		return nil, fmt.Errorf("read EIP counters from %s/%s: %w: %s", pod.Namespace, pod.Name, err, stderr)
	}
	return parseEipTrafficMetrics([]byte(stdout), eips)
}

func parseEipTrafficMetrics(data []byte, eips []*kubeovnv1.IptablesEIP) ([]prometheus.Metric, error) {
	var dump struct {
		Nftables []struct {
			Counter *struct {
				Name  string  `json:"name"`
				Bytes *uint64 `json:"bytes"`
			} `json:"counter"`
		} `json:"nftables"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		return nil, fmt.Errorf("parse EIP counters: %w", err)
	}
	counters := make(map[string]uint64)
	for _, item := range dump.Nftables {
		if item.Counter != nil {
			if item.Counter.Bytes == nil {
				return nil, fmt.Errorf("counter %s has no byte count", item.Counter.Name)
			}
			counters[item.Counter.Name] = *item.Counter.Bytes
		}
	}
	var samples []prometheus.Metric
	for _, eip := range eips {
		ip, err := netip.ParseAddr(eip.Status.IP)
		if err != nil || !ip.Is4() || eip.UID == "" {
			return nil, fmt.Errorf("EIP %s has invalid IPv4 address or UID", eip.Name)
		}
		for _, direction := range []string{"ingress", "egress"} {
			bytes, ok := counters[direction+"_"+strings.ReplaceAll(string(eip.UID), "-", "_")]
			if !ok {
				klog.Warningf("missing %s traffic counter for EIP %s", direction, eip.Name)
				continue
			}
			sample, err := prometheus.NewConstMetric(eipTrafficBytesDesc, prometheus.CounterValue, float64(bytes),
				eip.Name, ip.String(), eip.Spec.NatGwDp, direction)
			if err != nil {
				return nil, err
			}
			samples = append(samples, sample)
		}
	}
	return samples, nil
}
