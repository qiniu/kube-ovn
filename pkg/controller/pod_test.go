package controller

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	nadv1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	kubeovnv1 "github.com/kubeovn/kube-ovn/pkg/apis/kubeovn/v1"
	"github.com/kubeovn/kube-ovn/pkg/ipam"
	"github.com/kubeovn/kube-ovn/pkg/util"
)

// readCounter reads a metric value without pulling the prometheus testutil
// package (and its extra dependencies) into the test dependency tree.
func readCounter(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, c.Write(&m))
	return m.GetCounter().GetValue()
}

func TestCheckIsPodVpcNatGw(t *testing.T) {
	tests := []struct {
		name                string
		pod                 *corev1.Pod
		networkAttachments  []*nadv1.NetworkAttachmentDefinition
		subnets             []*kubeovnv1.Subnet
		enableNonPrimaryCNI bool
		expectedIsVpcNatGw  bool
		expectedVpcGwName   string
		description         string
	}{
		{
			name: "Pod with default provider VPC NAT gateway annotation",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						util.VpcNatGatewayAnnotation: "test-nat-gw",
					},
				},
			},
			networkAttachments:  []*nadv1.NetworkAttachmentDefinition{},
			subnets:             []*kubeovnv1.Subnet{},
			enableNonPrimaryCNI: false,
			expectedIsVpcNatGw:  true,
			expectedVpcGwName:   "test-nat-gw",
			description:         "Should detect VPC NAT gateway with default provider",
		},
		{
			name: "Pod with custom provider VPC NAT gateway annotation in non-primary CNI mode",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						// Network attachment annotation to indicate this pod uses net1
						nadv1.NetworkAttachmentAnnot: `[{"name": "net1"}]`,
						// Custom provider VPC NAT gateway annotation
						util.VpcNatGatewayAnnotation: "test-nat-gw",
						// Kube-OVN annotations for net1 provider
						fmt.Sprintf(util.LogicalSwitchAnnotationTemplate, "net1.default.ovn"): "net1-subnet",
						fmt.Sprintf(util.LogicalRouterAnnotationTemplate, "net1.default.ovn"): "net1-vpc",
						fmt.Sprintf(util.IPAddressAnnotationTemplate, "net1.default.ovn"):     "192.168.1.10",
					},
				},
			},
			networkAttachments: []*nadv1.NetworkAttachmentDefinition{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "net1",
						Namespace: "default",
					},
					Spec: nadv1.NetworkAttachmentDefinitionSpec{
						Config: `{
							"cniVersion": "0.3.1",
							"name": "net1",
							"type": "kube-ovn",
							"server_socket": "/run/openvswitch/kube-ovn-daemon.sock",
							"provider": "net1.default.ovn"
						}`,
					},
				},
			},
			subnets: []*kubeovnv1.Subnet{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "net1-subnet",
					},
					Spec: kubeovnv1.SubnetSpec{
						CIDRBlock: "192.168.1.0/24",
						Provider:  "net1.default.ovn",
					},
				},
			},
			enableNonPrimaryCNI: true,
			expectedIsVpcNatGw:  true,
			expectedVpcGwName:   "test-nat-gw",
			description:         "Should detect VPC NAT gateway with custom provider in non-primary CNI mode",
		},
		{
			name: "Pod without VPC NAT gateway annotation or with empty name",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						"other.annotation": "value",
					},
				},
			},
			networkAttachments:  []*nadv1.NetworkAttachmentDefinition{},
			subnets:             []*kubeovnv1.Subnet{},
			enableNonPrimaryCNI: false,
			expectedIsVpcNatGw:  false,
			expectedVpcGwName:   "",
			description:         "Should not detect VPC NAT gateway when annotation is missing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create controller with proper setup
			fakeController, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
				NetworkAttachments: tt.networkAttachments,
				Subnets:            tt.subnets,
				Pods:               []*corev1.Pod{tt.pod},
			})
			require.NoError(t, err, "Failed to create fake controller")
			controller := fakeController.fakeController
			// Set the non-primary CNI mode
			controller.config.EnableNonPrimaryCNI = tt.enableNonPrimaryCNI

			// Call the method under test
			isVpcNatGw, vpcGwName := controller.checkIsPodVpcNatGw(tt.pod)

			// Verify results
			assert.Equal(t, tt.expectedIsVpcNatGw, isVpcNatGw, "IsVpcNatGw mismatch: %s", tt.description)
			assert.Equal(t, tt.expectedVpcGwName, vpcGwName, "VpcGwName mismatch: %s", tt.description)
		})
	}

	// Test additional edge cases in a single sub-test for efficiency
	t.Run("Edge cases", func(t *testing.T) {
		fakeController, err := newFakeControllerWithOptions(t, nil)
		require.NoError(t, err)
		controller := fakeController.fakeController
		// Test nil pod
		isVpcNatGw, vpcGwName := controller.checkIsPodVpcNatGw(nil)
		assert.False(t, isVpcNatGw, "Nil pod should not be VPC NAT gateway")
		assert.Equal(t, "", vpcGwName, "Nil pod should have empty gateway name")

		// Test pod with empty VPC NAT gateway name
		podWithEmptyGw := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "test-pod",
				Namespace:   "default",
				Annotations: map[string]string{util.VpcNatGatewayAnnotation: ""},
			},
		}
		isVpcNatGw, vpcGwName = controller.checkIsPodVpcNatGw(podWithEmptyGw)
		assert.False(t, isVpcNatGw, "Pod with empty gateway name should not be VPC NAT gateway")
		assert.Equal(t, "", vpcGwName, "Pod with empty gateway name should return empty")

		// Test pod with no annotations
		podNoAnnotations := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "test-pod",
				Namespace:   "default",
				Annotations: nil,
			},
		}
		isVpcNatGw, vpcGwName = controller.checkIsPodVpcNatGw(podNoAnnotations)
		assert.False(t, isVpcNatGw, "Pod with no annotations should not be VPC NAT gateway")
		assert.Equal(t, "", vpcGwName, "Pod with no annotations should return empty")
	})
}

func TestBackfillVpcNatGwLanIPFromPod(t *testing.T) {
	const (
		gwName    = "test-nat-gw"
		subnet    = "nat-subnet"
		provider  = "net1.default.ovn"
		lanIP     = "10.244.0.10"
		namespace = "default"
	)

	tests := []struct {
		name                   string
		gwSpecLanIP            string
		subnetProtocol         string
		givenGwName            string
		podOwnerName           string
		podNamespace           string
		controllerPodNamespace string
		podAnnotation          map[string]string
		expectedLanIP          string
	}{
		{
			name:                   "backfill lanIP from pod annotation",
			gwSpecLanIP:            "",
			subnetProtocol:         kubeovnv1.ProtocolIPv4,
			givenGwName:            gwName,
			podOwnerName:           util.GenNatGwName(gwName),
			podNamespace:           namespace,
			controllerPodNamespace: namespace,
			podAnnotation: map[string]string{
				fmt.Sprintf(util.IPAddressAnnotationTemplate, provider): lanIP,
			},
			expectedLanIP: lanIP,
		},
		{
			name:                   "derive gateway name from owner reference",
			gwSpecLanIP:            "",
			subnetProtocol:         kubeovnv1.ProtocolIPv4,
			givenGwName:            "",
			podOwnerName:           util.GenNatGwName(gwName),
			podNamespace:           namespace,
			controllerPodNamespace: namespace,
			podAnnotation: map[string]string{
				fmt.Sprintf(util.IPAddressAnnotationTemplate, provider): lanIP,
			},
			expectedLanIP: lanIP,
		},
		{
			name:                   "skip when spec lanIP already set",
			gwSpecLanIP:            "10.244.0.99",
			subnetProtocol:         kubeovnv1.ProtocolIPv4,
			givenGwName:            gwName,
			podOwnerName:           util.GenNatGwName(gwName),
			podNamespace:           namespace,
			controllerPodNamespace: namespace,
			podAnnotation: map[string]string{
				fmt.Sprintf(util.IPAddressAnnotationTemplate, provider): lanIP,
			},
			expectedLanIP: "10.244.0.99",
		},
		{
			name:                   "backfill lanIP from pod in custom namespace",
			gwSpecLanIP:            "",
			subnetProtocol:         kubeovnv1.ProtocolIPv4,
			givenGwName:            gwName,
			podOwnerName:           util.GenNatGwName(gwName),
			podNamespace:           "other-ns",
			controllerPodNamespace: namespace,
			podAnnotation: map[string]string{
				fmt.Sprintf(util.IPAddressAnnotationTemplate, provider): lanIP,
			},
			expectedLanIP: lanIP,
		},
		{
			name:                   "skip when lanIP annotation is invalid",
			gwSpecLanIP:            "",
			subnetProtocol:         kubeovnv1.ProtocolIPv4,
			givenGwName:            gwName,
			podOwnerName:           util.GenNatGwName(gwName),
			podNamespace:           namespace,
			controllerPodNamespace: namespace,
			podAnnotation: map[string]string{
				fmt.Sprintf(util.IPAddressAnnotationTemplate, provider): "not-an-ip",
			},
			expectedLanIP: "",
		},
		{
			name:                   "prefer IPv6 address for IPv6 subnet",
			gwSpecLanIP:            "",
			subnetProtocol:         kubeovnv1.ProtocolIPv6,
			givenGwName:            gwName,
			podOwnerName:           util.GenNatGwName(gwName),
			podNamespace:           namespace,
			controllerPodNamespace: namespace,
			podAnnotation: map[string]string{
				fmt.Sprintf(util.IPAddressAnnotationTemplate, provider): "10.244.0.10,fd00:10:16::10",
			},
			expectedLanIP: "fd00:10:16::10",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw := &kubeovnv1.VpcNatGateway{
				ObjectMeta: metav1.ObjectMeta{
					Name: gwName,
				},
				Spec: kubeovnv1.VpcNatGatewaySpec{
					Vpc:    "vpc-a",
					Subnet: subnet,
					LanIP:  tt.gwSpecLanIP,
				},
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        util.GenNatGwPodName(gwName),
					Namespace:   tt.podNamespace,
					Annotations: tt.podAnnotation,
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: appsv1.SchemeGroupVersion.String(),
							Kind:       util.KindStatefulSet,
							Name:       tt.podOwnerName,
						},
					},
				},
			}

			fakeController, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
				Subnets: []*kubeovnv1.Subnet{
					{
						ObjectMeta: metav1.ObjectMeta{Name: subnet},
						Spec: kubeovnv1.SubnetSpec{
							Provider: provider,
							Protocol: tt.subnetProtocol,
						},
					},
				},
				VpcNatGateways: []*kubeovnv1.VpcNatGateway{gw},
			})
			require.NoError(t, err)

			controller := fakeController.fakeController
			controller.config.PodNamespace = tt.controllerPodNamespace
			err = controller.backfillVpcNatGwLanIPFromPod(pod, tt.givenGwName)
			require.NoError(t, err)

			gotGw, err := controller.config.KubeOvnClient.KubeovnV1().VpcNatGateways().Get(
				context.Background(), gwName, metav1.GetOptions{})
			require.NoError(t, err)
			assert.Equal(t, tt.expectedLanIP, gotGw.Spec.LanIP)
		})
	}
}

func TestGetPodKubeovnNetsNonPrimaryCNI(t *testing.T) {
	tests := []struct {
		name                string
		pod                 *corev1.Pod
		networkAttachments  []*nadv1.NetworkAttachmentDefinition
		subnets             []*kubeovnv1.Subnet
		enableNonPrimaryCNI bool
		expectedNetCount    int
		expectError         bool
		description         string
	}{
		{
			name: "Non-primary CNI mode with network attachments",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						nadv1.NetworkAttachmentAnnot: `[{"name": "net1"}]`,
						// Kube-OVN annotations for net1 provider
						fmt.Sprintf(util.LogicalSwitchAnnotationTemplate, "net1.default.ovn"): "net1-subnet",
						fmt.Sprintf(util.LogicalRouterAnnotationTemplate, "net1.default.ovn"): "net1-vpc",
						fmt.Sprintf(util.IPAddressAnnotationTemplate, "net1.default.ovn"):     "192.168.1.10",
					},
				},
			},
			networkAttachments: []*nadv1.NetworkAttachmentDefinition{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "net1",
						Namespace: "default",
					},
					Spec: nadv1.NetworkAttachmentDefinitionSpec{
						Config: `{
							"cniVersion": "0.3.1",
							"name": "net1",
							"type": "kube-ovn",
							"server_socket": "/run/openvswitch/kube-ovn-daemon.sock",
							"provider": "net1.default.ovn"
						}`,
					},
				},
			},
			subnets: []*kubeovnv1.Subnet{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "net1-subnet",
					},
					Spec: kubeovnv1.SubnetSpec{
						CIDRBlock: "192.168.1.0/24",
						Provider:  "net1.default.ovn",
					},
				},
			},
			enableNonPrimaryCNI: true,
			expectedNetCount:    1,
			expectError:         false,
			description:         "Should return only network attachment definitions in non-primary CNI mode",
		},
		{
			name: "Primary CNI mode vs Non-primary CNI behavior",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						nadv1.NetworkAttachmentAnnot: `[{"name": "net1"}]`,
						// Both custom and default provider annotations
						fmt.Sprintf(util.LogicalSwitchAnnotationTemplate, "net1.default.ovn"): "net1-subnet",
						fmt.Sprintf(util.LogicalSwitchAnnotationTemplate, util.OvnProvider):   "ovn-default",
						fmt.Sprintf(util.IPAddressAnnotationTemplate, "net1.default.ovn"):     "192.168.1.10",
						fmt.Sprintf(util.IPAddressAnnotationTemplate, util.OvnProvider):       "10.244.0.5",
					},
				},
			},
			networkAttachments: []*nadv1.NetworkAttachmentDefinition{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "net1",
						Namespace: "default",
					},
					Spec: nadv1.NetworkAttachmentDefinitionSpec{
						Config: `{
							"cniVersion": "0.3.1",
							"name": "net1",
							"type": "kube-ovn",
							"server_socket": "/run/openvswitch/kube-ovn-daemon.sock",
							"provider": "net1.default.ovn"
						}`,
					},
				},
			},
			subnets: []*kubeovnv1.Subnet{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "net1-subnet",
					},
					Spec: kubeovnv1.SubnetSpec{
						CIDRBlock: "192.168.1.0/24",
						Provider:  "net1.default.ovn",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "ovn-default",
					},
					Spec: kubeovnv1.SubnetSpec{
						CIDRBlock: "10.244.0.0/24",
						Provider:  util.OvnProvider,
						Default:   true,
					},
				},
			},
			enableNonPrimaryCNI: false, // This test will verify both modes
			expectedNetCount:    2,     // Both networks in primary mode
			expectError:         false,
			description:         "Should handle both network attachments and default network differently in primary vs non-primary modes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create controller with proper setup
			fakeController, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
				NetworkAttachments: tt.networkAttachments,
				Subnets:            tt.subnets,
				Pods:               []*corev1.Pod{tt.pod},
			})
			require.NoError(t, err, "Failed to create fake controller")
			controller := fakeController.fakeController

			// Set the non-primary CNI mode
			controller.config.EnableNonPrimaryCNI = tt.enableNonPrimaryCNI

			// Call the method under test
			nets, err := controller.getPodKubeovnNets(tt.pod)

			// Check for errors
			if tt.expectError {
				assert.Error(t, err, "Expected an error but got none: %s", tt.description)
				return
			}
			require.NoError(t, err, "Unexpected error: %s", tt.description)

			// Verify network count
			assert.Equal(t, tt.expectedNetCount, len(nets), "Network count mismatch: %s", tt.description)

			// For the comparison test, also test non-primary mode
			if tt.name == "Primary CNI mode vs Non-primary CNI behavior" {
				controller.config.EnableNonPrimaryCNI = true
				netsNonPrimary, err := controller.getPodKubeovnNets(tt.pod)
				require.NoError(t, err, "Unexpected error in non-primary mode")
				assert.Equal(t, 1, len(netsNonPrimary), "Non-primary mode should return only network attachments")
			}
		})
	}
}

func TestAcquireAddressWithSpecifiedSubnet(t *testing.T) {
	tests := []struct {
		name           string
		pod            *corev1.Pod
		namespaces     []*corev1.Namespace
		subnets        []*kubeovnv1.Subnet
		setupIPAM      func(*Controller)
		expectError    bool
		expectedSubnet string
		description    string
	}{
		{
			name: "User specifies subnet - should succeed",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						util.LogicalSwitchAnnotation: "subnet1",
						util.IPAddressAnnotation:     "10.0.1.10",
					},
				},
			},
			namespaces: []*corev1.Namespace{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "default",
						Annotations: map[string]string{
							util.LogicalSwitchAnnotation: "subnet1,subnet2",
						},
					},
				},
			},
			subnets: []*kubeovnv1.Subnet{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "subnet1"},
					Spec: kubeovnv1.SubnetSpec{
						CIDRBlock: "10.0.1.0/24",
						Protocol:  kubeovnv1.ProtocolIPv4,
						Provider:  util.OvnProvider,
					},
					Status: kubeovnv1.SubnetStatus{V4AvailableIPs: 100},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "subnet2"},
					Spec: kubeovnv1.SubnetSpec{
						CIDRBlock: "10.0.1.0/24",
						Protocol:  kubeovnv1.ProtocolIPv4,
						Provider:  util.OvnProvider,
					},
					Status: kubeovnv1.SubnetStatus{V4AvailableIPs: 100},
				},
			},
			expectError:    false,
			expectedSubnet: "subnet1",
			description:    "Should allocate from specified subnet",
		},
		{
			name: "User specifies subnet but IP occupied - should NOT fallback",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						util.LogicalSwitchAnnotation: "subnet1",
						util.IPAddressAnnotation:     "10.0.1.10",
					},
				},
			},
			namespaces: []*corev1.Namespace{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "default",
						Annotations: map[string]string{
							util.LogicalSwitchAnnotation: "subnet1,subnet2",
						},
					},
				},
			},
			subnets: []*kubeovnv1.Subnet{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "subnet1"},
					Spec: kubeovnv1.SubnetSpec{
						CIDRBlock: "10.0.1.0/24",
						Protocol:  kubeovnv1.ProtocolIPv4,
						Provider:  util.OvnProvider,
					},
					Status: kubeovnv1.SubnetStatus{V4AvailableIPs: 100},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "subnet2"},
					Spec: kubeovnv1.SubnetSpec{
						CIDRBlock: "10.0.1.0/24",
						Protocol:  kubeovnv1.ProtocolIPv4,
						Provider:  util.OvnProvider,
					},
					Status: kubeovnv1.SubnetStatus{V4AvailableIPs: 100},
				},
			},
			setupIPAM: func(c *Controller) {
				_, _, _, _ = c.ipam.GetStaticAddress("other-pod.default", "other-pod.default", "10.0.1.10", nil, "subnet1", true)
			},
			expectError: true,
			description: "Should NOT fallback to subnet2 when IP is occupied in specified subnet1",
		},
		{
			name: "No subnet specified - should try all namespace subnets",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						util.IPAddressAnnotation: "10.0.2.10",
					},
				},
			},
			namespaces: []*corev1.Namespace{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "default",
						Annotations: map[string]string{
							util.LogicalSwitchAnnotation: "subnet1,subnet2",
						},
					},
				},
			},
			subnets: []*kubeovnv1.Subnet{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "subnet1"},
					Spec: kubeovnv1.SubnetSpec{
						CIDRBlock: "10.0.1.0/24",
						Protocol:  kubeovnv1.ProtocolIPv4,
						Provider:  util.OvnProvider,
					},
					Status: kubeovnv1.SubnetStatus{V4AvailableIPs: 100},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "subnet2"},
					Spec: kubeovnv1.SubnetSpec{
						CIDRBlock: "10.0.2.0/24",
						Protocol:  kubeovnv1.ProtocolIPv4,
						Provider:  util.OvnProvider,
					},
					Status: kubeovnv1.SubnetStatus{V4AvailableIPs: 100},
				},
			},
			expectError:    false,
			expectedSubnet: "subnet2",
			description:    "Should try all subnets and find matching one when no subnet specified",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeController, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
				Namespaces: tt.namespaces,
				Subnets:    tt.subnets,
				Pods:       []*corev1.Pod{tt.pod},
			})
			require.NoError(t, err)
			controller := fakeController.fakeController
			controller.ipam = newIPAMForTest(tt.subnets)

			if tt.setupIPAM != nil {
				tt.setupIPAM(controller)
			}

			podNets, err := controller.getPodKubeovnNets(tt.pod)
			require.NoError(t, err)
			require.Greater(t, len(podNets), 0)

			_, _, _, subnet, err := controller.acquireAddress(tt.pod, podNets[0])

			if tt.expectError {
				assert.Error(t, err, tt.description)
			} else {
				require.NoError(t, err, tt.description)
				assert.Equal(t, tt.expectedSubnet, subnet.Name, tt.description)
			}
		})
	}
}

func newIPAMForTest(subnets []*kubeovnv1.Subnet) *ipam.IPAM {
	ipamInstance := ipam.NewIPAM()
	for _, subnet := range subnets {
		excludeIPs := subnet.Spec.ExcludeIps
		if len(excludeIPs) == 0 {
			excludeIPs = []string{}
		}
		s, err := ipam.NewSubnet(subnet.Name, subnet.Spec.CIDRBlock, excludeIPs)
		if err != nil {
			panic(err)
		}
		ipamInstance.Subnets[subnet.Name] = s
	}
	return ipamInstance
}

// TestReconcileAllocateSubnets_gatedOnTunnelKey verifies the core behavior of this change: a pod
// is not persisted with an IP from an OVN subnet whose tunnel key (VNI) has not been synced from
// OVN SB yet; instead the allocation returns an error so the pod is requeued.
func TestReconcileAllocateSubnets_gatedOnTunnelKey(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			Annotations: map[string]string{
				util.LogicalSwitchAnnotation: "ovn-subnet",
			},
		},
	}
	subnet := &kubeovnv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{Name: "ovn-subnet"},
		Spec: kubeovnv1.SubnetSpec{
			CIDRBlock: "10.0.1.0/24",
			Protocol:  kubeovnv1.ProtocolIPv4,
			Provider:  util.OvnProvider,
		},
		Status: kubeovnv1.SubnetStatus{V4AvailableIPs: 100, TunnelKey: 0},
	}

	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		Namespaces: []*corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "default"}}},
		Subnets:    []*kubeovnv1.Subnet{subnet},
		Pods:       []*corev1.Pod{pod},
	})
	require.NoError(t, err)
	c := fc.fakeController
	c.ipam = newIPAMForTest([]*kubeovnv1.Subnet{subnet})

	podNets, err := c.getPodKubeovnNets(pod)
	require.NoError(t, err)
	require.Greater(t, len(podNets), 0)

	_, err = c.reconcileAllocateSubnets(pod, podNets)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tunnel key not observed")

	persisted, err := c.config.KubeClient.CoreV1().Pods("default").Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEqual(t, "true", persisted.Annotations[util.AllocatedAnnotation])
	require.Empty(t, persisted.Annotations[util.IPAddressAnnotation])
	require.Empty(t, persisted.Annotations[util.TunnelKeyAnnotation])
}

// TestReconcileAllocateSubnets_allowedWhenTunnelKeySynced is the happy-path counterpart of the gate
// test: once the subnet tunnel key (VNI) is synced, allocation must proceed and record the tunnel_key
// annotation on the pod. It guards against a regression that over-blocks allocation.
func TestReconcileAllocateSubnets_allowedWhenTunnelKeySynced(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			Annotations: map[string]string{
				util.LogicalSwitchAnnotation: "ovn-subnet",
			},
		},
	}
	subnet := &kubeovnv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{Name: "ovn-subnet"},
		Spec: kubeovnv1.SubnetSpec{
			CIDRBlock: "10.0.1.0/24",
			Protocol:  kubeovnv1.ProtocolIPv4,
			Provider:  util.OvnProvider,
		},
		Status: kubeovnv1.SubnetStatus{V4AvailableIPs: 100, TunnelKey: 1234},
	}

	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		Namespaces: []*corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "default"}}},
		Subnets:    []*kubeovnv1.Subnet{subnet},
		Pods:       []*corev1.Pod{pod},
	})
	require.NoError(t, err)
	c := fc.fakeController
	c.ipam = newIPAMForTest([]*kubeovnv1.Subnet{subnet})

	// The only OVN NB call on the happy path is creating the logical switch port.
	fc.mockOvnClient.EXPECT().CreateLogicalSwitchPort(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(nil)

	podNets, err := c.getPodKubeovnNets(pod)
	require.NoError(t, err)
	require.Greater(t, len(podNets), 0)

	allocated, err := c.reconcileAllocateSubnets(pod, podNets)
	require.NoError(t, err)
	require.NotNil(t, allocated)
	tunnelKeyKey := fmt.Sprintf(util.TunnelKeyAnnotationTemplate, podNets[0].ProviderName)
	require.Equal(t, "1234", allocated.Annotations[tunnelKeyKey])
	require.Equal(t, "true", allocated.Annotations[util.AllocatedAnnotation])
	require.NotEmpty(t, allocated.Annotations[util.IPAddressAnnotation])
	require.Equal(t, subnet.Name, allocated.Annotations[util.LogicalSwitchAnnotation])

	persisted, err := c.config.KubeClient.CoreV1().Pods("default").Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, allocated.Annotations[util.IPAddressAnnotation], persisted.Annotations[util.IPAddressAnnotation])
	require.Equal(t, "1234", persisted.Annotations[util.TunnelKeyAnnotation])
	require.Equal(t, "true", persisted.Annotations[util.AllocatedAnnotation])
}

func TestReconcileAllocateSubnetsUsesResolvedSubnetForTunnelKey(t *testing.T) {
	initialSubnet := &kubeovnv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{Name: "ipam-only-subnet"},
		Spec: kubeovnv1.SubnetSpec{
			CIDRBlock: "10.0.3.0/24",
			Protocol:  kubeovnv1.ProtocolIPv4,
			Provider:  "net1.default",
		},
		Status: kubeovnv1.SubnetStatus{V4AvailableIPs: 100},
	}
	resolvedSubnet := &kubeovnv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{Name: "ovn-subnet"},
		Spec: kubeovnv1.SubnetSpec{
			CIDRBlock: "10.0.4.0/24",
			Protocol:  kubeovnv1.ProtocolIPv4,
			Provider:  util.OvnProvider,
		},
		Status: kubeovnv1.SubnetStatus{V4AvailableIPs: 100, TunnelKey: 2345},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			Annotations: map[string]string{
				util.IPAddressAnnotation: "10.0.4.5",
			},
		},
	}
	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default",
			Annotations: map[string]string{
				util.LogicalSwitchAnnotation: initialSubnet.Name + "," + resolvedSubnet.Name,
			},
		},
	}

	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		Namespaces: []*corev1.Namespace{namespace},
		Subnets:    []*kubeovnv1.Subnet{initialSubnet, resolvedSubnet},
		Pods:       []*corev1.Pod{pod},
	})
	require.NoError(t, err)
	c := fc.fakeController
	c.ipam = newIPAMForTest([]*kubeovnv1.Subnet{initialSubnet, resolvedSubnet})

	fc.mockOvnClient.EXPECT().CreateLogicalSwitchPort(
		resolvedSubnet.Name, gomock.Any(), "10.0.4.5", gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(nil)

	podNets, err := c.getPodKubeovnNets(pod)
	require.NoError(t, err)
	require.Len(t, podNets, 1)
	require.Equal(t, initialSubnet.Name, podNets[0].Subnet.Name)

	allocated, err := c.reconcileAllocateSubnets(pod, podNets)
	require.NoError(t, err)
	require.Equal(t, resolvedSubnet.Name, allocated.Annotations[util.LogicalSwitchAnnotation])
	require.Equal(t, "2345", allocated.Annotations[util.TunnelKeyAnnotation])
}

func TestReconcileAllocateSubnetsOVNVlanKeepsNicAnnotationsWithoutTunnelKey(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			Annotations: map[string]string{
				util.LogicalSwitchAnnotation: "vlan-subnet",
				util.TunnelKeyAnnotation:     "1234",
			},
		},
	}
	subnet := &kubeovnv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{Name: "vlan-subnet"},
		Spec: kubeovnv1.SubnetSpec{
			CIDRBlock: "10.0.5.0/24",
			Protocol:  kubeovnv1.ProtocolIPv4,
			Provider:  util.OvnProvider,
			Vlan:      "vlan-a",
		},
		Status: kubeovnv1.SubnetStatus{V4AvailableIPs: 100},
	}
	vlan := &kubeovnv1.Vlan{
		ObjectMeta: metav1.ObjectMeta{Name: "vlan-a"},
		Spec:       kubeovnv1.VlanSpec{ID: 100, Provider: "provider-a"},
	}

	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		Namespaces: []*corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "default"}}},
		Subnets:    []*kubeovnv1.Subnet{subnet},
		Vlans:      []*kubeovnv1.Vlan{vlan},
		Pods:       []*corev1.Pod{pod},
	})
	require.NoError(t, err)
	c := fc.fakeController
	c.config.PodNicType = util.VethType
	c.ipam = newIPAMForTest([]*kubeovnv1.Subnet{subnet})

	fc.mockOvnClient.EXPECT().CreateLogicalSwitchPort(
		subnet.Name, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(nil)

	allocated, err := c.reconcileAllocateSubnets(pod, []*kubeovnNet{{
		Type:         providerTypeOriginal,
		ProviderName: util.OvnProvider,
		Subnet:       subnet,
	}})
	require.NoError(t, err)
	require.Equal(t, subnet.Name, allocated.Annotations[util.LogicalSwitchAnnotation])
	require.Equal(t, util.VethType, allocated.Annotations[fmt.Sprintf(util.PodNicAnnotationTemplate, util.OvnProvider)])
	_, ok := allocated.Annotations[util.TunnelKeyAnnotation]
	require.False(t, ok, "OVN vlan/underlay subnet must keep the default tunnel key")
}

func TestReconcileAllocateSubnetsClearsStaleTunnelKeyForNonOVNProvider(t *testing.T) {
	const provider = "net1.default"
	logicalSwitchKey := fmt.Sprintf(util.LogicalSwitchAnnotationTemplate, provider)
	podNicKey := fmt.Sprintf(util.PodNicAnnotationTemplate, provider)
	tunnelKeyKey := fmt.Sprintf(util.TunnelKeyAnnotationTemplate, provider)
	allocatedKey := fmt.Sprintf(util.AllocatedAnnotationTemplate, provider)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			Annotations: map[string]string{
				logicalSwitchKey: "old-ovn-subnet",
				podNicKey:        util.VethType,
				tunnelKeyKey:     "1234",
			},
		},
	}
	subnet := &kubeovnv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{Name: "ipam-only-subnet"},
		Spec: kubeovnv1.SubnetSpec{
			CIDRBlock: "10.0.2.0/24",
			Protocol:  kubeovnv1.ProtocolIPv4,
			Provider:  provider,
		},
		Status: kubeovnv1.SubnetStatus{V4AvailableIPs: 100},
	}

	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		Namespaces: []*corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "default"}}},
		Subnets:    []*kubeovnv1.Subnet{subnet},
		Pods:       []*corev1.Pod{pod},
	})
	require.NoError(t, err)
	c := fc.fakeController
	c.ipam = newIPAMForTest([]*kubeovnv1.Subnet{subnet})

	allocated, err := c.reconcileAllocateSubnets(pod, []*kubeovnNet{{
		Type:         providerTypeIPAM,
		ProviderName: provider,
		Subnet:       subnet,
	}})
	require.NoError(t, err)
	require.NotNil(t, allocated)
	require.Equal(t, "true", allocated.Annotations[allocatedKey])
	_, ok := allocated.Annotations[logicalSwitchKey]
	require.False(t, ok, "non-OVN provider must not keep a stale logical_switch")
	_, ok = allocated.Annotations[podNicKey]
	require.False(t, ok, "non-OVN provider must not keep a stale pod_nic")
	_, ok = allocated.Annotations[tunnelKeyKey]
	require.False(t, ok, "non-OVN provider must not keep a stale tunnel_key")
}

func TestGetNamedPortByNsReturnsCopy(t *testing.T) {
	np := NewNamedPort()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "test-ns",
			Name:      "test-pod",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Ports: []corev1.ContainerPort{
						{Name: "http", ContainerPort: 80},
					},
				},
			},
		},
	}

	np.AddNamedPortByPod(pod)

	result := np.GetNamedPortByNs("test-ns")
	require.NotNil(t, result)
	assert.Contains(t, result, "http")

	// Mutating the returned map should not affect internal state
	delete(result, "http")

	result2 := np.GetNamedPortByNs("test-ns")
	require.NotNil(t, result2)
	assert.Contains(t, result2, "http", "internal map should not be affected by mutation of returned copy")
}

func TestDeleteNamedPortByPodWithRestartableInitContainers(t *testing.T) {
	restartAlways := corev1.ContainerRestartPolicyAlways
	np := NewNamedPort()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "test-ns",
			Name:      "test-pod",
		},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{
				{
					Name:          "sidecar",
					RestartPolicy: &restartAlways,
					Ports: []corev1.ContainerPort{
						{Name: "metrics", ContainerPort: 9090},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Ports: []corev1.ContainerPort{
						{Name: "http", ContainerPort: 80},
					},
				},
			},
		},
	}

	np.AddNamedPortByPod(pod)
	result := np.GetNamedPortByNs("test-ns")
	require.NotNil(t, result)
	assert.Contains(t, result, "http")
	assert.Contains(t, result, "metrics")

	np.DeleteNamedPortByPod(pod)
	result = np.GetNamedPortByNs("test-ns")
	assert.Empty(t, result, "both regular and sidecar init container named ports should be deleted")
}

func TestTunnelKeyNotReady(t *testing.T) {
	ovnSubnet := func(tunnelKey int) *kubeovnv1.Subnet {
		return &kubeovnv1.Subnet{
			ObjectMeta: metav1.ObjectMeta{Name: "ovn-subnet"},
			Spec:       kubeovnv1.SubnetSpec{Provider: util.OvnProvider},
			Status:     kubeovnv1.SubnetStatus{TunnelKey: tunnelKey},
		}
	}
	vlanSubnet := &kubeovnv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{Name: "vlan-subnet"},
		Spec:       kubeovnv1.SubnetSpec{Provider: util.OvnProvider, Vlan: "vlan-a"},
		Status:     kubeovnv1.SubnetStatus{TunnelKey: 0},
	}
	nonOvnSubnet := &kubeovnv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{Name: "ipam-only-subnet"},
		Spec:       kubeovnv1.SubnetSpec{Provider: "net1.default"},
		Status:     kubeovnv1.SubnetStatus{TunnelKey: 0},
	}

	tests := []struct {
		name     string
		subnet   *kubeovnv1.Subnet
		expected bool
	}{
		{"ovn subnet, zero tunnel key is not ready", ovnSubnet(0), true},
		{"ovn subnet, negative tunnel key is invalid", ovnSubnet(-1), true},
		{"ovn subnet, out-of-range tunnel key is invalid", ovnSubnet(util.MaxTunnelKey + 1), true},
		{"ovn subnet, tunnel key ready", ovnSubnet(5), false},
		{"ovn subnet, maximum tunnel key is valid", ovnSubnet(util.MaxTunnelKey), false},
		{"ovn vlan subnet keeps default tunnel key", vlanSubnet, false},
		{"non-ovn subnet is ignored", nonOvnSubnet, false},
		{"nil subnet is nil-safe", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tunnelKeyNotReady(tt.subnet))
		})
	}
}

func TestHandleRepairTunnelKey(t *testing.T) {
	ovnSubnet := func(tunnelKey int) *kubeovnv1.Subnet {
		return &kubeovnv1.Subnet{
			ObjectMeta: metav1.ObjectMeta{Name: "ovn-subnet"},
			Spec: kubeovnv1.SubnetSpec{
				CIDRBlock: "10.0.1.0/24",
				Protocol:  kubeovnv1.ProtocolIPv4,
				Provider:  util.OvnProvider,
			},
			Status: kubeovnv1.SubnetStatus{V4AvailableIPs: 100, TunnelKey: tunnelKey},
		}
	}

	ovnPod := func(tunnelKey *string) *corev1.Pod {
		annotations := map[string]string{
			util.LogicalSwitchAnnotation: "ovn-subnet",
			util.AllocatedAnnotation:     "true",
		}
		if tunnelKey != nil {
			annotations[util.TunnelKeyAnnotation] = *tunnelKey
		}
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "test-pod",
				Namespace:   "default",
				Annotations: annotations,
			},
		}
	}

	deletingPod := ovnPod(nil)
	deletingPod.DeletionTimestamp = ptr.To(metav1.Now())
	hostNetworkPod := ovnPod(nil)
	hostNetworkPod.Spec.HostNetwork = true

	tests := []struct {
		name          string
		pod           *corev1.Pod
		subnet        *kubeovnv1.Subnet
		wantErr       bool
		wantAnnotated bool
		wantValue     string
	}{
		{
			name:          "patches missing tunnel_key from subnet status",
			pod:           ovnPod(nil),
			subnet:        ovnSubnet(1234),
			wantAnnotated: true,
			wantValue:     "1234",
		},
		{
			name:          "leaves matching valid annotation untouched",
			pod:           ovnPod(ptr.To("1234")),
			subnet:        ovnSubnet(1234),
			wantAnnotated: true,
			wantValue:     "1234",
		},
		{
			name:          "repairs stale but valid annotation",
			pod:           ovnPod(ptr.To("999")),
			subnet:        ovnSubnet(1234),
			wantAnnotated: true,
			wantValue:     "1234",
		},
		{
			name:          "repairs zero annotation",
			pod:           ovnPod(ptr.To("0")),
			subnet:        ovnSubnet(1234),
			wantAnnotated: true,
			wantValue:     "1234",
		},
		{
			name:          "repairs non-numeric annotation",
			pod:           ovnPod(ptr.To("invalid")),
			subnet:        ovnSubnet(1234),
			wantAnnotated: true,
			wantValue:     "1234",
		},
		{
			name:          "repairs out-of-range annotation",
			pod:           ovnPod(ptr.To(strconv.Itoa(util.MaxTunnelKey + 1))),
			subnet:        ovnSubnet(1234),
			wantAnnotated: true,
			wantValue:     "1234",
		},
		{
			name:    "requeues when subnet tunnel key not synced yet",
			pod:     ovnPod(nil),
			subnet:  ovnSubnet(0),
			wantErr: true,
		},
		{
			name:    "requeues when subnet tunnel key is out of range",
			pod:     ovnPod(nil),
			subnet:  ovnSubnet(util.MaxTunnelKey + 1),
			wantErr: true,
		},
		{
			name: "removes stale key from OVN vlan underlay subnet",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						util.LogicalSwitchAnnotation: "vlan-subnet",
						util.AllocatedAnnotation:     "true",
						util.TunnelKeyAnnotation:     "1234",
					},
				},
			},
			subnet: &kubeovnv1.Subnet{
				ObjectMeta: metav1.ObjectMeta{Name: "vlan-subnet"},
				Spec:       kubeovnv1.SubnetSpec{Provider: util.OvnProvider, Vlan: "vlan-a"},
				Status:     kubeovnv1.SubnetStatus{TunnelKey: 0},
			},
		},
		{
			name: "removes stale key without logical switch",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						util.AllocatedAnnotation: "true",
						util.TunnelKeyAnnotation: "7",
					},
				},
			},
			subnet: &kubeovnv1.Subnet{
				ObjectMeta: metav1.ObjectMeta{Name: "unused-subnet"},
				Spec:       kubeovnv1.SubnetSpec{Provider: "net1.default"},
			},
		},
		{
			name: "removes stale key from non-ovn subnet",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						util.LogicalSwitchAnnotation: "underlay-subnet",
						util.AllocatedAnnotation:     "true",
						util.TunnelKeyAnnotation:     "7",
					},
				},
			},
			subnet: &kubeovnv1.Subnet{
				ObjectMeta: metav1.ObjectMeta{Name: "underlay-subnet"},
				Spec:       kubeovnv1.SubnetSpec{Provider: "underlay.default"},
				Status:     kubeovnv1.SubnetStatus{TunnelKey: 7},
			},
		},
		{name: "skips deleting pod", pod: deletingPod, subnet: ovnSubnet(1234)},
		{name: "skips hostNetwork pod", pod: hostNetworkPod, subnet: ovnSubnet(1234)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
				Namespaces: []*corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "default"}}},
				Subnets:    []*kubeovnv1.Subnet{tt.subnet},
				Pods:       []*corev1.Pod{tt.pod},
			})
			require.NoError(t, err)
			c := fc.fakeController

			err = c.handleRepairTunnelKey("default/test-pod")
			if tt.wantErr {
				require.Error(t, err)
				updated, getErr := c.config.KubeClient.CoreV1().Pods("default").Get(context.Background(), "test-pod", metav1.GetOptions{})
				require.NoError(t, getErr)
				_, ok := updated.Annotations[util.TunnelKeyAnnotation]
				require.False(t, ok, "invalid subnet tunnel key must never be patched")
				return
			}
			require.NoError(t, err)

			updated, err := c.config.KubeClient.CoreV1().Pods("default").Get(context.Background(), "test-pod", metav1.GetOptions{})
			require.NoError(t, err)
			if tt.wantAnnotated {
				require.Equal(t, tt.wantValue, updated.Annotations[util.TunnelKeyAnnotation])
			} else {
				_, ok := updated.Annotations[util.TunnelKeyAnnotation]
				require.False(t, ok, "tunnel_key annotation must not be added")
			}
		})
	}

	t.Run("pod not found is a no-op", func(t *testing.T) {
		fc, err := newFakeControllerWithOptions(t, nil)
		require.NoError(t, err)
		require.NoError(t, fc.fakeController.handleRepairTunnelKey("default/does-not-exist"))
	})
}

func TestHandleRepairTunnelKeyMultiNIC(t *testing.T) {
	subnets := []*kubeovnv1.Subnet{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "ovn-a"},
			Spec:       kubeovnv1.SubnetSpec{Provider: util.OvnProvider},
			Status:     kubeovnv1.SubnetStatus{TunnelKey: 1234},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "ovn-b"},
			Spec:       kubeovnv1.SubnetSpec{Provider: util.OvnProvider},
			Status:     kubeovnv1.SubnetStatus{TunnelKey: 0},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "multi-nic",
			Namespace: "default",
			Annotations: map[string]string{
				nadv1.NetworkAttachmentAnnot: "default/net1, default/net2, default/net3, default/net4",
				// NIC 1: allocated on ovn-a, missing tunnel_key -> patched
				util.AllocatedAnnotation:     "true",
				util.LogicalSwitchAnnotation: "ovn-a",
				// NIC 2: allocated on ovn-b whose key is not synced -> requeued, not patched
				"net1.ovn.kubernetes.io/allocated":      "true",
				"net1.ovn.kubernetes.io/logical_switch": "ovn-b",
				// NIC 3: allocated non-OVN NIC with no logical_switch -> silent skip, not counted
				"net2.kubernetes.io/allocated": "true",
				// NIC 4: logical_switch points to a subnet that does not exist -> skipped and counted
				"net3.ovn.kubernetes.io/allocated":      "true",
				"net3.ovn.kubernetes.io/logical_switch": "ovn-c",
				// NIC 5: has logical_switch but is NOT allocated -> skipped
				"net4.ovn.kubernetes.io/logical_switch": "ovn-a",
			},
		},
	}

	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		Namespaces: []*corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "default"}}},
		Subnets:    subnets,
		Pods:       []*corev1.Pod{pod},
	})
	require.NoError(t, err)
	c := fc.fakeController

	skippedBefore := readCounter(t, metricPodTunnelKeySkipped)
	err = c.handleRepairTunnelKey("default/multi-nic")
	require.Error(t, err, "must requeue while any allocated subnet key is not synced yet")

	updated, err := c.config.KubeClient.CoreV1().Pods("default").Get(context.Background(), "multi-nic", metav1.GetOptions{})
	require.NoError(t, err)
	// NIC 1 must be patched even though NIC 2 is not ready (partial progress lands).
	require.Equal(t, "1234", updated.Annotations[util.TunnelKeyAnnotation])
	// NIC 2 must not be patched with a stale zero key.
	_, ok := updated.Annotations["net1.ovn.kubernetes.io/tunnel_key"]
	require.False(t, ok)
	// NIC 5 (not allocated) must not be patched.
	_, ok = updated.Annotations["net4.ovn.kubernetes.io/tunnel_key"]
	require.False(t, ok)
	// Only the unresolvable logical_switch (NIC 4) counts as skipped; the
	// non-OVN NIC 3 with no logical_switch must be silent.
	require.Equal(t, float64(1), readCounter(t, metricPodTunnelKeySkipped)-skippedBefore)
}

func TestCanSkipDefaultTunnelKeyRepair(t *testing.T) {
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		Subnets: []*kubeovnv1.Subnet{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "ovn-subnet"},
				Spec:       kubeovnv1.SubnetSpec{Provider: util.OvnProvider},
				Status:     kubeovnv1.SubnetStatus{TunnelKey: 1234},
			},
			{ObjectMeta: metav1.ObjectMeta{Name: "vlan-subnet"}, Spec: kubeovnv1.SubnetSpec{Provider: util.OvnProvider, Vlan: "vlan-a"}},
		},
	})
	require.NoError(t, err)
	c := fc.fakeController

	tests := []struct {
		name        string
		annotations map[string]string
		want        bool
	}{
		{name: "unallocated pod", annotations: map[string]string{}, want: true},
		{name: "matching VPC key", annotations: map[string]string{util.AllocatedAnnotation: "true", util.LogicalSwitchAnnotation: "ovn-subnet", util.TunnelKeyAnnotation: "1234"}, want: true},
		{name: "missing VPC key", annotations: map[string]string{util.AllocatedAnnotation: "true", util.LogicalSwitchAnnotation: "ovn-subnet"}},
		{name: "stale VPC key", annotations: map[string]string{util.AllocatedAnnotation: "true", util.LogicalSwitchAnnotation: "ovn-subnet", util.TunnelKeyAnnotation: "999"}},
		{name: "vlan without key", annotations: map[string]string{util.AllocatedAnnotation: "true", util.LogicalSwitchAnnotation: "vlan-subnet"}, want: true},
		{name: "vlan stale key", annotations: map[string]string{util.AllocatedAnnotation: "true", util.LogicalSwitchAnnotation: "vlan-subnet", util.TunnelKeyAnnotation: "1234"}},
		{name: "multus pod uses full scan", annotations: map[string]string{nadv1.NetworkAttachmentAnnot: "default/net1", util.AllocatedAnnotation: "true", util.LogicalSwitchAnnotation: "ovn-subnet", util.TunnelKeyAnnotation: "1234"}},
		{name: "network status uses full scan", annotations: map[string]string{nadv1.NetworkStatusAnnot: `[{"name":"default/net1"}]`, util.AllocatedAnnotation: "true", util.LogicalSwitchAnnotation: "ovn-subnet", util.TunnelKeyAnnotation: "1234"}},
		{name: "custom default network uses full scan", annotations: map[string]string{util.DefaultNetworkAnnotation: "default/net1", util.AllocatedAnnotation: "true", util.LogicalSwitchAnnotation: "ovn-subnet", util.TunnelKeyAnnotation: "1234"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: tt.annotations}}
			require.Equal(t, tt.want, c.canSkipDefaultTunnelKeyRepair(pod))
		})
	}
}

func TestPodProvidersNeedingTunnelKeyRepair(t *testing.T) {
	fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{
		Subnets: []*kubeovnv1.Subnet{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "ovn-subnet"},
				Spec:       kubeovnv1.SubnetSpec{Provider: util.OvnProvider},
				Status:     kubeovnv1.SubnetStatus{TunnelKey: 1234},
			},
			{ObjectMeta: metav1.ObjectMeta{Name: "vlan-subnet"}, Spec: kubeovnv1.SubnetSpec{Provider: util.OvnProvider, Vlan: "vlan-a"}},
		},
	})
	require.NoError(t, err)
	c := fc.fakeController

	tests := []struct {
		name        string
		annotations map[string]string
		wantRepair  bool
	}{
		{name: "missing VPC key", annotations: map[string]string{util.AllocatedAnnotation: "true", util.LogicalSwitchAnnotation: "ovn-subnet"}, wantRepair: true},
		{name: "matching VPC key", annotations: map[string]string{util.AllocatedAnnotation: "true", util.LogicalSwitchAnnotation: "ovn-subnet", util.TunnelKeyAnnotation: "1234"}},
		{name: "stale VPC key", annotations: map[string]string{util.AllocatedAnnotation: "true", util.LogicalSwitchAnnotation: "ovn-subnet", util.TunnelKeyAnnotation: "999"}, wantRepair: true},
		{name: "invalid VPC key", annotations: map[string]string{util.AllocatedAnnotation: "true", util.LogicalSwitchAnnotation: "ovn-subnet", util.TunnelKeyAnnotation: "invalid"}, wantRepair: true},
		{name: "not allocated", annotations: map[string]string{util.LogicalSwitchAnnotation: "ovn-subnet"}},
		{name: "non-OVN without key", annotations: map[string]string{util.AllocatedAnnotation: "true"}},
		{name: "non-OVN stale key", annotations: map[string]string{util.AllocatedAnnotation: "true", util.TunnelKeyAnnotation: "1234"}, wantRepair: true},
		{name: "vlan without key", annotations: map[string]string{util.AllocatedAnnotation: "true", util.LogicalSwitchAnnotation: "vlan-subnet"}},
		{name: "vlan stale key", annotations: map[string]string{util.AllocatedAnnotation: "true", util.LogicalSwitchAnnotation: "vlan-subnet", util.TunnelKeyAnnotation: "1234"}, wantRepair: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: tt.annotations}}
			require.Equal(t, tt.wantRepair, len(c.podProvidersNeedingTunnelKeyRepair(pod)) != 0)
		})
	}
}

func TestRepairPodTunnelKeyOnStartup(t *testing.T) {
	pod := func(name, subnet string, phase corev1.PodPhase, sts bool) *corev1.Pod {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
				Annotations: map[string]string{
					util.AllocatedAnnotation:     "true",
					util.LogicalSwitchAnnotation: subnet,
				},
			},
			Status: corev1.PodStatus{Phase: phase},
		}
		if sts {
			p.OwnerReferences = []metav1.OwnerReference{{Kind: util.KindStatefulSet, Name: "sts", APIVersion: appsv1.SchemeGroupVersion.String()}}
		}
		return p
	}

	tests := []struct {
		name       string
		pod        *corev1.Pod
		tunnelKey  int
		wantKey    string
		wantQueued bool
	}{
		{name: "repairs synchronously when key is ready", pod: pod("ready", "ready-subnet", corev1.PodRunning, false), tunnelKey: 1234, wantKey: "1234"},
		{name: "defers until subnet worker provides key", pod: pod("deferred", "deferred-subnet", corev1.PodRunning, false), wantQueued: true},
		{name: "repairs non-alive STS selected by InitIPAM", pod: pod("sts-0", "ready-subnet", corev1.PodSucceeded, true), tunnelKey: 1234, wantKey: "1234"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subnet := &kubeovnv1.Subnet{
				ObjectMeta: metav1.ObjectMeta{Name: tt.pod.Annotations[util.LogicalSwitchAnnotation]},
				Spec:       kubeovnv1.SubnetSpec{Provider: util.OvnProvider},
				Status:     kubeovnv1.SubnetStatus{TunnelKey: tt.tunnelKey},
			}
			fc, err := newFakeControllerWithOptions(t, &FakeControllerOptions{Subnets: []*kubeovnv1.Subnet{subnet}, Pods: []*corev1.Pod{tt.pod}})
			require.NoError(t, err)
			c := fc.fakeController

			providers := c.podProvidersNeedingTunnelKeyRepair(tt.pod)
			require.NotEmpty(t, providers)
			c.repairPodTunnelKeyOnStartup(tt.pod, providers)

			updated, err := c.config.KubeClient.CoreV1().Pods("default").Get(context.Background(), tt.pod.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, tt.wantKey, updated.Annotations[util.TunnelKeyAnnotation])
			if tt.wantQueued {
				require.Equal(t, 1, c.repairTunnelKeyQueue.Len())
			} else {
				require.Zero(t, c.repairTunnelKeyQueue.Len())
			}
		})
	}
}
