package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	kubevirtv1 "kubevirt.io/api/core/v1"

	"github.com/kubeovn/kube-ovn/pkg/util"
)

func newLauncherPod(namespace, name, vmiName, vmName string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels: map[string]string{
				// kubevirt sets this label to the VM name, which differs from the VMI name
				kubevirtv1.DeprecatedVirtualMachineNameLabel: vmName,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: kubevirtv1.SchemeGroupVersion.String(),
				Kind:       util.KindVirtualMachineInstance,
				Name:       vmiName,
			}},
		},
	}
}

func newControllerWithPods(t *testing.T, pods ...*v1.Pod) *Controller {
	t.Helper()

	factory := informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0)
	podInformer := factory.Core().V1().Pods()
	for _, pod := range pods {
		require.NoError(t, podInformer.Informer().GetIndexer().Add(pod))
	}
	return &Controller{podsLister: podInformer.Lister()}
}

func TestHasVMILauncherPod(t *testing.T) {
	const (
		namespace = "vm-ns"
		vmiName   = "i-6aa29f5fb09e1918583469b5"
		vmName    = "verify-live-migration-extra"
	)

	// the source and target launcher pods coexisting during a live migration
	sourcePod := newLauncherPod(namespace, "virt-launcher-"+vmiName+"-wlfqv", vmiName, vmName)
	sourcePod.Status.Phase = v1.PodSucceeded
	targetPod := newLauncherPod(namespace, "virt-launcher-"+vmiName+"-k89m5", vmiName, vmName)
	otherVMIPod := newLauncherPod(namespace, "virt-launcher-other-xxxxx", "other-vmi", "other-vm")
	plainPod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "plain"}}

	t.Run("matches the launcher pods of the VMI by owner reference", func(t *testing.T) {
		c := newControllerWithPods(t, sourcePod, targetPod, otherVMIPod, plainPod)
		found, err := c.hasVMILauncherPod(namespace, vmiName)
		require.NoError(t, err)
		require.True(t, found)
	})

	t.Run("does not rely on the vm.kubevirt.io/name label holding the VMI name", func(t *testing.T) {
		c := newControllerWithPods(t, targetPod)
		require.NotEqual(t, vmiName, targetPod.Labels[kubevirtv1.DeprecatedVirtualMachineNameLabel])
		found, err := c.hasVMILauncherPod(namespace, vmiName)
		require.NoError(t, err)
		require.True(t, found)
	})

	t.Run("returns false when no pod is owned by the VMI", func(t *testing.T) {
		c := newControllerWithPods(t, otherVMIPod, plainPod)
		found, err := c.hasVMILauncherPod(namespace, vmiName)
		require.NoError(t, err)
		require.False(t, found)
	})

	t.Run("ignores owner references of another kind", func(t *testing.T) {
		pod := newLauncherPod(namespace, "virt-launcher-fake", vmiName, vmName)
		pod.OwnerReferences[0].Kind = util.KindVirtualMachine
		c := newControllerWithPods(t, pod)
		found, err := c.hasVMILauncherPod(namespace, vmiName)
		require.NoError(t, err)
		require.False(t, found)
	})

	t.Run("ignores owner references of another api group", func(t *testing.T) {
		pod := newLauncherPod(namespace, "virt-launcher-fake", vmiName, vmName)
		pod.OwnerReferences[0].APIVersion = "example.com/v1"
		c := newControllerWithPods(t, pod)
		found, err := c.hasVMILauncherPod(namespace, vmiName)
		require.NoError(t, err)
		require.False(t, found)
	})

	t.Run("ignores pods of other namespaces", func(t *testing.T) {
		pod := newLauncherPod("another-ns", "virt-launcher-"+vmiName+"-abcde", vmiName, vmName)
		c := newControllerWithPods(t, pod)
		found, err := c.hasVMILauncherPod(namespace, vmiName)
		require.NoError(t, err)
		require.False(t, found)
	})
}
