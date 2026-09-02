package controller

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"sort"
	"strings"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	kubeovnv1 "github.com/kubeovn/kube-ovn/pkg/apis/kubeovn/v1"
	"github.com/kubeovn/kube-ovn/pkg/util"
)

func (c *Controller) enqueueAddQoSPolicy(obj any) {
	qos := obj.(*kubeovnv1.QoSPolicy)
	key := cache.MetaObjectToName(qos).String()
	// A policy already marked for deletion must go through the update reconcile so its finalizer
	// can be released; handleAddQoSPolicy does not process terminating policies. This also covers
	// controller restart, where the informer re-lists objects already in their final state.
	if enqueueUpdateIfTerminating(c.updateQoSPolicyQueue, key, "qos", qos.DeletionTimestamp) {
		if err := c.enqueueQoSPolicyReferrers(qos, false); err != nil {
			klog.Errorf("failed to enqueue referrers of terminating qos policy %s: %v", key, err)
		}
		return
	}
	klog.V(3).Infof("enqueue add qos policy %s", key)
	c.addQoSPolicyQueue.Add(key)
}

// enqueueQoSPolicyReferrers wakes pending bindings when usable, or every live referrer when the
// policy is authoritatively unavailable.
func (c *Controller) enqueueQoSPolicyReferrers(qos *kubeovnv1.QoSPolicy, usable bool) error {
	var errs []error
	qosName := qos.Name
	qosUID := string(qos.UID)
	eips, err := c.iptablesEipsLister.List(labels.Everything())
	if err != nil {
		errs = append(errs, fmt.Errorf("failed to list eips referencing qos policy %s: %w", qosName, err))
	} else {
		for _, eip := range eips {
			if eip.DeletionTimestamp.IsZero() && eip.Spec.QoSPolicy == qosName {
				boundUID := eip.Labels[util.QoSPolicyUIDLabel]
				if !usable && boundUID != "" && boundUID != qosUID {
					continue
				}
				switch {
				case eip.Status.IP == "":
					c.addIptablesEipQueue.Add(eip.Name)
				case !usable || !eip.Status.Ready || eip.Status.QoSPolicy != qosName || boundUID != qosUID:
					c.updateIptablesEipQueue.Add(eip.Name)
				}
			}
		}
	}

	gateways, err := c.vpcNatGatewayLister.List(labels.Everything())
	if err != nil {
		errs = append(errs, fmt.Errorf("failed to list vpc nat gateways referencing qos policy %s: %w", qosName, err))
	} else {
		for _, gateway := range gateways {
			boundUID := gateway.Labels[util.QoSPolicyUIDLabel]
			if gateway.DeletionTimestamp.IsZero() && gateway.Spec.QoSPolicy == qosName &&
				(usable || boundUID == "" || boundUID == qosUID) &&
				(!usable || gateway.Status.QoSPolicy != qosName || boundUID != qosUID) {
				c.addOrUpdateVpcNatGatewayQueue.Add(gateway.Name)
			}
		}
	}
	return errors.Join(errs...)
}

// enqueueQoSPolicyRelease re-enqueues a QoS policy whose QoSLabel a referencing resource (EIP or
// NatGw) no longer carries, so a policy marked for deletion can drop its finalizer once unused.
// The in-use check selects on util.QoSPolicyUIDLabel, but the queue key is the policy name, so the
// re-enqueue keys on the old value of the name label. It is triggered from the delete handler (old
// labels only, newLabels nil) and from the update handler when the label is cleared or switched;
// both fire only after the informer
// cache already reflects the change, which avoids the stale-cache race that left policies stuck in
// Terminating.
func (c *Controller) enqueueQoSPolicyRelease(oldLabels, newLabels map[string]string) {
	if oldQoS := oldLabels[util.QoSLabel]; oldQoS != "" && oldQoS != newLabels[util.QoSLabel] {
		c.updateQoSPolicyQueue.Add(oldQoS)
	}
}

func compareQoSPolicyBandwidthLimitRules(oldObj, newObj kubeovnv1.QoSPolicyBandwidthLimitRules) bool {
	if len(oldObj) != len(newObj) {
		return false
	}

	// Sort both slices by Name for order-independent comparison
	// We need to sort copies to avoid mutating the original slices
	sortedOld := make(kubeovnv1.QoSPolicyBandwidthLimitRules, len(oldObj))
	sortedNew := make(kubeovnv1.QoSPolicyBandwidthLimitRules, len(newObj))
	copy(sortedOld, oldObj)
	copy(sortedNew, newObj)

	sort.Slice(sortedOld, func(i, j int) bool {
		return sortedOld[i].Name < sortedOld[j].Name
	})
	sort.Slice(sortedNew, func(i, j int) bool {
		return sortedNew[i].Name < sortedNew[j].Name
	})
	return reflect.DeepEqual(sortedOld, sortedNew)
}

func qosPolicyStatusMatchesSpec(qos *kubeovnv1.QoSPolicy) bool {
	return qos.Status.Shared == qos.Spec.Shared &&
		qos.Status.BindingType == qos.Spec.BindingType &&
		compareQoSPolicyBandwidthLimitRules(qos.Status.BandwidthLimitRules, qos.Spec.BandwidthLimitRules)
}

func qosPolicyUsable(qos *kubeovnv1.QoSPolicy) bool {
	return qos.DeletionTimestamp.IsZero() &&
		controllerutil.ContainsFinalizer(qos, util.KubeOVNControllerFinalizer) &&
		qosPolicyStatusMatchesSpec(qos)
}

func (c *Controller) enqueueUpdateQoSPolicy(oldObj, newObj any) {
	oldQos := oldObj.(*kubeovnv1.QoSPolicy)
	newQos := newObj.(*kubeovnv1.QoSPolicy)
	key := cache.MetaObjectToName(newQos).String()
	if !newQos.DeletionTimestamp.IsZero() {
		if err := c.enqueueQoSPolicyReferrers(newQos, false); err != nil {
			klog.Errorf("failed to enqueue referrers of terminating qos policy %s: %v", key, err)
		}
		klog.V(3).Infof("enqueue update to clean qos %s", key)
		c.updateQoSPolicyQueue.Add(key)
		return
	}
	if qosPolicyUsable(oldQos) && !controllerutil.ContainsFinalizer(newQos, util.KubeOVNControllerFinalizer) {
		if err := c.enqueueQoSPolicyReferrers(oldQos, false); err != nil {
			klog.Errorf("failed to enqueue referrers after qos policy %s lost its finalizer: %v", key, err)
		}
		c.updateQoSPolicyQueue.Add(key)
		return
	}
	// Wake referrers only after the informer observes the reconciled status. Enqueuing directly
	// after UpdateStatus returns would let an EIP read and apply the previous rules from the cache.
	if qosPolicyUsable(newQos) {
		if !qosPolicyUsable(oldQos) {
			if err := c.enqueueQoSPolicyReferrers(newQos, true); err != nil {
				klog.Errorf("failed to enqueue referrers of qos policy %s: %v", key, err)
			}
		}
		return
	}

	// Compare newQos.Status with newQos.Spec to check if reconciliation is needed.
	if !qosPolicyStatusMatchesSpec(newQos) {
		klog.V(3).Infof("enqueue update qos %s", key)
		c.updateQoSPolicyQueue.Add(key)
	}
}

func (c *Controller) enqueueDelQoSPolicy(obj any) {
	var qos *kubeovnv1.QoSPolicy
	switch t := obj.(type) {
	case *kubeovnv1.QoSPolicy:
		qos = t
	case cache.DeletedFinalStateUnknown:
		q, ok := t.Obj.(*kubeovnv1.QoSPolicy)
		if !ok {
			klog.Warningf("unexpected object type: %T", t.Obj)
			return
		}
		qos = q
	default:
		klog.Warningf("unexpected type: %T", obj)
		return
	}

	key := cache.MetaObjectToName(qos).String()
	klog.V(3).Infof("enqueue delete qos policy %s", key)
	c.delQoSPolicyQueue.Add(key)
	if err := c.enqueueQoSPolicyReferrers(qos, false); err != nil {
		klog.Errorf("failed to enqueue referrers of deleted qos policy %s: %v", key, err)
	}
}

func (c *Controller) handleAddQoSPolicy(key string) error {
	cachedQoS, err := c.qosPoliciesLister.Get(key)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		klog.Error(err)
		return err
	}
	// The key may have been queued while the object was still live; the update queue owns cleanup.
	if !cachedQoS.DeletionTimestamp.IsZero() {
		return nil
	}

	c.vpcNatGwKeyMutex.LockKey(key)
	defer func() { _ = c.vpcNatGwKeyMutex.UnlockKey(key) }()
	klog.Infof("handle add QoS policy %s", key)

	// Add the finalizer as soon as the policy exists so that deletion always goes through
	// the reference check, even for policies whose spec is never updated after creation.
	if err = c.handleAddQoSPolicyFinalizer(key); err != nil {
		klog.Errorf("failed to handle add finalizer for qos %s, %v", key, err)
		return err
	}

	sortedNewRules := cachedQoS.Spec.BandwidthLimitRules
	sort.Slice(sortedNewRules, func(i, j int) bool {
		return sortedNewRules[i].Name < sortedNewRules[j].Name
	})

	if qosPolicyUsable(cachedQoS) {
		return nil
	}
	if qosPolicyStatusMatchesSpec(cachedQoS) {
		// The finalizer patch above produces the informer event that proves this generation has
		// completed its first reconcile; that event wakes the referrers.
		return nil
	}
	klog.V(3).Infof("handle add qos %s", key)

	if err := c.validateQosPolicy(cachedQoS); err != nil {
		klog.Errorf("failed to validate qos %s, %v", key, err)
		return err
	}

	if err = c.patchQoSStatus(key, cachedQoS.Spec.Shared, cachedQoS.Spec.BindingType, sortedNewRules); err != nil {
		klog.Errorf("failed to patch status for qos %s, %v", key, err)
		return err
	}

	return nil
}

func (c *Controller) patchQoSStatus(
	key string, shared bool, qosType kubeovnv1.QoSPolicyBindingType, bandwidthRules kubeovnv1.QoSPolicyBandwidthLimitRules,
) error {
	oriQoS, err := c.qosPoliciesLister.Get(key)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		klog.Error(err)
		return err
	}
	qos := oriQoS.DeepCopy()
	qos.Status.Shared = shared
	qos.Status.BindingType = qosType
	qos.Status.BandwidthLimitRules = bandwidthRules
	bytes, err := qos.Status.Bytes()
	if err != nil {
		klog.Error(err)
		return err
	}
	if _, err = c.config.KubeOvnClient.KubeovnV1().QoSPolicies().Patch(context.Background(), qos.Name,
		types.MergePatchType, bytes, metav1.PatchOptions{}, "status"); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		klog.Errorf("failed to patch qos %s, %v", qos.Name, err)
		return err
	}
	return nil
}

func (c *Controller) handleDelQoSPoliciesFinalizer(key string) error {
	cachedQoSPolicies, err := c.qosPoliciesLister.Get(key)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		klog.Error(err)
		return err
	}
	if len(cachedQoSPolicies.GetFinalizers()) == 0 {
		return nil
	}
	newQoSPolicies := cachedQoSPolicies.DeepCopy()
	controllerutil.RemoveFinalizer(newQoSPolicies, util.KubeOVNControllerFinalizer)
	patch, err := util.GenerateMergePatchPayload(cachedQoSPolicies, newQoSPolicies)
	if err != nil {
		klog.Errorf("failed to generate patch payload for qos '%s', %v", cachedQoSPolicies.Name, err)
		return err
	}
	if _, err := c.config.KubeOvnClient.KubeovnV1().QoSPolicies().Patch(context.Background(), cachedQoSPolicies.Name,
		types.MergePatchType, patch, metav1.PatchOptions{}, ""); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		klog.Errorf("failed to remove finalizer from qos '%s', %v", cachedQoSPolicies.Name, err)
		return err
	}
	return nil
}

func (c *Controller) syncQoSPolicyFinalizer(cl client.Client) error {
	// migrate depreciated finalizer to new finalizer
	polices := &kubeovnv1.QoSPolicyList{}
	return migrateFinalizers(cl, polices, func(i int) (client.Object, client.Object) {
		if i < 0 || i >= len(polices.Items) {
			return nil, nil
		}
		return polices.Items[i].DeepCopy(), polices.Items[i].DeepCopy()
	})
}

func diffQoSPolicyBandwidthLimitRules(oldList, newList kubeovnv1.QoSPolicyBandwidthLimitRules) (added, deleted, updated kubeovnv1.QoSPolicyBandwidthLimitRules) {
	added = kubeovnv1.QoSPolicyBandwidthLimitRules{}
	deleted = kubeovnv1.QoSPolicyBandwidthLimitRules{}
	updated = kubeovnv1.QoSPolicyBandwidthLimitRules{}

	// Create a map of old rules indexed by name for efficient lookup
	// Store values (not pointers) to ensure correct reflect.DeepEqual comparison
	oldMap := make(map[string]kubeovnv1.QoSPolicyBandwidthLimitRule)
	for _, s := range oldList {
		oldMap[s.Name] = s
	}

	// Loop through new rules and compare with old rules
	for _, s := range newList {
		if old, ok := oldMap[s.Name]; !ok {
			// add the rule
			added = append(added, s)
		} else if !reflect.DeepEqual(old, s) {
			// updated the rule
			updated = append(updated, s)
		}
		// keep the rule not changed
		delete(oldMap, s.Name)
	}

	// Remaining rules in oldMap are deleted
	for _, s := range oldMap {
		deleted = append(deleted, s)
	}

	return added, deleted, updated
}

func (c *Controller) reconcileEIPBandwidthLimitRules(
	eip *kubeovnv1.IptablesEIP,
	added kubeovnv1.QoSPolicyBandwidthLimitRules,
	deleted kubeovnv1.QoSPolicyBandwidthLimitRules,
	updated kubeovnv1.QoSPolicyBandwidthLimitRules,
) error {
	var err error
	// in this case, we must delete rules first, then add or update rules
	if len(deleted) > 0 {
		if err = c.delEIPBandwidthLimitRules(eip, eip.Status.IP, deleted); err != nil {
			klog.Errorf("failed to delete eip %s bandwidth limit rules, %v", eip.Name, err)
			return err
		}
	}
	if len(added) > 0 {
		if err = c.addOrUpdateEIPBandwidthLimitRules(eip, eip.Status.IP, added); err != nil {
			klog.Errorf("failed to add eip %s bandwidth limit rules, %v", eip.Name, err)
			return err
		}
	}
	if len(updated) > 0 {
		if err = c.addOrUpdateEIPBandwidthLimitRules(eip, eip.Status.IP, updated); err != nil {
			klog.Errorf("failed to update eip %s bandwidth limit rules, %v", eip.Name, err)
			return err
		}
	}

	return nil
}

func validateIPMatchValue(matchValue string) bool {
	parts := strings.Split(matchValue, " ")
	if len(parts) != 2 {
		klog.Errorf("invalid ip MatchValue %s", matchValue)
		return false
	}

	direction := parts[0]
	if direction != "src" && direction != "dst" {
		klog.Errorf("invalid direction %s, must be src or dst", direction)
		return false
	}

	cidr := parts[1]
	if _, _, err := net.ParseCIDR(cidr); err != nil {
		klog.Errorf("invalid cidr %s", cidr)
		return false
	}
	return true
}

// numericRatePattern validates that rate/burst values are numeric (integer or decimal)
// Supports decimal values like "0.5" for sub-Mbps rates (0.5 Mbps = 500 Kbps)
// This prevents command injection when values are passed to shell scripts
// Defense in depth: CRD schema validation may be bypassed by direct API access
var numericRatePattern = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?$`)

// interfaceNamePattern validates network interface names
// Linux interface names: alphanumeric, underscore, hyphen, max 15 chars (IFNAMSIZ-1)
// Examples: eth0, net1, veth-abc, bond_0
// This prevents command injection when interface names are passed to shell scripts
var interfaceNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,15}$`)

func validateRateValue(value, fieldName string) error {
	if value == "" {
		return nil // empty is allowed (omitempty in CRD)
	}
	if !numericRatePattern.MatchString(value) {
		return fmt.Errorf("invalid %s value %q: must be a positive number (e.g., 100 or 0.5)", fieldName, value)
	}
	return nil
}

// validateInterfaceName validates network interface name to prevent command injection
// Linux interface names must be 1-15 characters, alphanumeric with underscore/hyphen
func validateInterfaceName(iface string) error {
	if iface == "" {
		return nil // empty is allowed (omitempty in CRD)
	}
	if !interfaceNamePattern.MatchString(iface) {
		return fmt.Errorf("invalid interface name %q: must be 1-15 alphanumeric characters, underscores, or hyphens", iface)
	}
	return nil
}

// validateDirection validates QoS rule direction to prevent command injection
// Only "ingress" and "egress" are valid values
func validateDirection(direction kubeovnv1.QoSPolicyRuleDirection) error {
	if direction == "" {
		return nil // empty is allowed (omitempty in CRD)
	}
	if direction != kubeovnv1.QoSDirectionIngress && direction != kubeovnv1.QoSDirectionEgress {
		return fmt.Errorf("invalid direction %q: must be 'ingress' or 'egress'", direction)
	}
	return nil
}

func (c *Controller) validateQosPolicy(qosPolicy *kubeovnv1.QoSPolicy) error {
	var err error
	if qosPolicy.Spec.BandwidthLimitRules != nil {
		for _, rule := range qosPolicy.Spec.BandwidthLimitRules {
			// Validate RateMax and BurstMax are numeric only (prevents command injection)
			if err = validateRateValue(rule.RateMax, "rateMax"); err != nil {
				klog.Error(err)
				return err
			}
			if err = validateRateValue(rule.BurstMax, "burstMax"); err != nil {
				klog.Error(err)
				return err
			}
			// Validate Interface name (prevents command injection)
			if err = validateInterfaceName(rule.Interface); err != nil {
				klog.Error(err)
				return err
			}
			// Validate Direction (prevents command injection)
			if err = validateDirection(rule.Direction); err != nil {
				klog.Error(err)
				return err
			}
			if rule.MatchType == "ip" {
				if !validateIPMatchValue(rule.MatchValue) {
					err = fmt.Errorf("invalid ip MatchValue %s", rule.MatchValue)
					klog.Error(err)
					return err
				}
			}
		}
	}
	if !qosPolicy.Spec.Shared && qosPolicy.Spec.BindingType == kubeovnv1.QoSBindingTypeNatGw {
		err = fmt.Errorf("qos policy %s is not shared, but binding to nat gateway", qosPolicy.Name)
		klog.Error(err)
		return err
	}
	return nil
}

func (c *Controller) handleUpdateQoSPolicy(key string) error {
	cachedQos, err := c.qosPoliciesLister.Get(key)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		klog.Error(err)
		return err
	}

	c.vpcNatGwKeyMutex.LockKey(key)
	defer func() { _ = c.vpcNatGwKeyMutex.UnlockKey(key) }()
	klog.Infof("handle update QoS policy %s", key)

	// should delete
	if !cachedQos.DeletionTimestamp.IsZero() {
		// Check if the QoS policy is still being used before allowing deletion.
		// Read through the API server: a referrer that claimed this policy moments ago may not be
		// in the informer cache yet, and releasing on that stale view drops a live reference.
		opts := metav1.ListOptions{
			LabelSelector: labels.SelectorFromSet(labels.Set{util.QoSPolicyUIDLabel: string(cachedQos.UID)}).String(),
		}
		var inUse bool
		if cachedQos.Spec.BindingType == kubeovnv1.QoSBindingTypeEIP {
			eips, err := c.config.KubeOvnClient.KubeovnV1().IptablesEIPs().List(context.Background(), opts)
			// when eip is not found, we should delete finalizer
			if err != nil && !k8serrors.IsNotFound(err) {
				klog.Errorf("failed to get eip list, %v", err)
				return err
			}
			inUse = eips != nil && len(eips.Items) != 0
		}

		if cachedQos.Spec.BindingType == kubeovnv1.QoSBindingTypeNatGw {
			gws, err := c.config.KubeOvnClient.KubeovnV1().VpcNatGateways().List(context.Background(), opts)
			// when nat gw is not found, we should delete finalizer
			if err != nil && !k8serrors.IsNotFound(err) {
				klog.Errorf("failed to get gw list, %v", err)
				return err
			}
			inUse = gws != nil && len(gws.Items) != 0
		}

		if inUse {
			// QoS policy is being deleted but still in use.
			// Return nil instead of error to avoid infinite retry loop.
			// The EIP/NatGw controller will remove the reference, which triggers
			// another reconciliation that will eventually delete the finalizer.
			klog.V(3).Infof("qos policy %s is marked for deletion but still in use, waiting for references to be removed", key)
			return nil
		}

		if err = c.handleDelQoSPoliciesFinalizer(key); err != nil {
			klog.Errorf("failed to handle del finalizer for qos %s, %v", key, err)
			return err
		}
		return nil
	}
	if err = c.handleAddQoSPolicyFinalizer(key); err != nil {
		klog.Errorf("failed to handle add finalizer for qos, %v", err)
		return err
	}

	if cachedQos.Status.Shared != cachedQos.Spec.Shared ||
		cachedQos.Status.BindingType != cachedQos.Spec.BindingType {
		err := fmt.Errorf("not support qos %s change shared", key)
		klog.Error(err)
		return err
	}

	if err := c.validateQosPolicy(cachedQos); err != nil {
		klog.Errorf("failed to validate qos %s, %v", key, err)
		return err
	}

	added, deleted, updated := diffQoSPolicyBandwidthLimitRules(cachedQos.Status.BandwidthLimitRules, cachedQos.Spec.BandwidthLimitRules)
	bandwidthRulesChanged := len(added) > 0 || len(deleted) > 0 || len(updated) > 0

	if bandwidthRulesChanged {
		klog.V(3).Infof(
			"bandwidth limit rules is changed for qos %s, added: %s, deleted: %s, updated: %s",
			key, added.Strings(), deleted.Strings(), updated.Strings(),
		)
		if cachedQos.Status.Shared {
			err := fmt.Errorf("not support shared qos %s change rule", key)
			klog.Error(err)
			return err
		}

		if cachedQos.Status.BindingType == kubeovnv1.QoSBindingTypeEIP {
			// filter to eip
			eips, err := c.iptablesEipsLister.List(
				labels.SelectorFromSet(labels.Set{util.QoSPolicyUIDLabel: string(cachedQos.UID)}),
			)
			if err != nil {
				klog.Errorf("failed to get eip list, %v", err)
				return err
			}
			switch {
			case len(eips) == 0:
				// not thing to do
			case len(eips) == 1:
				eip := eips[0]
				if err = c.reconcileEIPBandwidthLimitRules(eip, added, deleted, updated); err != nil {
					klog.Errorf("failed to reconcile eip %s bandwidth limit rules, %v", eip.Name, err)
					return err
				}
			default:
				err := fmt.Errorf("not support qos %s change rule, related eip more than one", key)
				klog.Error(err)
				return err
			}
		}

		sortedNewRules := cachedQos.Spec.BandwidthLimitRules
		sort.Slice(sortedNewRules, func(i, j int) bool {
			return sortedNewRules[i].Name < sortedNewRules[j].Name
		})

		// .Status.Shared and .Status.BindingType are not supported to change
		if err = c.patchQoSStatus(key, cachedQos.Status.Shared, cachedQos.Status.BindingType, sortedNewRules); err != nil {
			klog.Errorf("failed to patch status for qos %s, %v", key, err)
			return err
		}
	}
	return nil
}

func (c *Controller) handleDelQoSPolicy(key string) error {
	klog.V(3).Infof("deleted qos policy %s", key)
	return nil
}

func (c *Controller) handleAddQoSPolicyFinalizer(key string) error {
	cachedQoSPolicy, err := c.qosPoliciesLister.Get(key)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		klog.Error(err)
		return err
	}
	if !cachedQoSPolicy.DeletionTimestamp.IsZero() || controllerutil.ContainsFinalizer(cachedQoSPolicy, util.KubeOVNControllerFinalizer) {
		return nil
	}
	newQoSPolicy := cachedQoSPolicy.DeepCopy()
	controllerutil.AddFinalizer(newQoSPolicy, util.KubeOVNControllerFinalizer)
	patch, err := util.GenerateMergePatchPayload(cachedQoSPolicy, newQoSPolicy)
	if err != nil {
		klog.Errorf("failed to generate patch payload for qos '%s', %v", cachedQoSPolicy.Name, err)
		return err
	}
	if _, err := c.config.KubeOvnClient.KubeovnV1().QoSPolicies().Patch(context.Background(), cachedQoSPolicy.Name,
		types.MergePatchType, patch, metav1.PatchOptions{}, ""); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		klog.Errorf("failed to add finalizer for qos '%s', %v", cachedQoSPolicy.Name, err)
		return err
	}
	return nil
}
