# tunnel_key (VNI) annotation guarantee

Every non-hostNetwork pod on a non-vlan OVN VPC subnet carries a valid
`ovn.kubernetes.io/tunnel_key` (VNI) annotation by the time its network is
configured (CNI ADD). This includes both the default cluster-router VPC and
custom VPCs. The exact scope predicate is
`isOvnSubnet(subnet) && subnet.Spec.Vlan == ""`. VLAN/underlay and non-OVN
subnets keep the default
`status.tunnelKey=0` and do not carry the pod annotation. Cilium
(native-vpc mode) keys pod identities by this annotation, so a missing
annotation makes the pod fall back to the non-VPC scheme and collide with
overlapping VPC subnets.

The guarantee is upheld by two flows:

## Flow 1: normal pod creation

`reconcileAllocateSubnets` (pkg/controller/pod.go) refuses to complete the
allocation (requeues, persisting no pod annotations) while the resolved
subnet's tunnel key has not been synced from OVN SB
(`Status.TunnelKey == 0`), and writes the tunnel_key annotation in the same
atomic patch as `allocated=true`. For a non-vlan OVN VPC subnet, the
kube-ovn CNI server blocks until `allocated=true` and the pod annotation is
a valid value equal to `Subnet.Status.TunnelKey`. Cilium (chained after
kube-ovn, the primary CNI) only runs once kube-ovn's CNI ADD has succeeded,
so by the time Cilium reads the pod, the annotation is correct.

For a multi-NIC pod, an earlier loop iteration may already have created
idempotent LSP/IP CR state before a later NIC hits the gate. This does not
weaken the CNI-visible guarantee: the pod annotation patch is committed only
after every requested OVN VPC NIC passes the gate, and retries safely reconcile
the earlier objects again.

Why flow 1 cannot produce a missing annotation: `reconcileAllocateSubnets`
returns an error without persisting pod annotations while any requested
OVN VPC subnet's tunnel key is not ready; `tunnel_key` and `allocated=true`
are written in the same atomic patch; and the CNI server verifies both before
returning success. So under the current code a pod either carries the correct
annotation or is still waiting at CNI ADD - a missing annotation therefore
implies a legacy pod created before this guarantee existed, which is exactly
the population flow 2 repairs.

## Flow 2: kube-ovn-controller restart / legacy pods

- Pods created after the restart follow flow 1 (the gate lives in the
  allocation path and is unrelated to restarts).
- Pods that already carry the annotation keep it (persisted in etcd).
- Legacy pods allocated before the subnet tunnel key was synced (or before
  this code existed) can have a missing, non-numeric or out-of-range
  annotation (valid OVN tunnel keys are `1..16777215`). On startup
  `InitIPAM` enqueues them into the dedicated
  `repairTunnelKeyQueue` (`enqueuePodTunnelKeyRepair`); the repair worker
  (`handleRepairTunnelKey`) patches them from `subnet.Status.TunnelKey`,
  retrying indefinitely with exponential backoff capped at one minute and
  an aggregate 10-qps token bucket until the key becomes available (subnet
  reconcile may still be syncing it).

Enqueue is not one-shot: reconciliation is annotation-driven
(`podProvidersNeedingTunnelKeyRepair`), so the startup sweep cannot skip a pod
because its NAD or default subnet could not be resolved at that moment; the
pod reconcile path (`handleAddOrUpdatePod`) re-enqueues on any later update
event.

Repair is multi-NIC aware and driven by the per-provider annotations the
allocation wrote (`allocated` + `logical_switch`). A provider is repaired
only when its logical switch resolves to a non-vlan OVN VPC subnet and its
key is missing, invalid or different from `subnet.Status.TunnelKey`.
Providers managed by another CNI have no `logical_switch`; OVN vlan/underlay subnets have one but
are filtered by `isOvnVpcSubnet`. Both keep no pod tunnel_key annotation;
startup repair removes stale keys written by older controller versions.
The subnet is never guessed from namespace/default fallbacks, because
writing a wrong VNI is worse than a missing one (nothing would correct it
afterwards); a `logical_switch` that does not resolve is counted by
`pod_tunnel_key_repair_skipped_total` as the anomaly it is. Progress is
not all-or-nothing: ready providers are patched in one call even when
another provider's subnet key is still 0 (the handler then returns an
error to requeue for the remainder).

## Failure mode and diagnosis

The allocation gate is intentionally fail-closed. If a non-vlan OVN VPC
subnet's tunnel key cannot be observed (for example, OVN SB is unreachable
or northd has not created the logical switch's `Datapath_Binding`), new
pods on that subnet are not allocated with a guessed or zero VNI:

- the pod remains in `ContainerCreating`;
- kube-ovn CNI waits for `allocated=true`, fails after its wait loop, and the
  kubelet retries CNI ADD;
- the pod receives an `AcquireAddressFailed` event containing
  `subnet <name> tunnel key not observed on allocation`.

Check the subnet status with:

```bash
kubectl get subnet <name> -o jsonpath='{.status.tunnelKey}'
```

A value outside `1..16777215` is invalid and triggers fail-closed
re-synchronization. Check OVN SB reachability and whether the logical switch
has a `Datapath_Binding` in SB.
VLAN/underlay subnets are not subject to this gate and keep the default
TunnelKey value.

## Intended upgrade sequence

Restart kube-ovn-controller first (flow 2 backfills the annotations), then
restart cilium so it re-reads them for already-created endpoints. For every
new CNI ADD on a non-vlan OVN VPC subnet, the CNI server waits for both
`allocated=true` and a valid annotation equal to `Subnet.Status.TunnelKey`;
if backfill has not completed within the wait loop, ADD fails and kubelet
retries. The cilium restart is still required for endpoints created before
the backfill logic existed.

## Known gaps (documented, not closed by code)

- Repair progress is observable via `pod_tunnel_key_repair_patch_total`
  (annotation patches that added, corrected or removed one or more tunnel_key
  annotations; a multi-NIC pod can be patched more than once) and
  `pod_tunnel_key_repair_skipped_total` (repairs skipped because the
  logical_switch subnet no longer exists) - see
  pkg/controller/tunnel_key_metrics.go.
- Repair keying is per provider parsed from the `*.kubernetes.io/allocated`
  annotation suffix (the same `*.kubernetes.io/*` templates as the
  allocation path). Cilium currently only recognizes the primary NIC
  (default provider, `ovn.kubernetes.io/*` annotations), so a mismatch on
  a multus secondary NIC would be harmless today; it only matters if
  Cilium starts keying secondary NICs.
- A pod whose `logical_switch` subnet no longer exists is skipped (counted
  by `pod_tunnel_key_repair_skipped_total`) and not retried: the repair
  queue forgets it, and there is no periodic resync. It is re-enqueued on
  the next pod update event or the next controller restart - acceptable
  because the pod usually cannot survive its subnet's deletion anyway.
- The upgrade sequence above is operator discipline, not enforced by code:
  restarting cilium before the backfill completes leaves already-created
  endpoints on the non-VPC scheme until the next cilium restart. This affects
  only endpoints that already exist; CNI ADD itself is fail-closed on the key.
