# tunnel_key (VNI) annotation guarantee

Every non-hostNetwork pod on an OVN subnet carries a non-zero
`ovn.kubernetes.io/tunnel_key` (VNI) annotation by the time its network is
configured (CNI ADD). Cilium (native-vpc mode) keys pod identities by this
annotation, so a missing annotation makes the pod fall back to the non-VPC
scheme and collide with overlapping VPC subnets.

The guarantee is upheld by two flows:

## Flow 1: normal pod creation

`reconcileAllocateSubnets` (pkg/controller/pod.go) refuses to complete the
allocation (requeues, persisting no pod annotations) while the resolved
subnet's tunnel key has not been synced from OVN SB
(`Status.TunnelKey == 0`), and writes the tunnel_key annotation in the same
atomic patch as `allocated=true`. The kube-ovn CNI server blocks until
`allocated=true`, and Cilium (chained after kube-ovn, the primary CNI) only
runs once kube-ovn's CNI ADD has succeeded, so by the time Cilium reads the
pod, the annotation is present and non-zero.

For a multi-NIC pod, an earlier loop iteration may already have created
idempotent LSP/IP CR state before a later NIC hits the gate. This does not
weaken the CNI-visible guarantee: the pod annotation patch is committed only
after every requested OVN NIC passes the gate, and retries safely reconcile
the earlier objects again.

Why flow 1 cannot produce a missing annotation: `reconcileAllocateSubnets`
returns an error without persisting pod annotations while any requested
OVN subnet's tunnel key is not ready; `tunnel_key` and `allocated=true` are
written in the same atomic patch; and the CNI server blocks on
`allocated=true`. So under the current code a pod either carries the
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

Enqueue is not one-shot: the enqueue is annotation-driven
(`podProvidersMissingTunnelKey`), so the startup sweep cannot skip a pod
because its NAD or default subnet could not be resolved at that moment; the
pod reconcile path (`handleAddOrUpdatePod`) re-enqueues on any later update
event.

Repair is multi-NIC aware and driven purely by the per-provider annotations
the allocation wrote (`allocated` + `logical_switch`): every OVN subnet has
its own valid `tunnel_key` annotation. A provider is repaired if its key is
missing, non-numeric or outside `1..16777215`, and only if it is marked
allocated and its `logical_switch` annotation resolves to an OVN subnet. An allocated provider with an empty `logical_switch` is a NIC whose
subnet provider is not ovn (kube-ovn acts as IPAM only, another CNI
configures the NIC) and is skipped silently - the OVN allocation path
always writes `logical_switch`, so the empty case reliably means no
tunnel_key is ever written. Note that vlan/underlay subnets keep provider
`ovn`: they are part of this mechanism (the allocation gate also applies,
so underlay pods wait for the tunnel key before IP allocation).
The subnet is never guessed from namespace/default fallbacks, because
writing a wrong VNI is worse than a missing one (nothing would correct it
afterwards); a `logical_switch` that does not resolve is counted by
`pod_tunnel_key_repair_skipped_total` as the anomaly it is. Progress is
not all-or-nothing: ready providers are patched in one call even when
another provider's subnet key is still 0 (the handler then returns an
error to requeue for the remainder).

## Failure mode and diagnosis

The allocation gate is intentionally fail-closed. If an OVN subnet's tunnel
key cannot be observed (for example, OVN SB is unreachable or northd has not
created the logical switch's `Datapath_Binding`), new pods on that subnet are
not allocated with a guessed or zero VNI:

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
Underlay/vlan subnets also use OVN logical switches and are subject to this
gate.

## Intended upgrade sequence

Restart kube-ovn-controller first (flow 2 backfills the annotations), then
restart cilium so it re-reads them for already-created endpoints. Note the
CNI server only waits for `allocated=true`, so a CNI ADD for a legacy pod
racing ahead of the repair proceeds without the annotation; if the ADD
succeeds there is no kubelet retry, and the stale Cilium endpoint is
corrected by the ordered cilium restart.

## Known gaps (documented, not closed by code)

- Repair progress is observable via `pod_tunnel_key_repair_patch_total`
  (annotation patches that wrote one or more tunnel_key annotations; a
  multi-NIC pod can be patched more than once) and
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
  endpoints on the non-VPC scheme until the next cilium restart.
- A CNI ADD racing the repair for a legacy pod is not closed by the CNI
  server (it only waits for `allocated=true`) nor by a kubelet retry when
  the ADD succeeds; the stale Cilium endpoint is corrected by the ordered
  cilium restart in the upgrade sequence above.
