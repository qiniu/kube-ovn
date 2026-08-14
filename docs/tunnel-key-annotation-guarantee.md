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
subnet's tunnel key is not valid (`Status.TunnelKey` is outside
`1..16777215`), and writes the tunnel_key annotation in the same
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
- Pods whose persisted annotation is valid and matches subnet status keep it.
- Legacy pods allocated before the subnet tunnel key was synced (or before
  this code existed) can have a missing, non-numeric or out-of-range
  annotation (valid OVN tunnel keys are `1..16777215`). On startup,
  `InitIPAM` calls `repairPodTunnelKeyOnStartup`, which invokes
  `handleRepairTunnelKey` synchronously using `subnet.Status.TunnelKey`.
  If it cannot finish, the dedicated queue retries indefinitely with
  exponential backoff capped at one minute and an aggregate 10-qps token
  bucket (subnet reconcile may still be syncing the key).

InitIPAM first attempts every eligible repair synchronously, using persisted
pod annotations (`podProvidersNeedingTunnelKeyRepair`) before NAD/default-
subnet resolution. If subnet status is already valid, the pod is fixed before
InitIPAM continues. Only an unavailable key or transient API error is queued.
That asynchronous fallback is necessary because InitIPAM runs before
`startWorkers`; a missing key may require the subnet worker, so waiting for it
inside InitIPAM would block the worker that produces it. Deferred repairs run
with rate limiting after workers start. There is no periodic task or pod-update
fallback. Detection and successful repair are logged at Warning level,
including the instruction to restart Cilium after backfill so already-created
endpoints reload the corrected VNI.

The deferred asynchronous repair is fallback recovery only; it is not part of
normal pod allocation. Normal allocation writes IP/network, tunnel_key and
`allocated=true` in one pod annotation patch.

Repair is multi-NIC aware and driven by the per-provider annotations the
allocation wrote (`allocated` + `logical_switch`). A provider is repaired
only when its logical switch resolves to a non-vlan OVN VPC subnet and its
key is missing, invalid or different from `subnet.Status.TunnelKey`.
Providers managed by another CNI have no `logical_switch`; OVN
vlan/underlay subnets have one but are filtered by `isOvnVpcSubnet`. Both keep no pod tunnel_key annotation;
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
restart cilium so it re-reads them for already-created endpoints. Every CNI
ADD waits for `allocated=true`; when the persisted logical_switch resolves to
a non-vlan OVN VPC subnet, it additionally waits for a valid annotation equal
to `Subnet.Status.TunnelKey`. If backfill has not completed within the wait
loop, ADD fails and kubelet retries. The Cilium restart is still required for
endpoints created before the backfill logic existed.

## Compatibility and rolling upgrade

The Subnet API is unchanged: `status.tunnelKey` remains an optional integer,
and pod annotation keys keep their existing provider-based format. Normal
non-vlan OVN VPC allocation remains compatible with controllers that already
wrote the annotation atomically.

Two behavior changes are intentional:

- VLAN/underlay and non-OVN subnets now converge to
  `status.tunnelKey=0` and no pod tunnel_key annotation. Older controller
  versions may have populated those values even though native-vpc identity
  does not consume them; startup reconciliation removes those stale values.
- The new daemon fails CNI ADD for a provider whose logical_switch resolves
  to an OVN VPC subnet until `allocated=true` and the matching tunnel_key are
  visible. This converts a previously possible legacy race into a retryable
  CNI failure. A provider without logical_switch is not classified as VPC and
  keeps the old no-key behavior (required by NoDefaultEIP/IPAM-only networks).

For rolling upgrades, restart/upgrade kube-ovn-controller first, wait for the
startup repair warnings to complete, then restart/upgrade kube-ovn-daemon and
Cilium. An old daemon does not enforce the new CNI key gate; a new daemon is
compatible with a controller that already performs the atomic VPC annotation
patch. Rollback is safe: an older controller may repopulate VLAN tunnel-key
status, and a subsequent new-controller startup cleans it again.

Non-primary-CNI remains compatible: non-vlan OVN attachment providers use the
same per-provider annotation contract. IPAM-only and NoDefaultEIP attachments
without a persisted logical_switch bypass the VPC key gate, including legacy
provider naming.

## Known gaps (documented, not closed by code)

- `Subnet.Status.TunnelKey` is not periodically compared with OVN SB. This is
  intentional because a `Datapath_Binding` key is stable during the normal
  lifecycle. After a destructive SB rebuild or an inconsistent DR restore,
  northd may assign a different key while the persisted status still contains
  an otherwise valid old value; `reconcileSubnetTunnelKey` will not re-read SB
  until that status is invalidated. Recover every affected subnet as follows:

  1. Clear its persisted key:

     ```bash
     kubectl patch subnet <name> --subresource=status --type=merge \
       -p '{"status":{"tunnelKey":0}}'
     ```

  2. Restart kube-ovn-controller. InitIPAM detects affected pods; after subnet
     workers re-read the new SB key, the deferred repair queue updates their
     annotations. Wait for the startup repair Warning logs to complete.
  3. Restart Cilium so existing endpoints reload the new VNI.

  A controller restart without first invalidating status is insufficient.
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
  queue forgets it, and there is no periodic or pod-update resync. It is
  reconsidered on the next controller restart - acceptable because the pod
  usually cannot survive its subnet's deletion anyway, and accidental manual
  deletion is outside this guarantee.
- The upgrade sequence above is operator discipline, not enforced by code:
  restarting cilium before the backfill completes leaves already-created
  endpoints on the non-VPC scheme until the next cilium restart. This affects
  only endpoints that already exist; CNI ADD itself is fail-closed on the key.
