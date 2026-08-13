# tunnel_key (VNI) annotation guarantee

Every non-hostNetwork pod on an OVN subnet carries a non-zero
`ovn.kubernetes.io/tunnel_key` (VNI) annotation by the time its network is
configured (CNI ADD). Cilium (native-vpc mode) keys pod identities by this
annotation, so a missing annotation makes the pod fall back to the non-VPC
scheme and collide with overlapping VPC subnets.

The guarantee is upheld by two flows:

## Flow 1: normal pod creation

`reconcileAllocateSubnets` (pkg/controller/pod.go) refuses to allocate
(requeues, persisting nothing) while the resolved subnet's tunnel key has
not been synced from OVN SB (`Status.TunnelKey == 0`), and writes the
tunnel_key annotation in the same atomic patch as `allocated=true`. The
kube-ovn CNI server blocks until `allocated=true`, and Cilium (chained
after kube-ovn, the primary CNI) only runs once kube-ovn's CNI ADD has
succeeded, so by the time Cilium reads the pod, the annotation is present
and non-zero.

Why flow 1 cannot produce a missing annotation: the pod NIC (LSP) is
created at a single point, `reconcileAllocateSubnets`, and that path
returns an error (persisting nothing) while the tunnel key is not ready;
`tunnel_key` and `allocated=true` are written in the same atomic patch;
and the CNI server blocks on `allocated=true` before any NIC exists. So
under the current code a pod either carries the annotation or is still
waiting at CNI ADD - a missing annotation therefore implies a legacy pod
created before this guarantee existed, which is exactly the population
flow 2 repairs.

## Flow 2: kube-ovn-controller restart / legacy pods

- Pods created after the restart follow flow 1 (the gate lives in the
  allocation path and is unrelated to restarts).
- Pods that already carry the annotation keep it (persisted in etcd).
- Legacy pods allocated before the subnet tunnel key was synced (or before
  this code existed) are missing the annotation. On startup `InitIPAM`
  enqueues them into the dedicated `repairTunnelKeyQueue`
  (`enqueuePodTunnelKeyRepair`); the repair worker
  (`handleRepairTunnelKey`) patches them from `subnet.Status.TunnelKey`,
  retrying with unbounded backoff until the key becomes available (subnet
  reconcile may still be syncing it).

Enqueue is not one-shot: the pod reconcile path (`handleAddOrUpdatePod`)
and a post-start resync (`resyncPodTunnelKey`, bounded to a backfill
window of `tunnelKeyResyncWindow`) also enqueue repairs, so a pod the
startup sweep missed (e.g. its NAD or default subnet could not be resolved
at that moment) heals on the next pod update event, or on a resync tick
within the window, instead of requiring another controller restart.

Repair is multi-NIC aware and driven purely by the per-provider annotations
the allocation wrote (`allocated` + `logical_switch`): every OVN subnet has
its own `tunnel_key` annotation. A provider is repaired only if it is
marked allocated and its `logical_switch` annotation resolves to an OVN
subnet; the subnet is never guessed from namespace/default fallbacks,
because writing a wrong VNI is worse than a missing one (nothing would
correct it afterwards). Progress is not all-or-nothing: ready providers are
patched in one call even when another provider's subnet key is still 0 (the
handler then returns an error to requeue for the remainder).

## Intended upgrade sequence

Restart kube-ovn-controller first (flow 2 backfills the annotations), then
restart cilium so it re-reads them for already-created endpoints. Note the
CNI server only waits for `allocated=true`, so a CNI ADD for a legacy pod
racing ahead of the repair proceeds without the annotation; if the ADD
succeeds there is no kubelet retry, and the stale Cilium endpoint is
corrected by the ordered cilium restart.

## Known gaps (documented, not closed by code)

- Repair progress is observable via `pod_tunnel_key_missing` (gap size at
  the last resync during the post-start backfill window),
  `pod_tunnel_key_repair_total` and `pod_tunnel_key_repair_skipped_total`
  (see pkg/controller/tunnel_key_metrics.go).
- Repair keying is per `podNet.ProviderName` (the `*.kubernetes.io/*`
  annotation templates). Cilium currently only recognizes the primary NIC
  (default provider, `ovn.kubernetes.io/*` annotations), so a mismatch on
  a multus secondary NIC would be harmless today; it only matters if
  Cilium starts keying secondary NICs.
- The upgrade sequence above is operator discipline, not enforced by code:
  restarting cilium before the backfill completes leaves already-created
  endpoints on the non-VPC scheme until the next cilium restart.
- A CNI ADD racing the repair for a legacy pod is not closed by the CNI
  server (it only waits for `allocated=true`) nor by a kubelet retry when
  the ADD succeeds; the stale Cilium endpoint is corrected by the ordered
  cilium restart in the upgrade sequence above.
