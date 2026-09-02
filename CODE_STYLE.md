# Kube-OVN Code Style Guide

## Introduction

The goal of this guide is to manage the complexity, keep a consistent code style and prevent common mistakes.
New code should follow the guides below and reviewers should check if new PRs follow the rules.

## Style

### Naming

Always use camelcase to name variables and functions.

<table>
<thead><tr><th>Bad</th><th>Good</th></tr></thead>
<tbody>
<tr><td>

```go
var command_line string
```

</td><td>

```go
var commandLine string
```

</td></tr>
</tbody></table>

### Error Handle

All error that not expected should be handled with error log. No error should be skipped silently.

<table>
<thead><tr><th>Bad</th><th>Good</th></tr></thead>
<tbody>
<tr><td>

```go
kubeClient, _ := kubernetes.NewForConfig(cfg)
```

</td><td>

```go
kubeClient, err := kubernetes.NewForConfig(cfg)
if err != nil {
    klog.Errorf("init kubernetes client failed %v", err)
    return err
}
```

</td></tr>
</tbody></table>

We prefer use `if err := somefunction(); err != nil {}` to check error in one line.

<table>
<thead><tr><th>Bad</th><th>Good</th></tr></thead>
<tbody>
<tr><td>

```go
err := c.initNodeRoutes()
if err != nil {
  klog.Fatalf("failed to initialize node routes: %v", err)
}
```

</td><td>

```go
if err := c.initNodeRoutes(); err != nil {
    klog.Fatalf("failed to initialize node routes: %v", err)
}
```

</td></tr>
</tbody></table>

### Function

The length of one function should not exceed 100 lines.

When err occurs in the function, it should be returned to the caller not skipped silently.

<table>
<thead><tr><th>Bad</th><th>Good</th></tr></thead>
<tbody>
<tr><td>

```go
func startHandle() {
 if err = some(); err != nil {
  klog.Errorf(err)
    }
 return
}
```

</td><td>

```go
func startHandle() error {
    if err = some(); err != nil {
        klog.Errorf(err)
  return err
    }
    return nil
}
```

</td></tr>
</tbody></table>

## CRD

When adding a new CRD to Kube-OVN, you should consider things below to avoid common bugs.

1. The new feature should be disabled for performance and stability reasons.
2. The `install.sh`, `charts` and `yamls` should install the new CRD.
3. The `cleanup.sh` should clean the CRD and all the related resources.
4. The `gc.go` should check the inconsistent resource and do the cleanup.
5. The add/update/delete event can be triggered many times during the lifecycle, the handler should be reentrant.
6. Never bind a resource to a referenced CRD that is being deleted.
7. If a finalizer is released only when nobody references the CRD, reference writers must first check that the referenced CRD is not being deleted.

When CRD `A` only drops its finalizer once nothing references it any more, every place
that *establishes* such a reference must first check `A.DeletionTimestamp`. Otherwise a
reference created after `A` was marked for deletion keeps `A` terminating forever, and
`A` in turn blocks whatever the platform deletes next. The API server rejects a same-named
create while the old object is still terminating, so the danger is not a live tombstone but a
*new generation*: these resources are named after the address they carry (`IptablesEIP`,
`IptablesFIPRule`, `QoSPolicy`, ...), so once the previous instance is gone a recreated one
inherits every stale reference still recorded under that shared name or address.

These rules make the check correct:

- Put it on the **writer of the reference**, not on the handler. The reference is what the
  in-use check counts, e.g. `util.QoSPolicyUIDLabel` written by `patchEipLabel` and
  `updateCrdNatGwLabels`, or `util.EipUIDLabel` written by `patchFipLabel`, `patchDnatLabel`
  and `patchSnatLabel`. Guarding one
  caller leaves the delayed and replay paths (`resetIptablesEipQueue.AddAfter`, `redoFip`,
  gateway re-init) wide open. A guard placed earlier is only equivalent when every writer
  is provably downstream of it, as `getBindableEip` is for the three `patch*Label` helpers.
- Store the referenced object's **UID** on the referrer and make the in-use check select by
  UID, not by name, IP, or other reusable business keys. When adding a new UID reference
  label, add startup backfill before workers run so existing objects are not seen as unused.
- Reject only a reference that would be **added or changed**. Rewriting the value the object
  already carries adds nothing to the in-use count, and refusing it is actively harmful where
  the caller iterates: `handleUpdateVpcFloatingIP` returns on the first error, so one EIP
  pointing at a tombstone would stop `redoFip` for every other FIP of that gateway and leave
  their rules unapplied after a gateway pod restart.
- Guard **binding only**, never reading or cleanup. Deletion paths legitimately read a
  terminating object, `finalDeleteFipInPod` needs the dying EIP's `Status.IP` to remove the
  rules from the gateway pod. Rejecting there deadlocks the cleanup itself.
- **Exempt a terminating referrer.** It establishes no new binding, and blocking it can
  break its own deletion: `handleResetIptablesEip` must reach its status patch, which is
  what re-enqueues the terminating EIP.

Returning an error is enough, the work queues retry with backoff and never give up, so the
binding succeeds as soon as the tombstone is reclaimed.

Dependency recovery must be event-driven. A referrer cannot become Ready until its dependency
exists, is not terminating, and its required data plane is ready. The dependency's informer event
wakes pending referrers only after authoritative state reaches the cache; an API `UpdateStatus`
success does not make the new Status immediately visible. Zero-value Spec and Status may match
before the first reconcile, so use a persistent marker such as the controller finalizer when that
distinction matters.

An authoritative usable-to-unusable transition also wakes established referrers. They stop new
data-plane writes, retain cleanup identity, and report not Ready until recovery. A pending Spec
update is not invalidation while old Status still describes a working data plane: update that data
plane before advancing Status, while continuing to block new bindings.

Route incomplete Status to add, complete but mismatched Status to update, and replay a matching
not-ready identity idempotently on recovery. A same-name dependency is converged only when its UID
credential matches the current generation; rebind a mismatch only when the old data plane can be
cleaned, and ignore invalidation events from an older generation once the referrer carries the newer UID. Skip terminating and converged
referrers. On restart,
each referrer's initial Add enqueues its key while caches synchronize; workers consume queued keys
only after all required caches sync. Use dependency Add replay only where it provides a required
readiness or invalidation notification. Keep rate-limited retry as fallback. Tests must cover usable
and unusable transitions, add/update routing, cleanup identity, termination, restart, and the
API/cache gap.

Startup backfill fills only missing UID credentials; it must not overwrite a different UID and hide
a same-name generation mismatch. A reconciler may rebind only when it can clean the previously
applied data plane. For example, a VPC NAT Gateway cannot delete QoS rules from a force-deleted,
same-name policy using the new policy's rule list; without a persisted old-rule snapshot, leave the
gateway un-converged and require an explicit remove or rebind instead.

The check and the credential write are two calls against two different objects, so the pair is
not atomic: the referenced object can start terminating in between. What keeps that window from
orphaning rules in a gateway pod is that a rule is never programmed without a credential
covering it: a new claim is written before the rule it covers exists, and an old claim is held
until the rule it covers is gone. On a rebind that puts the swap between the two data-plane
calls, not before both, since one label carries both claims. Where cleanup needs the referenced
object to know what to undo, as `delEipQoS` does, the swap can only come after both. The release
path reads the referrers back from the API server rather than the informer cache for the same
reason. None of this makes the check and the write mutually exclusive. Closing the window
completely means recording the reference on the referenced object itself under a resourceVersion
precondition, which is a much larger change and is deliberately left for later.
