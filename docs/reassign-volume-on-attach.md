# Reassign volume ownership on attach

`features.reassignVolumeOnAttach` (default: `false`) makes a CSI volume's Proxmox
disk owned by, and named for, the VM it is attached to. While a pod is running, the
volume is `local:3021/vm-3021-pvc-<uuid>.raw` rather than
`local:9999/vm-9999-pvc-<uuid>.raw`; when the pod goes away, the volume goes back to
the controller's placeholder VM ID (`controllerVMID`, default `9999`).

It requires the [`proxmox-csi-storage` proxmod extension](../hack/proxmod-csi-storage/)
(**>= 0.3.1**) installed on every PVE node, and `proxmod_endpoint: true` on the cluster.
Without it the rename fails, the attach proceeds under the volume's existing name, and
the feature is a no-op — the controller logs a warning at startup if the flag is set
while no cluster enables the endpoint.

0.3.0 is not sufficient: its rename endpoint runs on whichever host the driver's
cluster URL points at rather than the node named in the request, so the feature works
only for volumes that happen to live on that one host. See
[Troubleshooting](#troubleshooting).

## Why this exists

CSI-provisioned volumes are created and owned by the controller's placeholder VM
(`controllerVMID`), then attached to whatever real VM a pod lands on. Without this
feature the volume's Proxmox `vmid` never changes to match, so Proxmox-side tooling —
backup jobs, accounting, anything keying off `vmid` — sees every CSI volume as
belonging to a VM that does not exist.

## How it works

**Rename symmetrically, and match on the part of the name a rename does not change.**

| Step | What happens |
|---|---|
| `ControllerPublishVolume` | rename `9999 → <target>` **first**, then attach the new name |
| while attached | the volume is named for the target VM and *owned* by it |
| `ControllerUnpublishVolume` | detach **first**, then rename `<target> → 9999`, then clear the `unused<n>` key |
| at rest | the volume is back on the name the PV's `volumeHandle` carries |

The rename is `PVE::Storage::rename_volume`, reached through
`POST /nodes/{node}/proxmod/csi-storage/rename`. It is the only mechanism PVE has for
reassigning an **unattached** volume, and it has no REST endpoint of its own — hence
the extension. Proxmox's own `move_disk` cannot substitute: it moves a disk key
between two *real* VMs, and the placeholder vmid a CSI volume carries at rest is not a
VM. See [Live test result](#live-test-result-2026-08-12) for the failure that
established this.

### `volumeHandle` stays valid

`spec.csi.volumeHandle` is immutable, and every later CSI call parses the vmid and
disk name straight out of it. This design never rewrites it. Instead:

- **At rest the handle is exactly right**, because unpublish renames back. `DeleteVolume`,
  `CreateSnapshot` and the [migration controller](migration-controller.md) are untouched.
- **While attached the handle is stale**, so the attached-state paths match on the
  stable `pvc-<uuid>.raw` suffix instead of the full name. `isVolumeAttached` already
  matched by substring, so this is a change of argument, not of algorithm.
- **A volume found on neither name is searched for by suffix** (`resolveVolume`), and
  whichever vmid it currently carries is adopted. A controller that dies between
  renaming and attaching, or between detaching and renaming back, leaves the volume on
  one of the two names; this is what picks it back up on retry.

### A failed rename never fails the attach

If proxmod is unavailable, the storage does not support rename, or the endpoint
refuses, the error is logged and the volume attaches under its existing name. The
lesson from the 2026-08-12 live test is that this feature must not be able to take
attach down cluster-wide.

## Two consequences worth knowing before you enable it

**1. While attached, the VM genuinely owns the volume.** `qm destroy 3021` will
destroy the CSI volumes attached to VM 3021 along with it, because as far as PVE is
concerned they are that VM's disks. Without this feature they are named for 9999 and
`qm destroy` leaves them behind. If your node lifecycle tooling destroys VMs, drain
the node first — which you should be doing anyway, but the failure mode is worse here.

**2. Detach leaves an `unused<n>` key, and clearing it is ordering-sensitive.**
Because the VM owns the volume, `UnlinkDisk(force=false)` moves it to `unused0` rather
than simply dropping the line. Deleting an `unused<n>` key is how PVE *deallocates* a
volume (`try_deallocate_drive` → `vdisk_free`):

- delete the key while the volume still exists under that name → **the volume is
  destroyed**
- delete the key after the volume has been renamed away → the key names a path that no
  longer exists, and the delete removes the config line and nothing else

The driver therefore renames first and clears second, and refuses to clear the key at
all if the referenced volume is still on storage. If you clean these up by hand, apply
the same rule.

## Enabling it

```yaml
features:
  reassignVolumeOnAttach: true

clusters:
  - url: https://cluster-api-1.example.com:8006/api2/json
    token_id: "kubernetes-csi@pve!csi"
    token_secret: "secret"
    region: Region-1
    proxmod_endpoint: true
```

Install the extension (>= 0.3.1) on **every** PVE node first — a node without it
silently falls back to no-op renames for volumes on that node. See
[`hack/proxmod-csi-storage/README.md`](../hack/proxmod-csi-storage/README.md) for
packaging and [`SMOKE-TEST.md`](../hack/proxmod-csi-storage/SMOKE-TEST.md) for
verifying the endpoint before any CSI driver is pointed at it.

## Turning it back off

Drain the volumes first. The suffix search that finds a volume under a name its
`volumeHandle` does not carry runs **only while the flag is on** — with the flag off,
a volume left on a target VM's name is not resolvable, and its unpublish will report
success without detaching.

So: cordon and drain so every CSI volume is detached and renamed back, confirm with
`pvesm list <storage>` that nothing sits on a non-placeholder vmid, and only then
unset the flag. If you have already unset it and volumes are stranded, set it back on,
drain, and unset again — or rename them back by hand with the same endpoint.

## Required permissions

The rename endpoint enforces the same ACL set `move_disk` does — no new privilege
class — checked in code rather than only declared:

* `Datastore.Audit` and `Datastore.AllocateSpace` on `/storage/<storage>` for each
  storage backing CSI volumes (a rename is a write)
* `VM.Config.Disk` on `/vms/<target-vmid>` for every VM volumes may attach to, since
  the rename assigns ownership to that VM
* read access to the source volume, via `PVE::Storage::check_volume_access`

No `root@pam` credential is needed; a scoped API token works. `/vms/9999` needs
nothing — the placeholder is a name, not a VM, and no permission check is run against
it.

If the token was created with privilege separation (`--privsep 1`, the default), the
effective permissions are the **intersection** of the owning user's and the token's, so
each ACL must be granted twice:

```sh
pveum acl modify /storage/local --users  'kubernetes-csi@pve'     --roles CSIReassign
pveum acl modify /storage/local --tokens 'kubernetes-csi@pve!csi' --roles CSIReassign
```

Granting only the user, or only the token, yields `403 Permission check failed`.

Broad `VM.Config.Disk` across the VMs a cluster schedules onto is a real privilege
grant — it permits disk reconfiguration on those VMs, not just CSI volume renames.
Scope it to the VMs that actually run workloads with CSI volumes rather than granting
it at `/vms`.

[Volume snapshots](volumesnapshot.md) and the migration controller's copy path are
separate operations with their own credential requirements. Nothing here changes those.

## The endpoint's own guard

The rename endpoint refuses a volume that any VM config still references, and it does
so by walking the cluster vmlist rather than trusting the caller. `rename_volume`
performs no such check on its own — renaming a referenced volume would leave a
dangling reference and an unbootable disk — so this guard is what makes the endpoint
safe independent of driver bugs. Verify it fires (attempt a rename of an attached
volume; expect a refusal, not a rename) before trusting anything else about the
install.

## Troubleshooting

### `source volume ... not found` for a volume that exists

```
ControllerPublishVolume: failed to rename volume, attaching under its current name
  error="500 source volume 'local:9999/vm-9999-pvc-<uuid>.raw' not found"
```

Extension 0.3.0 registered `rename` and `copy` without PVE's `proxyto => 'node'`, so
the `{node}` segment in the request path was decorative: the handler ran on whichever
host received the HTTP request, which for this driver is always the single address the
cluster's `url` points at. `local` is node-local storage, so every volume on any other
node was invisible to it and the rename failed — even for a volume created seconds
earlier. **Upgrade every node to >= 0.3.1.**

Recognizing it:

- The failures partition perfectly by node. Renames succeed for volumes on the host in
  the cluster `url` and fail for volumes everywhere else, with no other pattern.
- Nothing else breaks. The rename is non-fatal, so pods attach and run normally; the
  feature is simply inert on every node but one.
- The message comes from the extension itself, so the package *is* installed and the
  endpoint *is* reachable. It is answering about the wrong machine, not failing to
  answer.

Nothing needs repairing afterwards. A failed rename leaves the volume under its
placeholder name, which is exactly what the PV's `volumeHandle` says, so each volume
renames correctly on its next attach once the nodes are upgraded. The same defect
affects `copy`, and would fail cross-node migration the same way.

## Live verification of the rename design (2026-08-12)

Same cluster as below (12 nodes, 67 CSI PVs, PVE 8, `local` directory storage, `raw`),
extension 0.3.0 on every node, flag and `proxmod_endpoint` on, scratch PVC pinned to a
worker.

| Check | Result |
|---|---|
| Attach renames `9999 → 3021`, pod reads/writes its mount | pass |
| `volumeHandle` still reads `…/9999/…` while attached | pass |
| `ControllerExpandVolume` and `ControllerModifyVolume` against the stale handle | pass |
| Detach renames back to `9999`, no `unused<n>` left behind | pass |
| Reschedule to another node — rename follows to the new vmid | pass |
| Controller killed mid-attach — `resolveVolume` adopts on retry | pass |
| PVC deleted — volume gone from storage, no orphan, VM config clean | pass |
| The other 66 CSI volumes unmoved | pass |

This run predates the 0.3.1 fix, and every check above passed only because the scratch
PVC's volume sat on the host the cluster `url` points at. Repeat it against a volume on
a different node before trusting the result on a fleet — that is the case the missing
`proxyto` broke, and this table did not cover it. See
[Troubleshooting](#troubleshooting).

## Live test result (2026-08-12)

The record of why `move_disk` was abandoned. Retained because the conclusion is
load-bearing for the current design.

Tested on a real cluster (12 nodes, 67 CSI PVs, PVE 8, `local` directory storage,
`raw`). Controller image built from the feature branch, flag enabled via the config
secret, scratch PVC attached to a pinned worker (vmid `3021`).

**Result: attach failed, unconditionally.**

```
FailedAttachVolume  rpc error: code = Internal desc = failed to reassign disk:
  500 could not find VM 9999, params=map[disk:scsi9 target-vmid:3021]
```

### Root cause

`move_disk` is not a storage operation — it is `PVE::API2::Qemu::move_vm_disk`, which
operates on a **disk key inside an existing VM's config**. It needs two things that
are not true here:

1. The source VM in the request path must exist. The call built
   `/nodes/<node>/qemu/<vmid>/move_disk` from `vol.VMID()`, which is `controllerVMID`
   = **9999** — a *naming placeholder embedded in the VolumeID*, not a real VM.
   Nothing with that id exists in the cluster, hence the 500.
2. The named disk key must be in that VM's config. `scsi9` was the key the volume had
   just been attached under on the **target** VM; it has never been a key of 9999.

Reordering does not save it. Before attach the volume is unattached and in no VM
config at all, so there is no disk key to name; after attach the volume is already on
the target. **A CSI volume at rest is an unattached volume whose name merely encodes
9999, and `move_disk` has no expression for renaming one.**

The earlier bench measurement that appeared to validate the approach (`9990` → `9991`)
used two *real* VMs with the disk genuinely attached to the source. That is the only
shape `move_disk --target-vmid` supports — VM-to-VM — and it is not the shape
`ControllerPublishVolume` is in.

The rename it performs is real, and is the behavior the current design depends on.
Measured against directory storage (qcow2) on PVE 9.2.6:

```
before: local:9990/vm-9990-disk-0.qcow2   (owned by vmid 9990)
after:  local:9991/vm-9991-disk-0.qcow2   (owned by vmid 9991)
```

Both the filename and its containing directory are rewritten to the target vmid.
`rename_volume` — what the endpoint calls — produces the same result for an
*unattached* volume.

### Blast radius while the flag was on

Cluster-wide, not scoped to the volume under test: the flag is a single bool on the
controller, so *every* `ControllerPublishVolume` whose encoded vmid differed from its
target — i.e. all of them — took the same path and got the same 500. Any pod needing a
fresh attach or re-attach was stuck in `ContainerCreating` for the duration.

Damage was nil: publish fails *before* any Proxmox mutation, so nothing was renamed. A
67-row PV snapshot taken before and after the window was byte-identical, 0 volumes
diverged from `/9999/`, and the scratch volume deleted cleanly with no orphan.

This is why the rename is now non-fatal to the attach.

### Incidental finding

After the controller restart, `csi-resizer` v2.1.0 re-issued `ControllerModifyVolume`
for all 44 PVCs carrying a `VolumeAttributesClass`, of which 25 were attempted: 5
failed with `unexpected end of JSON input` and 7 with `volume is not published`. This
is a resync triggered by *any* controller restart, not a regression — but the
`unexpected end of JSON input` error is unexplained and worth chasing separately.

It also makes `ControllerModifyVolume` a live path on every restart, which is why that
RPC must resolve the volume's current name rather than rebuild it from the handle.

## Verification checklist before enabling in production

- [ ] Install the extension on **one** node and run
      [`SMOKE-TEST.md`](../hack/proxmod-csi-storage/SMOKE-TEST.md) against a scratch
      volume, with no CSI driver involved.
- [ ] Confirm the in-use guard refuses a rename of an attached volume. This is the
      guard protecting every VM config on the cluster.
- [ ] Only then install on the remaining nodes.
- [ ] Provision a PVC, attach it to a pod. `pvesm list <storage>` shows
      `<storage>:<vmid>/vm-<vmid>-pvc-<uuid>.raw`, and
      `kubectl get pv <n> -o jsonpath='{.spec.csi.volumeHandle}'` still reads `…/9999/…`
      — and the pod reads and writes its mount anyway. **Handle validity is the point.**
- [ ] Delete the pod. The volume returns to `<storage>:9999/vm-9999-pvc-<uuid>.raw`,
      and no `unused<n>` key is left in the VM config.
- [ ] Reschedule to a **different** node — specifically one that is *not* the host the
      cluster's `url` points at — and confirm the rename follows to the new vmid. A
      rename that works only on the API host is the 0.3.0 failure described in
      [Troubleshooting](#troubleshooting), and nothing else in this list detects it.
- [ ] `ControllerExpandVolume` and `ControllerModifyVolume` succeed **while attached**,
      i.e. against a stale handle — the paths suffix matching exists to protect.
- [ ] Delete the PVC; the volume is gone from `pvesm list`, with no orphan.
- [ ] Kill the controller pod mid-attach; the volume is adopted on retry, not stranded.
- [ ] If you use the [migration controller](migration-controller.md) or [volume
      snapshots](volumesnapshot.md), confirm they still resolve volumes correctly.
