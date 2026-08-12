# On-cluster smoke test

Verifies that the `csi-storage` extension registers, that its endpoints answer,
that their ACL gates actually gate, and that `rename` refuses a volume some VM
is still using — end to end on a real Proxmox node, using only scratch volumes
and one scratch VM.

**This procedure has not been run.** It is written down so the install is a
reviewed, deliberate step rather than an improvised one. Read
[Blast radius](#blast-radius) before starting.

## Blast radius

Installing `proxmod` is **not** a passive operation. Its `postinst` runs
`proxmod-reapply`, which copies `10-proxmod.conf` into
`/etc/systemd/system/pvedaemon.service.d/` and `/etc/systemd/system/pveproxy.service.d/`,
rewriting each unit's `ExecStart` to route through `/usr/lib/proxmod/proxmod-exec`,
then `try-restart`s both units.

That is necessary — `perl -T` ignores `PERL5LIB` and `PERL5OPT`, so `-M` on the
command line is the only way to get a module into `pvedaemon`'s interpreter — but
it means **the Proxmox API on that node briefly restarts**. On a node that is the
CSI cluster's API endpoint, the CSI controller, the CCM, and the migrator all talk
to it. Expect a short interruption; run it during a quiet window.

Three things limit the downside:

* **`panic_unwrap`** — if a unit fails to come back after the rewrite, proxmod
  removes its own drop-ins and restores the stock unit automatically.
* **`proxmodctl disable`** (writes `/etc/proxmod/disabled`) — kill switch that
  makes the wrapper a no-op without uninstalling anything.
* **`apt remove proxmod`** — runs `proxmod-reapply --remove`, restoring stock
  units.

Pick a node deliberately. Doing this on the node the CSI cluster URL points at
tests the realistic path but takes the widest outage; doing it on a non-endpoint
node is safer but does not exercise the client.

## Prerequisites

Both packages, built locally (see [README.md](README.md) and proxmod's own
`make deb`):

* `proxmod_<version>_all.deb`
* `proxmox-csi-storage_<version>_all.deb`

Pick a **target node** (`$NODE`) and note its API address (`$HOST`).

## 1. Install

```sh
scp proxmod_*_all.deb proxmox-csi-storage_*_all.deb root@$HOST:/tmp/
ssh root@$HOST 'apt install -y /tmp/proxmod_*_all.deb /tmp/proxmox-csi-storage_*_all.deb'
```

## 2. Verify the injection is sane

```sh
ssh root@$HOST '
  proxmod-verify                                              # exit 0
  proxmodctl list                                             # lists csi-storage
  systemctl status pvedaemon pveproxy --no-pager               # both active
  dpkg -V pve-manager libpve-common-perl libpve-http-server-perl   # silent
'
```

`dpkg -V` being silent is the important one: it proves no Proxmox-owned file was
modified. The extension adds files under `/usr/share/perl5/ProxmodExt/` and
`/usr/share/proxmod/extensions.d/` only.

## 3. Confirm the endpoint is registered

**Use HTTPS against the API, not `pvesh`.** `pvesh` builds its own API tree in a
process that never went through the proxmod wrapper, so extensions are invisible
to it and it will report the path as nonexistent. This is a property of how
`pvesh` works, not a failure.

```sh
curl -sk -H "Authorization: PVEAPIToken=$TOKENID=$SECRET" \
  "https://$HOST:8006/api2/json/nodes/$NODE/proxmod/csi-storage"
# expect: {"data":[{"subdir":"copy"},{"subdir":"rename"}]}
```

## 4. Positive test: copy a scratch volume

Same node, `local` → `datastore1`, so the copy never needs inter-node SSH.

Create a scratch volume owned by a scratch VMID that does not exist:

```sh
ssh root@$HOST 'pvesm alloc local 9990 "" 1G'
# -> local:9990/vm-9990-disk-0.raw
```

Grant a scoped token the minimum the endpoint checks:

```sh
ssh root@$HOST '
  pveum role add CSICopySrc -privs "Datastore.Audit"
  pveum role add CSICopyDst -privs "Datastore.AllocateSpace"
  pveum user add csi-smoke@pve
  pveum user token add csi-smoke@pve smoke --privsep 1
  # privsep tokens get the INTERSECTION of user and token ACLs — grant each twice
  pveum acl modify /storage/local      --roles CSICopySrc --users  csi-smoke@pve
  pveum acl modify /storage/local      --roles CSICopySrc --tokens "csi-smoke@pve!smoke"
  pveum acl modify /storage/datastore1 --roles CSICopyDst --users  csi-smoke@pve
  pveum acl modify /storage/datastore1 --roles CSICopyDst --tokens "csi-smoke@pve!smoke"
'
```

Then POST the copy. Note `storage` is a **body** parameter here, not a URI
segment — the method is mounted at node scope inside proxmod's subtree:

```sh
curl -sk -X POST -H "Authorization: PVEAPIToken=$TOKENID=$SECRET" \
  --data-urlencode "storage=local" \
  --data-urlencode "volume=9990/vm-9990-disk-0.raw" \
  --data-urlencode "target=datastore1:9990/vm-9990-disk-0.raw" \
  --data-urlencode "target_node=$NODE" \
  "https://$HOST:8006/api2/json/nodes/$NODE/proxmod/csi-storage/copy"
# expect: a UPID
```

Poll the task, then confirm the volume landed:

```sh
ssh root@$HOST 'pvesm list datastore1 | grep 9990'
```

**Do not skip this as a formality — it is the only test of the one unverified
assumption in the design.** Both storages here are directory-backed, so the
volname carries a leading VMID component on *both* sides:
`volume=9990/vm-9990-disk-0.raw` and a `target_volname` of
`9990/vm-9990-disk-0.raw`. Two things are being settled at once:

1. The 0.2.0 allow-list admits that shape (0.1.0 returned `400` for it, on both
   the source parameter and the target). Directory storage is often the *only*
   snapshot-capable storage on a cluster — `CreateSnapshot` rejects `cifs`,
   `pbs`, `rbd`, and anything `shared` — so this is the common case, not a
   corner.
2. `PVE::Storage::storage_migrate` accepts a **slashed `target_volname`**. That
   is inferred, not documented: the built-in content `copy` sends the same
   `datastore1:9990/vm-9990-disk-0.raw` form successfully, and this endpoint is
   a port of it making the identical `storage_migrate` call. If the inference is
   wrong, it fails here, at the API, with no PVC involved.

For a block storage (LVM, LVM-thin, ZFS) the same request uses the flat form on
both sides — `volume=vm-9990-disk-0`, `target=<storage>:vm-9990-disk-0` — which
0.1.0 also accepted. Worth a second pass if the fleet has both storage classes.

## 5. Negative test: the ACL gate

An untested permission check is an unproven one — this is the entire reason the
endpoint exists, so do not skip it.

```sh
ssh root@$HOST 'pveum acl delete /storage/datastore1 --roles CSICopyDst --tokens "csi-smoke@pve!smoke"'
```

Re-run the POST from step 4 (to a fresh target name). **Expect HTTP 403.** A
success here means the endpoint is not checking `Datastore.AllocateSpace` on the
target and must not be deployed.

## 6. Negative test: `rename`'s in-use guard

**Do this before the positive rename test.** `PVE::Storage::rename_volume`
performs no in-use check of its own, so this guard is the only thing standing
between a caller's mistake and a dangling disk reference in a real VM's config.
An unproven guard is not a guard.

Grant the token what `rename` checks, and give it a scratch VM to target:

```sh
ssh root@$HOST '
  pveum role add CSIRename -privs "Datastore.Audit,Datastore.AllocateSpace"
  pveum role add CSIRenameVM -privs "VM.Config.Disk"
  pveum acl modify /storage/local --roles CSIRename   --users  csi-smoke@pve
  pveum acl modify /storage/local --roles CSIRename   --tokens "csi-smoke@pve!smoke"
  pveum acl modify /vms/9991      --roles CSIRenameVM --users  csi-smoke@pve
  pveum acl modify /vms/9991      --roles CSIRenameVM --tokens "csi-smoke@pve!smoke"

  qm create 9991 --name csi-smoke --memory 512 --net0 virtio,bridge=vmbr0
  qm set 9991 --scsi1 local:9990/vm-9990-disk-0.raw   # left stopped throughout
'
```

The volume is *foreign* to VM 9991 (its name still says 9990), which is
deliberate: it is the same relationship a CSI volume has to the VM it is
attached to today, and it means `--delete scsi1` below drops the key without
deallocating anything.

Now attempt the rename while it is attached:

```sh
curl -sk -X POST -H "Authorization: PVEAPIToken=$TOKENID=$SECRET" \
  --data-urlencode "storage=local" \
  --data-urlencode "volume=9990/vm-9990-disk-0.raw" \
  --data-urlencode "target_vmid=9991" \
  --data-urlencode "target_volname=vm-9991-disk-0.raw" \
  "https://$HOST:8006/api2/json/nodes/$NODE/proxmod/csi-storage/rename"
# expect: an error naming VM 9991 and the scsi1 key — NOT a volid
```

Confirm nothing moved:

```sh
ssh root@$HOST 'pvesm list local | grep 9990'   # still 9990/vm-9990-disk-0.raw
```

**A success here means the endpoint is not scanning VM configs and must not be
deployed.** A renamed-out-from-under-it disk is silent until the VM next
starts.

## 7. Positive test: rename an unattached volume

Detach first, then re-issue the identical request:

```sh
ssh root@$HOST 'qm set 9991 --delete scsi1'
```

```sh
curl -sk -X POST -H "Authorization: PVEAPIToken=$TOKENID=$SECRET" \
  --data-urlencode "storage=local" \
  --data-urlencode "volume=9990/vm-9990-disk-0.raw" \
  --data-urlencode "target_vmid=9991" \
  --data-urlencode "target_volname=vm-9991-disk-0.raw" \
  "https://$HOST:8006/api2/json/nodes/$NODE/proxmod/csi-storage/rename"
# expect: {"data":"local:9991/vm-9991-disk-0.raw"}
```

```sh
ssh root@$HOST 'pvesm list local | grep -E "9990|9991"'
# expect: 9991/vm-9991-disk-0.raw only; the 9990 name is gone
```

Two properties are being settled here, both of which the CSI driver relies on:

1. `target_volname` is the **bare filename**. The plugin composes the path as
   `<basedir>/<target_vmid>/<target_volname>` itself, so `9991/vm-9991-disk-0.raw`
   would be a second path component it never intended — the endpoint rejects
   that shape with `400`, unlike the `volume` parameter which requires it on
   directory storage. Worth sending once to confirm the asymmetry is real.
2. The containing directory moves with the volume. On directory storage the
   volume physically relocates from `<basedir>/images/9990/` to
   `…/9991/`; PVE creates the target directory and leaves the emptied source
   one behind (`rmdir` only happens on free).

Finally, prove the ACL gate on the target VM, mirroring step 5:

```sh
ssh root@$HOST 'pveum acl delete /vms/9991 --roles CSIRenameVM --tokens "csi-smoke@pve!smoke"'
```

Re-issue the rename (target `9992`, or back to `9990`). **Expect HTTP 403.**

## 8. Cleanup

Free the volume **before** destroying the scratch VM: after step 7 the volume is
genuinely owned by 9991, so `qm destroy` would deallocate it — correct
behaviour, but it makes the "no orphan left" check vacuous.

```sh
ssh root@$HOST '
  pvesm free local:9991/vm-9991-disk-0.raw       || true
  pvesm free local:9990/vm-9990-disk-0.raw       || true
  pvesm free datastore1:9990/vm-9990-disk-0.raw  || true
  qm destroy 9991                                || true
  pveum user token remove csi-smoke@pve smoke
  pveum user delete csi-smoke@pve
  pveum acl delete /storage/local      --roles CSICopySrc
  pveum acl delete /storage/local      --roles CSIRename
  pveum acl delete /storage/datastore1 --roles CSICopyDst
  pveum acl delete /vms/9991           --roles CSIRenameVM
  pveum role delete CSICopySrc
  pveum role delete CSICopyDst
  pveum role delete CSIRename
  pveum role delete CSIRenameVM
  apt remove -y proxmod proxmox-csi-storage
  proxmod-verify
  systemctl status pvedaemon pveproxy --no-pager
'
```

The final `proxmod-verify` and `systemctl status` confirm the stock units came
back — that the node is exactly as it was before step 1.

## What this does not test

The CSI driver and migrator are untouched by this procedure. Wiring
`--proxmod-endpoint` (or `proxmod_endpoint: true`) into a live migrator and
moving a real PVC is a separate exercise with a much larger blast radius; do it
only after this smoke test passes and the packages stay installed.
