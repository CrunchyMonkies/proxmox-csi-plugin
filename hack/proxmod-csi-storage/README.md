# proxmox-csi-storage

**Status:** Living
**Applies to:** Proxmox VE 9.x, proxmod >= 0.1.0

A [proxmod](https://github.com/CrunchyMonkies/proxmod) extension that gives
this plugin a token-authorised version of the one operation the [migration
controller](../../docs/migration-controller.md) currently needs `root@pam`
for. It adds:

- `GET /nodes/{node}/proxmod/csi-storage` — an index
- `POST /nodes/{node}/proxmod/csi-storage/copy` — cross-storage volume copy,
  used by the migration controller and by the CSI driver's volume snapshots
  ([`docs/volumesnapshot.md`](../../docs/volumesnapshot.md))
- `POST /nodes/{node}/proxmod/csi-storage/rename` — reassign an **unattached**
  volume to another vmid, used by the CSI driver's
  [`features.reassignVolumeOnAttach`](../../docs/reassign-volume-on-attach.md)

Both are `protected => 1` (bridged to root inside `pvedaemon`) but gated by
an explicit ACL `check` instead of PVE's usual root@pam-only default, so a
scoped API token can call them.

## Relationship to [`../pve-token-copy/`](../pve-token-copy/)

This repo also ships a hand-rolled version of the `copy` endpoint at
`hack/pve-token-copy/`, built before proxmod existed. It works by injecting
a `-MPVECSICopy` module load onto the `pvedaemon`/`pveproxy` systemd
`ExecStart` line, to route around `perl -T` taint mode ignoring
`PERL5LIB`/`PERL5OPT`. This extension supersedes that workaround: proxmod's
own `-MProxmod` injection and manifest-driven `require` make the systemd
wrapper unnecessary, and the endpoint moves off `/nodes/{node}/storage/{storage}/csi-copy`
(a sibling to PVE's own `content/{volume}`, chosen specifically to dodge that
route's greedy `{volume}` param) onto proxmod's isolated
`/nodes/{node}/proxmod/csi-storage` subtree, where a routing collision with
any PVE-owned tree is structurally impossible.

`hack/pve-token-copy/` is not removed by shipping this — both remain
supported so a mixed fleet can migrate node by node, and the Go client picks
between them per cluster (see [Using it](#using-it) below).

## Using it

Install on each Proxmox node (needs [proxmod](https://github.com/CrunchyMonkies/proxmod)
installed first — this package `Depends:` on it). A prebuilt
`proxmox-csi-storage_<ver>_all.deb` is attached to every
[fork release](https://github.com/CrunchyMonkies/proxmox-csi-plugin/releases);
its version tracks `debian/changelog`, not the driver version, so the same deb
usually spans several driver releases.

To build it yourself instead:

```sh
make -C hack/proxmod-csi-storage build          # syntax check
cd hack/proxmod-csi-storage && dpkg-buildpackage -b -us -uc
# produces hack/proxmox-csi-storage_<ver>_all.deb
```

Then point the client at it, either per cluster in the config file:

```yaml
clusters:
  - url: https://cluster-api-1.example.com:8006/api2/json
    token_id: "kubernetes-csi@pve!csi"
    token_secret: "secret"
    region: Region-1
    proxmod_endpoint: true
```

or globally with `pvecsictl --proxmod-endpoint`. `proxmod_endpoint` and
`token_copy_endpoint` are mutually exclusive in practice; if both are set,
proxmod wins. The same setting also routes the CSI controller's **volume
snapshots** through this endpoint — see
[`docs/migration-controller.md`](../../docs/migration-controller.md) and
[`docs/volumesnapshot.md`](../../docs/volumesnapshot.md).

**Directory storage needs >= 0.2.0.** `parse_volume_id` only validates the
storage-id prefix (the volname part is `.+`), so the endpoint does not rely on it
to reject traversal: `volume` and the target volname are both constrained by an
explicit allow-list, `^(?:[1-9][0-9]{2,8}/)?[A-Za-z0-9][A-Za-z0-9._-]*$`
(maxLength 160). The optional leading component is a PVE VMID (100–999999999) and
nothing else, which admits the directory plugin's `9999/vm-9999-disk-0.raw` shape
— every volume on a file-backed storage, including every CSI snapshot — while
still rejecting `:`, `..`, absolute paths, and deeper nesting. Version 0.1.0 had
no VMID component and returned `400` for all of those. There is deliberately no
client-side fallback to the built-in endpoint, so an out-of-date package fails
loudly rather than quietly reinstating the root requirement.

Installing proxmod rewrites the `pvedaemon`/`pveproxy` systemd `ExecStart` and
restarts both daemons — a brief API interruption on that node. See
[`SMOKE-TEST.md`](SMOKE-TEST.md) for a rollback-tested install procedure.

## The `rename` endpoint (>= 0.3.0)

```sh
curl -sk -X POST -H "Authorization: PVEAPIToken=$TOKENID=$SECRET" \
  --data-urlencode "storage=local" \
  --data-urlencode "volume=9999/vm-9999-pvc-abc.raw" \
  --data-urlencode "target_vmid=3021" \
  --data-urlencode "target_volname=vm-3021-pvc-abc.raw" \
  "https://$HOST:8006/api2/json/nodes/$NODE/proxmod/csi-storage/rename"
# expect: {"data":"local:3021/vm-3021-pvc-abc.raw"}
```

Synchronous — a rename is an in-directory `rename(2)` on dir/LVM/ZFS, so there
is no worker to fork and no UPID to poll. It returns the volume's new volid as a
plain string.

`target_volname` is the **bare filename**, never the `<vmid>/<name>` form the
`volume` parameter accepts: `PVE::Storage::Plugin::rename_volume` composes the
destination itself as `<basedir>/<target_vmid>/<target_volname>`, so a slash
there would be a traversal. It is validated by a correspondingly stricter
allow-list, and may be omitted, in which case PVE picks a free
`vm-<target_vmid>-disk-N`.

### Why it has to exist

`PVE::Storage::rename_volume` is the only mechanism PVE has for reassigning an
**unattached** volume to another vmid, and it has no REST endpoint — it is
reachable only from Perl.

The obvious alternative does not work. `move_disk`'s `target-vmid`
(`PVE::API2::Qemu::move_vm_disk`) operates on a **disk key inside an existing
VM's config** and moves it between two **real** VMs. A CSI volume at rest is
unattached, and the vmid baked into its filename (`9999`) is a naming
placeholder that exists as no VM — so the call fails with
`500 could not find VM 9999` before it does anything. Reordering cannot save it:
before attach there is no disk key to name, and after attach the volume is
already on the target. See
[`docs/reassign-volume-on-attach.md`](../../docs/reassign-volume-on-attach.md)
for the full account, including the live test that established it.

An earlier revision of this extension shipped a `reassign` endpoint wrapping
`move_disk`, on the assumption that PVE required `root@pam` for it. That
assumption was also wrong — `move_vm_disk` is ACL-gated, not root-only — and the
endpoint was removed for adding no capability while skipping a check PVE itself
enforces. `rename` is not a revival of it: different PVE call, different shape,
and it is the only one of the two that can express what the driver needs.

### Permissions

The same ACL set `move_disk` enforces, so no new privilege class is introduced:

- `Datastore.Audit` on `/storage/<storage>` (the declared `check`), plus
  `PVE::Storage::check_volume_access` on the source volume
- `Datastore.AllocateSpace` on `/storage/<storage>` — a rename is a write
- `VM.Config.Disk` on `/vms/<target_vmid>` — it assigns ownership to that VM

Under privilege separation (`--privsep 1`, the default) each must be granted to
**both** the owning user and the token; privsep tokens get the intersection.

### The in-use guard

`rename_volume` performs **no in-use check of its own** — unlike `vdisk_free`,
it will happily rename a volume a running VM is using, leaving a dangling
config reference and an unbootable disk. So the endpoint supplies one: it walks
the cluster vmlist and refuses if any qemu or lxc config references the source
volume. Failure to read any config is fatal, so the scan fails closed.

One reference is deliberately tolerated: a **pure `unused<n>`** entry, with no
attached drive, no pending change and no snapshot reference. That is the residue
the CSI driver's own detach leaves behind, and it removes it *after* the rename.

**That ordering is load bearing, and callers must preserve it.** Deleting an
`unused<n>` key whose volume still exists makes PVE deallocate the volume for
real (`try_deallocate_drive` → `vdisk_free`); deleting one whose volume has
already been renamed away is a no-op (`Plugin::free_image` warns
`disk image ... does not exist` and returns). Rename first, then clear the key.

## Read it in this order

1. `conf/50-proxmox-csi-storage.conf` — the manifest
2. `perl/ProxmodExt/CSIStorage.pm` — `proxmod_register()`, the `copy`
   endpoint's ACL model, and `rename`'s in-use guard
3. `Makefile` and `debian/` — packaging. `t/lib/PVE/` holds from-scratch
   compile-check stubs, not real PVE modules; see the header of each.

No frontend asset: this extension is backend-only, nothing renders in the
web UI.
