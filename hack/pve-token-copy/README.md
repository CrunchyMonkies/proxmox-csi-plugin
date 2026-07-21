# Token copy-volume endpoint (root@pam-free migration)

Lets the migration controller move VM-disk volumes between storages **with an API
token** instead of `root@pam`, by adding a permission-checked copy endpoint to PVE
— **without patching any Proxmox file**.

## Why
PVE's built-in `copy` content method (`POST /nodes/{node}/storage/{storage}/content/{volume}`)
has **no `permissions` block**, so PVE restricts it to `root@pam`. That is the only
reason the migrator needs full root: it performs a disk copy that no supported,
token-usable API covers (`pvesm copy-volume`, the upcoming supported successor,
explicitly **excludes** VM-disk `images`/`rootdir`, deferring them to `qm disk move`,
which needs a real owning VM — CSI volumes are parked on a placeholder VMID).

This adds a **sibling** method with a proper permission gate, so a scoped token can do it.

## How it works (no Proxmox files modified)
- A self-contained Perl module tree is installed under `/usr/lib/pve-csi-copy/perl/`
  — a path we own, not a Proxmox one:
  - `PVECSICopy.pm` — a **byte-trivial, always-compiles loader**. It does nothing but
    `eval { require PVECSICopy::Impl; PVECSICopy::Impl::register(); }`.
  - `PVECSICopy/Impl.pm` — all the real logic (the method + its handler).
- Two systemd drop-ins (`/etc/systemd/system/{pvedaemon,pveproxy}.service.d/`) set
  `PERL5LIB` + `PERL5OPT=-MPVECSICopy`, so both daemons load the loader at startup.
- On load, `Impl::register()` calls `PVE::API2::Storage::Status->register_method(...)`,
  adding `POST .../storage/{storage}/csi-copy` to the live API tree (idempotently).
  It registers at the **storage level, not under `content/`**: the content subtree is
  mounted with a fragment-joining, **greedy `{volume}` path parameter** (deliberate,
  so dir-storage volnames like `9999/vm-9999-disk-0.raw` can contain `/`), which
  swallows any `content/{volume}/suffix` path — a method registered there exists but
  is unreachable over HTTP (requests route to the built-in root-only `copy` with
  `volume="<vol>/copy"`). Do not move it back.
- **Fail-safe by construction**: the loader is trivial so it always compiles, and it
  wraps *all* implementation work — including the `require` that compiles `Impl.pm` —
  in `eval`. So a compile error, a changed PVE internal, or any `die` in the impl is
  caught: the module warns and registers nothing, and **the daemon still starts**. A
  load-time failure can therefore never take pvedaemon/pveproxy (the node's
  management API) down. PVE is left exactly as shipped; there is no Proxmox file to
  re-apply after an upgrade.

## The endpoint
```
POST /nodes/{node}/storage/{storage}/csi-copy
  volume       = "<src-volname>"            # source volume within {storage}
  target       = "<dst-storage>:<dst-volname>"
  target_node  = <node>   # optional, default local
```
Returns a task UPID. The source volume is a **body** parameter — do not repeat
`node` (or `storage`) in the body with a different value; PVE rejects URI/body
parameter conflicts.

## Security model
Auth is PVE's own ACL — not a bespoke allow-list. A copy is a **read of the source**
and a **write to the target**, and those are gated differently (getting this wrong was
the main review finding):

- **Source (read).** The `permissions` block requires `Datastore.Audit` on the source
  storage, and the handler *additionally* calls
  `PVE::Storage::check_volume_access(...)` on the specific source volume. This is what
  stops a token that merely has create rights on a shared storage from copying (i.e.
  exfiltrating) a volume owned by another VM/tenant. `Datastore.AllocateSpace` alone is
  **not** sufficient for the source — it is a create privilege, not a read privilege.
- **Target (write).** The handler checks `Datastore.AllocateSpace` on the target
  storage.
- **Runs as root only inside `pvedaemon`** (the method is `protected`); **no root
  credential is stored anywhere** — the token authorises, `pvedaemon` provides root.
- **Input validation.** `parse_volume_id` only validates the *storage-id* prefix (the
  volname part is `.+`), so it is **not** relied on to reject traversal. The `volume`
  parameter is constrained by an explicit pattern (`^[A-Za-z0-9][A-Za-z0-9._-]*$`),
  rejecting `:`, `/` and `..` before it is ever concatenated into a volid.
- **No clobber** (advisory): refuses if the target volume already exists. This is
  best-effort against a TOCTOU race; the hard guarantee is the target plugin's
  exclusive `alloc` during `storage_migrate`.
- Net capability of a fully-compromised token holder: copy a volume it has read access
  to onto a storage it has `AllocateSpace` on — **not** root code execution, arbitrary
  fs access, or reach into storages/volumes outside its ACL.

## Install (per PVE node)
This is a **host-level** package (systemd + Perl on the hypervisor) — it installs on
each PVE node the CSI plugin targets, over SSH, not through the Proxmox API and not
into a guest VM. Two ways:

### 1. Debian package (recommended)
A `.deb` is built by the [`pve-csi-copy deb`](../../.github/workflows/pve-csi-copy-deb.yml)
GitHub Action — as a CI artifact on every PR, and attached to a GitHub Release when a
`pve-csi-copy-v*` tag is pushed. Install it with `apt`/`dpkg`; its maintainer scripts
place the loader drop-ins, restart `pvedaemon`/`pveproxy`, and verify registration.
For a fleet, install it with **Ansible** — see [`ANSIBLE.md`](ANSIBLE.md).

```bash
apt install ./pve-csi-copy_0.2.0_all.deb      # or: dpkg -i …
pve-csi-copy-verify                            # ships in the package
```

Build it locally the same way CI does:
```bash
cd hack/pve-token-copy && dpkg-buildpackage -b -us -uc   # -> ../pve-csi-copy_*.deb
```

### 2. install.sh (no packaging / dev)
```bash
./install.sh install     # drops module + loaders, restarts pvedaemon/pveproxy, verifies
./install.sh verify
./install.sh uninstall
```

Either way, install restarts `pvedaemon`/`pveproxy` (a few seconds of API downtime;
running guests and existing CSI mounts are unaffected), and it must be present on
**every** PVE node CSI targets.

## Migrator configuration change
With this installed, the migrator (`pvecsictl` / the migration controller) no longer
needs `root@pam`. Pass **`--token-copy-endpoint`** (or set `token_copy_endpoint: true`
per cluster in the cloud-config) and point the cloud config at a **token** — omit
`username`/`password`, since the client prefers them when both are present. On the CSI
storages the token needs:
- `Datastore.Audit` (+ access to the source volumes — for CSI-owned volumes the
  `kubernetes-csi@pve` identity already owns them) on the **source**, and
- `Datastore.AllocateSpace` on the **target**.

It also needs the privileges the rest of the migration uses: `VM.Audit`, `VM.Allocate`
+ `VM.Config.Disk` (qcow2/vmdk helper-VM conversion), and `Datastore.Allocate`
(partial-file / helper cleanup). The migrator then holds only a scoped token — no root
credential in the cluster. See [migration-controller.md](../../docs/migration-controller.md).

## Caveats / must-validate before production
- **Per-major-version validation.** This couples to PVE internals (`register_method`,
  `check_volume_access`, `parse_volname`, `storage_migrate`). It fails *safe* (a
  changed internal → the endpoint silently doesn't register, daemons still start), but
  that also means the migrator quietly loses the endpoint. After every PVE upgrade,
  run `install.sh verify` (asserts the running daemons registered it) and do a real
  copy. Consider wiring the verify into monitoring so a post-upgrade regression alerts
  rather than surfacing as a stuck migration.
- **The permission gate is the whole control.** You are re-enabling a root-capable copy
  for token holders. Keep the source `check_volume_access` + `Datastore.Audit` and the
  target `Datastore.AllocateSpace` checks, and scope the token's ACL tightly. Confirm
  on your PVE version that `check_volume_access`'s signature/behaviour matches — it is
  the control that prevents cross-tenant volume reads.
- **`$PERL5LIB` is in a root daemon's `@INC`.** `/usr/lib/pve-csi-copy/perl` is
  root-owned `0755` / files `0644`, and `install.sh` refuses to proceed if anything
  there is non-root-owned or group/world-writable — because a writable entry would be
  unauthenticated root code-exec in pvedaemon. If you ever repackage this (`.deb`,
  DaemonSet bind-mount), preserve those perms and don't source the tree from anywhere
  less trusted than root.
- **Same-node copy + `storage_migrate`.** The built-in `copy` code notes an
  ssh-to-localhost issue for local targets; the worker sshes as root to `target_node`
  (bounded to a valid cluster node, not an arbitrary host). Confirm same-node copies
  work on your PVE version, or special-case local (skip ssh) in the worker.
- **`PERL5OPT` inheritance.** The env var is inherited by any child `perl` the daemons
  fork. The loader is trivial and fails safe, so this is low-risk, but it is a wider
  surface than a single daemon.
- **Not an official Proxmox extension point.** PVE has no supported API-plugin hook;
  this is a community mechanism (same class as the widely-used systemd/PERL5OPT hooks),
  chosen over patching Proxmox files because it fails safe and survives upgrades.

## Review
An adversarial security review (Fable 5) of an earlier revision produced the fixes now
folded in: source read gated by `check_volume_access` + `Datastore.Audit` (was
`AllocateSpace`-only → cross-tenant copy); loader split into a trivial always-compiles
shim + isolated impl so no load-time failure can down the daemons; explicit `volume`
pattern (parse_volume_id does not reject traversal); `install.sh` now verifies the
*running* daemons, rolls back on failure, and hard-fails on insecure `@INC` perms.
Residual items are the caveats above — re-validate on the target PVE version and on-host
before production; this remains a draft until that on-host validation is done.
