# Node instance-id write-back

`controller.annotateNodeInstanceID` (default: `false`) makes the controller record
the Proxmox VMID it resolved for a node back onto that node, as a
`proxmox.crunchymonkies.com/instance-id` annotation. Later attaches and detaches
read the annotation instead of searching the cluster for the VM again.

It is only worth anything where the node's `providerID` cannot carry the VMID. If a
Proxmox CCM sets `proxmox://<region>/<vmid>`, the driver already has the VMID for
free and this flag changes nothing.

## Why this exists

Every `ControllerPublishVolume` and `ControllerUnpublishVolume` needs the VMID of
the node the volume is going to. The controller resolves it in this order:

1. the VMID embedded in the CSI `NodeId`, which the node plugin fills in from the
   SMBIOS serial — but only when the VM's `smbios1` carries an `i=<vmid>` option,
   which Proxmox does not set by itself
2. `spec.providerID`, parsed as `proxmox://<region>/<vmid>`
3. the `proxmox.crunchymonkies.com/instance-id` annotation, then the legacy
   `proxmox.sinextra.dev/instance-id`
4. failing all of those, a search: `FindVMByNode` walks every VM in every
   configured cluster, and for each VM whose name starts with the node's name it
   reads the VM config and compares the SMBIOS UUID

Step 4 is correct and it is what makes the driver work at all on these clusters,
but it is a cluster-wide scan plus a config read per candidate, and it runs on
every single attach and detach for the lifetime of the node.

**rke2 is the case that cannot avoid it.** It sets `providerID: rke2://<name>` at
registration, and `providerID` is immutable — a Proxmox CCM cannot overwrite it, so
step 2 can never succeed, and nothing else writes the annotation for step 3. Once
this flag is on, the first attach pays for the scan and every attach after it reads
an annotation.

## What it does

On the fallback path only — steps 1 to 3 having all failed — the controller issues
a merge patch adding the annotation:

```yaml
metadata:
  annotations:
    proxmox.crunchymonkies.com/instance-id: "221"
```

It is best effort. The VMID is already resolved by the time the patch is attempted,
so a patch that fails is logged at `-v=3` and the RPC continues normally; the only
consequence is that the next lookup scans again. A controller without `patch` on
nodes therefore degrades to exactly the behaviour it has with the flag off, one
denied request per fallback.

Nothing re-reads or refreshes the annotation, because nothing needs to: once it is
there, step 3 answers and step 4 is never reached.

## Enabling it

```yaml
controller:
  annotateNodeInstanceID: true
```

The chart adds `--annotate-node-instance-id` to the controller and grants the
controller's ClusterRole `patch` on `nodes`, which it does not otherwise hold. Both
are gated on the same value, so there is no configuration in which the flag is set
without the permission to act on it.

To confirm it took effect:

```sh
kubectl get nodes -o custom-columns=\
NAME:.metadata.name,VMID:'.metadata.annotations.proxmox\.crunchymonkies\.com/instance-id'
```

An entry appears the first time a volume is attached to that node, not at startup.

## Caveats

**The annotation is not invalidated.** If a node is rebuilt under the same name on
a different VMID, the stale annotation wins over the search that would have found
the new VM, and attaches go to the wrong VMID or fail outright. This is inherent to
the annotation — the driver has read it since long before this flag existed, and a
Proxmox CCM writes the same key — but turning the write-back on means a cluster
that previously resolved fresh every time now caches. Delete the annotation to
force a re-resolve:

```sh
kubectl annotate node <name> proxmox.crunchymonkies.com/instance-id-
```

**It grants the controller write access to node objects.** The patch is confined to
one annotation key, but RBAC cannot express that: the grant is `patch` on `nodes`,
cluster-wide. If that is not acceptable, leave the flag off and pay for the scan.

**It does not help the migration controller**, which resolves nodes through its own
config and not through this path.

## See also

- [`docs/install.md`](install.md) — where this sits in a full install
- [`docs/architecture.md`](architecture.md) — how nodes and VMIDs relate
