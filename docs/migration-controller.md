# Volume Migration Controller

The migration controller automates what `pvecsictl migrate` does manually: moving the backing Proxmox disk of a PersistentVolumeClaim to another Proxmox node (zone). It watches for request annotations and executes migrations **strictly one at a time** (force migrations cordon every CSI node, so concurrency is never safe).

It is shipped as a subcommand of `pvecsictl`:

```shell
pvecsictl controller --config=/etc/proxmox/config.yaml
```

**Warning**: like `pvecsictl migrate`, the controller requires a Proxmox **root** account (username/password) in its cloud config. Keep that config in a dedicated secret, separate from the CSI controller's token-based config.

## Annotation protocol

All annotations use the driver prefix `csi.proxmox.sinextra.dev`.

### Request annotations (set these on a PVC)

| Annotation | Value | Meaning |
|---|---|---|
| `csi.proxmox.sinextra.dev/migrate-node` | Proxmox node name | Migrate this PVC's volume to the given zone |
| `csi.proxmox.sinextra.dev/migrate-force` | `"true"` | Allow disrupting pods that use the PVC (cordon + delete pods). Without it, an in-use PVC is skipped |
| `csi.proxmox.sinextra.dev/migrate-storage` | storage ID | Move the disk into this storage on the target node (default: keep the storage name) |

Example:

```shell
kubectl -n default annotate pvc storage-test-0 csi.proxmox.sinextra.dev/migrate-node=hvm-2
```

### Status annotations (written by the controller)

| Annotation | Meaning |
|---|---|
| `csi.proxmox.sinextra.dev/migrate-phase` | `Pending`, `Draining`, `Moving`, `Rewiring`, `Completed`, or `Failed` |
| `csi.proxmox.sinextra.dev/migrate-message` | Last error or progress message |
| `csi.proxmox.sinextra.dev/migrate-attempts` | Reconcile attempts so far |
| `csi.proxmox.sinextra.dev/migrate-started-at` | RFC3339 start time |

The controller also emits Events on the PVC (`MigrationStarted`, `MigrationMoving`, `MigrationCompleted`, `MigrationSkipped`, `MigrationError`, `MigrationFailed`), visible in `kubectl describe pvc`.

`Failed` is terminal: the controller will not retry until you change or remove the request annotations. Transient errors are retried with exponential backoff up to `--max-attempts` (default 5).

### Node evacuation (set these on a Kubernetes Node)

| Annotation | Value | Meaning |
|---|---|---|
| `csi.proxmox.sinextra.dev/evacuate` | Proxmox node name or `auto` | Request migration of **all** CSI volumes out of this node's zone. `auto` picks a target per volume by free capacity |
| `csi.proxmox.sinextra.dev/evacuate-force` | `"true"` | Stamp `migrate-force` on the evacuated PVCs |

Example — drain all volumes off the Proxmox node that hosts `kube-store-11` before maintenance:

```shell
kubectl annotate node kube-store-11 csi.proxmox.sinextra.dev/evacuate=auto
```

The controller expands this into per-PVC `migrate-node` annotations and removes the node annotation. The migrations then run one at a time.

### Volume follows pods (opt-in)

With `--pod-follow` (Helm: `migrator.podFollow: true`), the controller watches pods and migrates a volume automatically when **all pods mounting its PVC** have been scheduled onto nodes in a **different zone** than the volume — for example after the Kubernetes-node VM was migrated to another Proxmox host (Proxmox CCM keeps the zone labels current), leaving pods stuck in `ContainerCreating` because the disk cannot attach across Proxmox nodes.

A migration is requested only when the pods have *settled*:

- at least one pod references the PVC and every one of them is scheduled (`spec.nodeName` set, not Succeeded/Failed)
- all pods are in the **same** zone, in the volume's region
- that zone differs from the volume's zone
- the volume is zonal (shared-storage volumes are reachable from every zone and are never moved)

Target storage selection:

1. If the pods' zone hosts a storage with the **same name** as the volume's, the disk moves there (same storage name, new node).
2. Otherwise the cluster's **`primary_storage`** map decides — configure it per Proxmox node in the migrator cloud config:

   ```yaml
   clusters:
     - url: https://cluster-api-1.example.com:8006/api2/json
       username: root@pam
       password: "strong-password"
       region: cluster-1
       primary_storage:
         pve-3: local-zfs    # volumes following pods to pve-3 land on local-zfs
   ```

   The controller stamps `csi.proxmox.sinextra.dev/migrate-storage` and the disk is moved **into that storage** on the target node (the PV's volume handle is rewritten accordingly).
3. If neither matches, the controller emits a `FollowSkipped` warning event on the PVC and does nothing.

Pod-follow never force-drains: stuck pods are left in place, and once the PV/PVC are rewired the attach retry succeeds. Migrations triggered this way go through the same serialized, resumable pipeline as annotation requests.

### Storage behavior

- **Shared storage is detected at runtime**: before moving, the controller checks whether the disk is already visible on the target node's storage. If it is, the storage is shared between the nodes — the migration is **skipped** (`MigrationSkipped` event, phase `Skipped`, request annotations removed) instead of interrupting or self-overwriting the file.
- **qcow2/vmdk volumes are converted to raw during migration**: the Proxmox copy endpoint cannot stream those formats (its `raw+size` export of qcow2 fails with "Cannot grow device files"), so the controller first converts the disk to a raw COPY on the source node via a transient, stopped, diskless helper VM (`migrator.helperVMID`, default `9998`) and the `move_disk` API (file-to-file `qemu-img`, unaffected by the streaming bug), then moves the raw copy to the target with the standard copy endpoint. The volume arrives as `….raw` and the PV handle is rewired accordingly; data and filesystem are unchanged, qcow2 thin/snapshot features are lost. Safety by construction: the helper's VM ID differs from the volume's owner VM ID, and Proxmox only ever frees volumes OWNED by a destroyed VM — so helper cleanup (success, failure, or crash recovery) can only remove the helper's own disposable conversion copy, never the original volume.
- **raw/LVM/LVM-thin volumes** migrate through the copy endpoint with unchanged names.

### Crash recovery

Before moving a disk, the controller stamps `csi.proxmox.sinextra.dev/migrate-state` on the PV. If the controller crashes between the disk move and the PV/PVC rewire, the next reconcile detects the disk already on the target node and resumes at the rewire step instead of moving again. Nodes cordoned by a force migration are always uncordoned, on every exit path.

## Deployment (Helm)

```yaml
# values.yaml
migrator:
  enabled: true
  config:
    clusters:
      - url: https://cluster-api-1.example.com:8006/api2/json
        insecure: false
        username: root@pam
        password: "strong-password"
        region: cluster-1
```

```shell
helm upgrade -i --namespace=csi-proxmox -f values.yaml \
    proxmox-csi-plugin oci://ghcr.io/sergelogvinov/charts/proxmox-csi-plugin
```

This deploys:

- a `…-migrator` Deployment running `pvecsictl controller` with leader election
- a dedicated ServiceAccount, ClusterRole (pods delete, PV/PVC recreate, node cordon, events), and a namespaced Role for leases
- a `…-migrator` Secret holding the root-credential cloud config (or use `migrator.existingConfigSecret`)

### Scheduled rebalancing

```yaml
migrator:
  enabled: true
  rebalance:
    enabled: true
    schedule: "0 3 * * *"
    highThreshold: 0.80   # move volumes off zones above 80% used
    lowThreshold: 0.60    # only onto zones below 60% used
    maxMigrations: 2      # per run
    window: "22:00-04:00" # optional maintenance window
    windowTz: "UTC"
```

The CronJob runs `pvecsictl rebalance`, which **only plans idle volumes** (never disrupts running pods) and stamps migration annotations for the controller to execute. `concurrencyPolicy: Forbid` prevents overlapping runs.

## Controller flags

| Flag | Default | Description |
|---|---|---|
| `--config` | (required) | Proxmox cloud config with root credentials |
| `--leader-election` | `true` | Use a `pvecsictl-migrator` Lease |
| `--pod-follow` | `false` | Migrate volumes automatically when all their pods moved to another zone |
| `--namespace` | `POD_NAMESPACE` or `kube-system` | Lease namespace |
| `--max-attempts` | `5` | Attempts before a migration is marked `Failed` |
| `--timeout` | `10800` | Proxmox move-task timeout (seconds) |
| `--drain-timeout` | `10m` | Max wait for pods to terminate during force-drain |
| `--detach-timeout` | `5m` | Max wait for the disk to detach from a VM |
| `--metrics-address` | disabled | Prometheus metrics address (e.g. `:8081`) |

## Failure recovery runbook

| Symptom | Action |
|---|---|
| PVC stuck in phase `Failed` | Read `migrate-message` and the PVC events; fix the cause; remove `migrate-phase`/`migrate-attempts` annotations (or re-annotate `migrate-node`) to retry |
| Nodes left cordoned | Should not happen (deferred uncordon); if the controller host died mid-run: `kubectl uncordon <node>` |
| Disk moved but PVC unchanged | The PV carries `migrate-state`; the next reconcile resumes the rewire automatically |
| Invalid target zone | The controller validates the target hosts the volume's storage before doing anything, and marks the request `Failed` with an explanatory event |

## Manual verification (integration)

1. `kubectl -n default annotate pvc storage-test-0 csi.proxmox.sinextra.dev/migrate-node=hvm-2`
2. Watch `kubectl -n default get pvc storage-test-0 -o jsonpath='{.metadata.annotations}'` — phase progresses `Pending → Moving → Rewiring → Completed`
3. Verify the PV: `kubectl get pv <pv> -o jsonpath='{.spec.nodeAffinity}'` shows the new zone
4. Kill the controller pod during `Moving`; confirm the migration resumes after restart
5. `kubectl annotate node <node> csi.proxmox.sinextra.dev/evacuate=auto`; confirm all PVCs in that zone get `migrate-node` annotations and migrate one at a time
