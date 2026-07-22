# pvecsictl tool

`pvecsictl` is a command line tool for managing the Proxmox CSI PV/PVC resources.

**Warning**: This tool is under development and should be used with caution.
The commands and flags may change in the future.

## Installation

It works on macOS (Intel/ARM) and Linux (amd64/arm64)

```shell
brew install sergelogvinov/tap/pvecsictl
```

RBAC permissions required for the tool

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: pvecsictl
rules:
  # Get list of pods with PVCs
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch", "delete"]
  - apiGroups: ["storage.k8s.io"]
    resources: ["csinodes"]
    verbs: ["get", "list"]
  # Create and delete PV/PVC
  - apiGroups: [""]
    resources: ["persistentvolumes"]
    verbs: ["get", "list", "watch", "create", "patch", "delete"]
  - apiGroups: [""]
    resources: ["persistentvolumeclaims"]
    verbs: ["get", "list", "watch", "create", "patch", "delete"]
  # Node cordoning/uncordoning
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "watch", "patch"]
  # Migration progress events (controller/evacuate/rebalance)
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create", "patch"]
```

The `controller` subcommand additionally needs `leases` (get/watch/list/create/update/delete) in its own namespace for leader election.

## Usage

```shell
Usage:
  pvecsictl [command]

Available Commands:
  controller  Run the annotation-driven volume migration controller
  evacuate    Evacuate all CSI volumes from a Proxmox node
  migrate     Migrate data from one Proxmox node to another
  rebalance   Rebalance idle CSI volumes from overloaded Proxmox nodes to emptier ones
  rename      Rename PersistentVolumeClaim
  swap        Swap PersistentVolumes between two PersistentVolumeClaims
```

## Commands

### Migrate

Migration requires root privileges on the Proxmox cluster by default, because the
built-in disk-copy endpoint is root-only. Provide the cloud-config with root
credentials (username/password):

```yaml
clusters:
  - url: https://cluster-1:8006/api2/json
    username: root@pam
    password: "strong-password"
    region: fsn1
    ...
```

Alternatively, install the [`pve-csi-copy`](../hack/pve-token-copy/) package on the
Proxmox nodes and pass `--token-copy-endpoint`; the copy then runs through a
permission-gated endpoint and a scoped API token is sufficient (no `root@pam`):

```yaml
clusters:
  - url: https://cluster-1:8006/api2/json
    token_id: "kubernetes-csi@pve!csi"
    token_secret: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    region: fsn1
    ...
```

To migrate the data for PVC `storage-test-0` first find the backing PV by running

```shell
kubectl -n default get pvc storage-test-0 -ojsonpath='{.spec.volumeName}'
```

which in our case is `pvc-0d79713b-6d0b-41e5-b387-42af370d083f`.

Next find the PV topology by inspecting its `nodeAffinity`

```shell
kubectl -n default get pv pvc-0d79713b-6d0b-41e5-b387-42af370d083f -ojsonpath='{.spec.nodeAffinity}'
```

which gives us

```json
{"required":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"topology.kubernetes.io/region","operator":"In","values":["fsn1"]},{"key":"topology.kubernetes.io/zone","operator":"In","values":["hvm-1"]}]}]}}
```

By looking at the above `topology.kubernetes.io` fields we see that the PV is located in zone (node) `hvm-1` in region (cluster) `fsn1`.

To move the PVC from zone `hvm-1` to `hvm-2` we can run

```shell
pvecsictl migrate --config=hack/cloud-config.yaml -n default storage-test-0 hvm-2
````

If the target node does not have a storage with the same name, move the disk into a different storage with `--storage`:

```shell
pvecsictl migrate --config=hack/cloud-config.yaml -n default storage-test-0 hvm-2 --storage local-zfs
```

`--storage` also works with the volume's **current** node as the target: the disk is moved to the other storage on the same node (the zone does not change, only the storage in the volume handle). Without `--storage`, or with the current storage name, a same-node request is a no-op and is rejected as "already on target".

If you're met with

```shell
ERROR Error: persistentvolumeclaims is using by pods: test-0 on node kube-store-11, cannot move volume
```

you can force the process by adding the `--force` flag.

Forced migration needs the Proxmox VMID of the node running the pods. The
node's `proxmox://<region>/<vmid>` providerID (set by the Proxmox CCM) or the
`proxmox.sinextra.dev/instance-id` annotation is used when present; otherwise
the migrator finds the VM through the Proxmox API — by node-name prefix
verified against the node's SMBIOS system UUID, then by system UUID alone —
the same lookup the CSI controller uses for attaching volumes, so clusters
without the Proxmox CCM work out of the box. Use the annotation as an explicit
override if the lookup cannot verify a VM (e.g. the node reports no system
UUID).

```shell
pvecsictl migrate --config=hack/cloud-config.yaml -n default storage-test-0 hvm-2 --force

INFO persistentvolumeclaims is using by pods: test-0 on node kube-store-11, trying to force migration
INFO cordoning nodes: kube-11,kube-12,kube-21,kube-22,kube-store-11,kube-store-21
INFO terminated pods: test-0
INFO waiting pods: test-0
...
INFO waiting pods: test-0
INFO moving disk vm-9999-pvc-0d79713b-6d0b-41e5-b387-42af370d083f to proxmox node hvm-2
INFO replacing persistentvolume topology
INFO uncordoning nodes: kube-11,kube-12,kube-21,kube-22,kube-store-11,kube-store-21
INFO persistentvolumeclaims storage-test-0 has been migrated to proxmox node hvm-2
```

To check that the zone has changed run
```shell
kubectl -n default get pv pvc-0d79713b-6d0b-41e5-b387-42af370d083f -ojsonpath='{.spec.nodeAffinity}'
```

again

```json
{"required":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"topology.kubernetes.io/region","operator":"In","values":["fsn1"]},{"key":"topology.kubernetes.io/zone","operator":"In","values":["hvm-2"]}]}]}}
```

to verify that the zone is now `hvm-2`.

Pod lifetime when running `pvecsictl` with the `--force` flag

```shell
# kubectl -n default get pods -owide -w
test-0   1/1     Running            0          4m28s   10.32.19.119   kube-store-11   <none>           <none>
test-0   1/1     Terminating        0          6m44s   10.32.19.119   kube-store-11   <none>           <none>
test-0   0/1     Terminating        0          7m      <none>         kube-store-11   <none>           <none>
test-0   0/1     Terminating        0          7m      10.32.19.119   kube-store-11   <none>           <none>
test-0   0/1     Terminating        0          7m      10.32.19.119   kube-store-11   <none>           <none>
test-0   0/1     Terminating        0          7m      10.32.19.119   kube-store-11   <none>           <none>
test-0   0/1     Pending            0          0s      <none>         <none>          <none>           <none>
test-0   0/1     Pending            0          0s      <none>         <none>          <none>           <none>
test-0   0/1     Pending            0          62s     <none>         <none>          <none>           <none>
test-0   0/1     Pending            0          71s     <none>         kube-21         <none>           <none>
test-0   0/1     ContainerCreating  0          71s     <none>         kube-21         <none>           <none>
test-0   1/1     Running            0          85s     10.32.11.96    kube-21         <none>           <none>
```

Here we've migrated the StatefulSet Pod with PVC to another node.
Force mode helps to migrate StatefulSet deployment to another node without scaling down all replicas.
It cordoned all nodes which have csi-proxmox plugin. Migrated the disk to another node and un-cordoned all nodes.

> The rewire step repoints the PV/PVC at the migrated disk using a `claimRef` pre-bind (the [Reserving a PersistentVolume](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#reserving-a-persistentvolume) pattern): the new PV is reserved for the PVC before the old PVC is deleted, and the old PV is forced to `Retain` first. This makes migration safe for GitOps/controller-managed PVCs (Argo CD `selfHeal`, StatefulSet, operators) — a controller that recreates the deleted PVC rebinds to the migrated disk instead of provisioning an empty one, so **no controller pause is needed** as long as the manifest does not pin `spec.volumeName`. Once the migrated PV is verified and bound, `migrate` **reclaims the source disk copy automatically** (the move is a copy and the `Retain` guard stops the provisioner from doing so, for both cross-node and same-node cross-storage moves) — no manual `pvesm free` is needed on success; a reclaim failure is warned about without failing the migration.

### Rename

Rename PersistentVolumeClaim.

Check the current PVCs:

```shell
# kubectl -n default get pvc
storage-test-0   Bound    pvc-0d79713b-6d0b-41e5-b387-42af370d083f   5Gi        RWO            proxmox-xfs    <unset>                 7m6s
storage-test-1   Bound    pvc-2727795f-680c-410a-b130-2e5dc85efcb3   5Gi        RWO            proxmox-xfs    <unset>                 15m
```

Rename the `storage-test-0` PVC by running

```shell
pvecsictl rename -n default storage-test-0 storage-test-2
```

If you're met with

```shell
ERROR Error: persistentvolumeclaims is using by pods: test-0 on node kube-21, cannot move volume
```

You can force the process by adding the `--force` flag

```shell
pvecsictl rename -n default storage-test-0 storage-test-2 --force

INFO persistentvolumeclaims is using by pods: test-0 on node kube-21, trying to force migration
INFO cordoning nodes: kube-11,kube-12,kube-21,kube-22,kube-store-11,kube-store-21
INFO terminated pods: test-0
INFO waiting pods: test-0
...
INFO waiting pods: test-0
INFO uncordoning nodes: kube-11,kube-12,kube-21,kube-22,kube-store-11,kube-store-21
INFO persistentvolumeclaims storage-test-0 has been renamed
```

Check the result:

* storage-test-0 -> storage-test-2
* storage-test-0 has new PV

```shell
# kubectl -n default get pvc
storage-test-0   Bound    pvc-2c6a06b2-e693-4807-a872-63ca67e6ee52   5Gi        RWO            proxmox-xfs    <unset>                 100s
storage-test-1   Bound    pvc-2727795f-680c-410a-b130-2e5dc85efcb3   5Gi        RWO            proxmox-xfs    <unset>                 19m
storage-test-2   Bound    pvc-0d79713b-6d0b-41e5-b387-42af370d083f   5Gi        RWO            proxmox-xfs    <unset>                 102s
```

Pod lifetime during rename

```shell
# kubectl -n default get pods -owide -w
test-0   1/1     Running             0          9m37s   10.32.11.96   kube-21         <none>           <none>
test-1   1/1     Running             0          16m     10.32.4.232   kube-store-21   <none>           <none>
test-0   1/1     Terminating         0          10m     10.32.11.96   kube-21         <none>           <none>
test-0   0/1     Terminating         0          11m     <none>        kube-21         <none>           <none>
test-0   0/1     Terminating         0          11m     10.32.11.96   kube-21         <none>           <none>
test-0   0/1     Terminating         0          11m     10.32.11.96   kube-21         <none>           <none>
test-0   0/1     Terminating         0          11m     10.32.11.96   kube-21         <none>           <none>
test-0   0/1     Pending             0          0s      <none>        <none>          <none>           <none>
test-0   0/1     Pending             0          0s      <none>        <none>          <none>           <none>
test-0   0/1     Pending             0          8s      <none>        <none>          <none>           <none>
test-0   0/1     Pending             0          13s     <none>        kube-store-11   <none>           <none>
test-0   0/1     ContainerCreating   0          13s     <none>        kube-store-11   <none>           <none>
test-0   1/1     Running             0          24s     10.32.19.17   kube-store-11   <none>           <none>
```

### Swap

Swap PersistentVolumeClaim between two PVCs.

Check the current PVC:

```shell
# kubectl get pods,pvc
NAME         READY   STATUS    RESTARTS   AGE
pod/test-0   1/1     Running   0          2m58s
pod/test-1   1/1     Running   0          2m58s

NAME                                   STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   VOLUMEATTRIBUTESCLASS   AGE
persistentvolumeclaim/storage-test-0   Bound    pvc-e248bc56-dcf4-4145-93b9-a374a7c3b900   10Gi       RWO            proxmox-lvm    <unset>                 2m51s
persistentvolumeclaim/storage-test-1   Bound    pvc-41b7078d-aa9f-4757-9056-8bd1e8e0697f   15Gi       RWO            proxmox-lvm    <unset>                 2m52s
```

Swap PVCs:

```shell
pvecsictl swap -n default storage-test-0 storage-test-1 -f

INFO persistentvolumeclaims is using by pods: test-0 on node builder-03a, trying to force swap
INFO persistentvolumeclaims is using by pods: test-1 on node builder-04b, trying to force swap
INFO cordoned nodes: builder-03a,builder-03b,builder-04a,builder-04b
INFO terminated pods: test-0,test-1
INFO waiting pods: test-0
INFO waiting pods: test-0
INFO persistentvolumeclaims storage-test-0,storage-test-1 has been swapped
INFO uncordoning nodes: builder-03a,builder-03b,builder-04a,builder-04b
```

Check the result:

* storage-test-0 <-> storage-test-1

```shell
# kubectl get pods,pvc
NAME         READY   STATUS    RESTARTS   AGE
pod/test-0   1/1     Running   0          19s
pod/test-1   1/1     Running   0          19s

NAME                                   STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   VOLUMEATTRIBUTESCLASS   AGE
persistentvolumeclaim/storage-test-0   Bound    pvc-41b7078d-aa9f-4757-9056-8bd1e8e0697f   15Gi       RWO            proxmox-lvm    <unset>                 13s
persistentvolumeclaim/storage-test-1   Bound    pvc-e248bc56-dcf4-4145-93b9-a374a7c3b900   10Gi       RWO            proxmox-lvm    <unset>                 13s
```

### Evacuate

Evacuate all CSI volumes from a Proxmox node (zone), e.g. before node maintenance.

By default, evacuate stamps `csi.proxmox.sinextra.dev/migrate-node` annotations on the affected PVCs and lets the [migration controller](migration-controller.md) execute them one at a time. With `--now` it runs the migrations synchronously (requires root credentials in the config, like `migrate`).

```shell
# Plan only: show which volume goes where (targets picked by free capacity)
pvecsictl evacuate --config=hack/cloud-config.yaml hvm-1 --dry-run

# Request evacuation; the migration controller does the work
pvecsictl evacuate --config=hack/cloud-config.yaml hvm-1

# Force-migrate in-use volumes to an explicit target, synchronously
pvecsictl evacuate --config=hack/cloud-config.yaml hvm-1 --target hvm-2 --force --now
```

Alternatively, annotate the Kubernetes node directly (no CLI needed):

```shell
kubectl annotate node kube-store-11 csi.proxmox.sinextra.dev/evacuate=auto
```

### Rebalance

Move idle volumes (not used by any pod) from Proxmox nodes above `--high-threshold` storage usage to the emptiest node below `--low-threshold`. Designed to run unattended from a CronJob (see the Helm chart's `migrator.rebalance` values); requires the migration controller to execute the planned moves.

```shell
pvecsictl rebalance --config=hack/cloud-config.yaml --dry-run

pvecsictl rebalance --config=hack/cloud-config.yaml \
    --high-threshold=0.8 --low-threshold=0.6 --max-migrations=2 --window "22:00-04:00"
```

Rebalance never disrupts running pods: in-use volumes are skipped.

### Controller

Run the annotation-driven migration controller in-cluster. See [migration-controller.md](migration-controller.md) for the annotation protocol, Helm deployment, and the failure-recovery runbook.

```shell
pvecsictl controller --config=/etc/proxmox/config.yaml --leader-election
```

# Feedback

Use the [GitHub discussions](https://github.com/sergelogvinov/proxmox-csi-plugin/discussions) for feedback and questions.
