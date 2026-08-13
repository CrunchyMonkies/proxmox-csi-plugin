# Install plugin

This plugin allows Kubernetes to use `Proxmox VE` storage as a persistent storage solution for stateful applications.
Supported storage types:
- Directory
- LVM
- LVM-thin
- ZFS
- NFS
- Ceph

## Proxmox configuration

Proxmox CSI Plugin requires the correct privileges in order to allocate and attach disks.

Create `CSI` role in Proxmox:

```shell
pveum role add CSI -privs "VM.Audit VM.Config.Disk Datastore.Allocate Datastore.AllocateSpace Datastore.Audit"
# Or if you need to use Replication feature (zfs replication)
pveum role add CSI -privs "VM.Audit VM.Allocate VM.Clone VM.Config.CPU VM.Config.Disk VM.Config.HWType VM.Config.Memory VM.Config.Options VM.Migrate VM.PowerMgmt Datastore.Allocate Datastore.AllocateSpace Datastore.Audit"
```

Next create a user `kubernetes-csi@pve` for the CSI plugin and grant it the above role

```shell
pveum user add kubernetes-csi@pve
pveum aclmod / -user kubernetes-csi@pve -role CSI
pveum user token add kubernetes-csi@pve csi -privsep 0
```

The optional [volume ownership reassignment](#enable-volume-ownership-reassignment-fork) feature needs
no extra privileges: its `rename` call enforces the same ACL set as `move_disk` — `Datastore.Audit` and
`Datastore.AllocateSpace` on `/storage/<storage>`, `VM.Config.Disk` on `/vms/<target-vmid>`, plus a
`check_volume_access` on the source volume — all of which the `CSI` role above already grants at `/`.
Note that under privilege separation (`pveum user token add … -privsep 1`) each privilege must be granted
to **both** the user and the token, since a privsep token gets the intersection of the two; the
`-privsep 0` token created above sidesteps this.

Or through terraform:

```hcl
# Plugin: bpg/proxmox

resource "proxmox_virtual_environment_role" "csi" {
  role_id = "Kubernetes-CSI"

  privileges = [
    "VM.Audit",
    "VM.Config.Disk",
    "Datastore.Allocate",
    "Datastore.AllocateSpace",
    "Datastore.Audit",
  ]
}

resource "proxmox_virtual_environment_user" "kubernetes" {
  acl {
    path      = "/"
    propagate = true
    role_id   = proxmox_virtual_environment_role.csi.role_id
  }

  comment = "Kubernetes"
  user_id = "kubernetes-csi@pve"
}

resource "proxmox_virtual_environment_user_token" "csi" {
  comment    = "Kubernetes CSI"
  token_name = "csi"
  user_id    = proxmox_virtual_environment_user.kubernetes.user_id
}

resource "proxmox_virtual_environment_acl" "csi" {
  token_id = proxmox_virtual_environment_user_token.csi.id
  role_id  = proxmox_virtual_environment_role.csi.role_id

  path      = "/"
  propagate = true
}
```

All VMs in the cluster must have the `SCSI Controller` set to `VirtIO SCSI single` or `VirtIO SCSI` type to be able to attach disks.

## Prepare Kubernetes cluster

Proxmox CSI Plugin relies on the well-known Kubernetes topology node labels to define the disk location.
* `topology.kubernetes.io/region` - Cluster name, the name must be the same as in cloud config region name
* `topology.kubernetes.io/zone` - Proxmox node name


```shell
kubectl label nodes region1-node-1 topology.kubernetes.io/region=Region1
kubectl label nodes region1-node-1 topology.kubernetes.io/zone=pve-1
```
> Note: All nodes provisioned by Proxmox CSI Plugin should be labeled.


Alternatively, you can use [Proxmox Cloud Controller Manager](https://github.com/sergelogvinov/proxmox-cloud-controller-manager). Proxmox CCM will manage topology labels for you.


## Install CSI Driver

Create a namespace `csi-proxmox` for the plugin and grant it the `privileged` permissions

```shell
kubectl create ns csi-proxmox
kubectl label ns csi-proxmox pod-security.kubernetes.io/enforce=privileged
```

All examples below assume that plugin controller runs on control-plane. Change the `nodeSelector` to match your environment if needed.

```yaml
nodeSelector:
  node-role.kubernetes.io/control-plane: ""
tolerations:
  - key: node-role.kubernetes.io/control-plane
    effect: NoSchedule
```

### Install the plugin by using kubectl

Create a Proxmox cloud config to connect to your cluster with the Proxmox user you just created.
More information about the configuration can be found in [Plugin configuration file](config.md).

```yaml
# config.yaml
clusters:
  # List of Proxmox clusters

  - url: https://cluster-api-1.exmple.com:8006/api2/json
    # Skip the certificate verification, if needed
    insecure: false
    # Proxmox api token
    token_id: "kubernetes-csi@pve!csi"
    token_secret: "secret"
    # Region name, which is cluster name
    region: Region-1

  # Add more clusters if needed
  - url: https://cluster-api-2.exmple.com:8006/api2/json
    insecure: false
    token_id: "kubernetes-csi@pve!csi"
    token_secret: "secret"
    region: Region-2
```

Upload the configuration to the Kubernetes as a secret

```shell
kubectl -n csi-proxmox create secret generic proxmox-csi-plugin --from-file=config.yaml
```

Install latest release version

```shell
kubectl apply -f https://raw.githubusercontent.com/CrunchyMonkies/proxmox-csi-plugin/main/docs/deploy/proxmox-csi-plugin-release.yml
```

Or install latest stable version (edge)

```shell
kubectl apply -f https://raw.githubusercontent.com/CrunchyMonkies/proxmox-csi-plugin/main/docs/deploy/proxmox-csi-plugin.yml
```

### Install the plugin by using Helm

Create the helm values file, for more information see [values.yaml](../charts/proxmox-csi-plugin/values.yaml)

```yaml
# proxmox-csi.yaml
config:
  clusters:
    - url: https://cluster-api-1.exmple.com:8006/api2/json
      insecure: false
      token_id: "kubernetes-csi@pve!csi"
      token_secret: "secret"
      region: Region-1
    # Add more clusters if needed
    - url: https://cluster-api-2.exmple.com:8006/api2/json
      insecure: false
      token_id: "kubernetes-csi@pve!csi"
      token_secret: "secret"
      region: Region-2

# Define the storage classes
storageClass:
  - name: proxmox-data-xfs
    storage: data
    reclaimPolicy: Delete
    fstype: xfs
    # Define the storage class as default
    annotations:
      storageclass.kubernetes.io/is-default-class: "true"
```

Install the plugin. You need to prepare the `csi-proxmox` namespace first, see above

```shell
helm upgrade -i -n csi-proxmox -f proxmox-csi.yaml proxmox-csi-plugin oci://ghcr.io/crunchymonkies/charts/proxmox-csi-plugin
```

#### Option for k0s

If you're running [k0s](https://k0sproject.io/) you need to add extra value to the helm chart

```yaml
kubeletDir: /var/lib/k0s/kubelet
```

#### Option for microk8s

If you're running [microk8s](https://microk8s.io/) you need to add extra value to the helm chart

```yaml
kubeletDir: /var/snap/microk8s/common/var/lib/kubelet
```

#### Enable the Volume Migration Controller (fork)

The migration controller performs online cross-node disk moves. By default it authenticates as
**root@pam** — Proxmox' built-in disk-copy endpoint has no permission check and is therefore
root-only. A **scoped API token** works instead once the Proxmox nodes carry a permission-gated
copy endpoint, enabled per cluster with `proxmod_endpoint: true` (recommended) or
`token_copy_endpoint: true`; see [Prerequisites](migration-controller.md#prerequisites) for the
privileges such a token still needs. Either way the credentials live in a **separate** secret from
the CSI controller config. Add the following to your helm values:

```yaml
migrator:
  enabled: true
  config:
    clusters:
      - url: https://cluster-api-1.exmple.com:8006/api2/json
        insecure: false
        username: "root@pam"
        password: "super-secret"
        region: Region-1
        # Preferred storage per Proxmox node, used when the source storage
        # name does not exist on the target node (pod-follow / evacuation):
        primary_storage:
          pve-1: local-zfs
          pve-2: local-zfs
  # Transient helper VM used to convert qcow2/vmdk volumes to raw before moving.
  # Must be a free VMID and differ from the controller VMID (default 9999).
  helperVMID: 9998
  # Optional scheduled rebalance CronJob:
  rebalance:
    enabled: false
    schedule: "0 3 * * *"
```

See the [Volume Migration Controller — Operator Guide](migration-controller.md) for the full annotation
workflow (`csi.proxmox.sinextra.dev/migrate-node`, `evacuate`, rebalance, pod-follow).

#### Enable volume ownership reassignment (fork)

CSI volumes are created by, and named for, the controller's placeholder VM ID (`controllerVMID`,
default `9999`), so Proxmox-side tooling that keys off `vmid` — backup jobs, accounting — attributes
every CSI disk to a VM that does not exist. With this feature enabled the disk is renamed to the VM
it is attached to (`local:3021/vm-3021-pvc-<uuid>.raw`) for as long as a pod is using it, and renamed
back to the placeholder on detach. The PV's `volumeHandle` is never rewritten.

It requires [proxmod](https://github.com/CrunchyMonkies/proxmod) and the
[`proxmox-csi-storage`](../hack/proxmod-csi-storage/) extension **>= 0.3.0** on **every** Proxmox
node — the rename has no REST endpoint of its own in PVE. Build and install steps are in
[hack/proxmod-csi-storage/README.md](../hack/proxmod-csi-storage/README.md), and a rollback-tested
install procedure in [SMOKE-TEST.md](../hack/proxmod-csi-storage/SMOKE-TEST.md). Installing proxmod
rewrites the `pvedaemon`/`pveproxy` unit and restarts both, so expect a brief API interruption on
each node as you roll it out.

Both keys are required — the feature flag alone is a no-op, and the controller logs a warning at
startup if it is set while no cluster enables the endpoint:

```yaml
config:
  features:
    reassignVolumeOnAttach: true
  clusters:
    - url: https://cluster-api-1.exmple.com:8006/api2/json
      token_id: "kubernetes-csi@pve!csi"
      token_secret: "secret"
      region: Region-1
      proxmod_endpoint: true
```

If you install with `existingConfigSecret`, the chart does not template the config — edit the secret
itself instead.

Two things to know before enabling it:

- While a volume is attached the VM genuinely owns it, so `qm destroy <vmid>` destroys the CSI volumes
  attached to that VM along with it. Drain the node first.
- Volumes that were already attached when you enabled the flag keep their `vm-9999-…` name until their
  next detach/attach cycle.

See [Reassign volume ownership on attach](reassign-volume-on-attach.md) for the mechanism, the
`unused<n>` ordering rule, and recovery behaviour.

### Install the plugin by using Talos machine config

If you're running [Talos](https://www.talos.dev/) you can install Proxmox CSI plugin using the machine config

```yaml
cluster:
  externalCloudProvider:
    enabled: true
    manifests:
      - https://raw.githubusercontent.com/CrunchyMonkies/proxmox-csi-plugin/main/docs/deploy/proxmox-csi-plugin.yml
```

Or all together with the Proxmox Cloud Controller Manager

* Proxmox CCM will label the nodes
* Proxmox CSI will use the labeled nodes to define the regions and zones

```yaml
cluster:
  inlineManifests:
    - name: proxmox-cloud-controller-manager
      contents: |-
        apiVersion: v1
        kind: Secret
        type: Opaque
        metadata:
          name: proxmox-cloud-controller-manager
          namespace: kube-system
        stringData:
          config.yaml: |
            clusters:
              - url: https://cluster-api-1.exmple.com:8006/api2/json
                insecure: false
                token_id: "kubernetes@pve!ccm"
                token_secret: "secret"
                region: Region-1
    - name: proxmox-csi-plugin
      contents: |-
        apiVersion: v1
        kind: Secret
        type: Opaque
        metadata:
          name: proxmox-csi-plugin
          namespace: csi-proxmox
        stringData:
          config.yaml: |
            clusters:
              - url: https://cluster-api-1.exmple.com:8006/api2/json
                insecure: false
                token_id: "kubernetes-csi@pve!csi"
                token_secret: "secret"
                region: Region-1
  externalCloudProvider:
    enabled: true
    manifests:
      - https://raw.githubusercontent.com/sergelogvinov/proxmox-cloud-controller-manager/main/docs/deploy/cloud-controller-manager.yml
      - https://raw.githubusercontent.com/CrunchyMonkies/proxmox-csi-plugin/main/docs/deploy/proxmox-csi-plugin.yml
```
