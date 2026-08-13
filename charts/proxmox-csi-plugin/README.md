# proxmox-csi-plugin

![Version: 0.12.0](https://img.shields.io/badge/Version-0.12.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: v0.20.0-1.4.0](https://img.shields.io/badge/AppVersion-v0.20.0--1.4.0-informational?style=flat-square)

Container Storage Interface plugin for Proxmox

The Container Storage Interface (CSI) plugin is a specification designed to standardize the way container orchestration systems like Kubernetes, interact with different storage systems. The CSI plugin abstracts the underlying storage, enabling the seamless integration of different storage solutions (such as local block devices, file systems, or cloud-based storage) with containerized applications.

This plugin allows Kubernetes to use `Proxmox VE` storage as a persistent storage solution for stateful applications.
Supported storage types:
- Directory
- LVM
- LVM-thin
- ZFS
- NFS
- Ceph

**Homepage:** <https://github.com/CrunchyMonkies/proxmox-csi-plugin>

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| CrunchyMonkies |  | <https://github.com/CrunchyMonkies> |

## Source Code

* <https://github.com/CrunchyMonkies/proxmox-csi-plugin>

## Proxmox permissions

```shell
# Create role CSI
pveum role add CSI -privs "VM.Audit VM.Config.Disk Datastore.Allocate Datastore.AllocateSpace Datastore.Audit"
# Or if you need to use Replication feature (zfs replication)
pveum role add CSI -privs "VM.Audit VM.Allocate VM.Clone VM.Config.CPU VM.Config.Disk VM.Config.HWType VM.Config.Memory VM.Config.Options VM.Migrate VM.PowerMgmt Datastore.Allocate Datastore.AllocateSpace Datastore.Audit"

# Create user and grant permissions
pveum user add kubernetes-csi@pve
pveum aclmod / -user kubernetes-csi@pve -role CSI
pveum user token add kubernetes-csi@pve csi -privsep 0
```

## Helm values example

```yaml
# proxmox-csi.yaml

config:
  clusters:
    - url: https://cluster-api-1.exmple.com:8006/api2/json
      insecure: false
      token_id: "kubernetes-csi@pve!csi"
      token_secret: "key"
      region: cluster-1

# Deploy Node CSI driver only on proxmox nodes
node:
  nodeSelector:
    # It will work only with Talos CCM, remove it overwise
    node.cloudprovider.kubernetes.io/platform: nocloud
  tolerations:
    - operator: Exists

# Deploy CSI controller only on control-plane nodes
nodeSelector:
  node-role.kubernetes.io/control-plane: ""
tolerations:
  - key: node-role.kubernetes.io/control-plane
    effect: NoSchedule

# Define storage classes
# See https://pve.proxmox.com/wiki/Storage
storageClass:
  - name: proxmox-data-xfs
    storage: data
    reclaimPolicy: Delete
    fstype: xfs
  - name: proxmox-data
    storage: data
    reclaimPolicy: Delete
    fstype: ext4
    cache: writethrough
```

## Deploy

```shell
# Prepare namespace
kubectl create ns csi-proxmox
kubectl label ns csi-proxmox pod-security.kubernetes.io/enforce=privileged
# Install Proxmox CSI plugin
helm upgrade -i --namespace=csi-proxmox -f proxmox-csi.yaml \
    proxmox-csi-plugin oci://ghcr.io/crunchymonkies/charts/proxmox-csi-plugin
```

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| replicaCount | int | `1` |  |
| imagePullSecrets | list | `[]` |  |
| nameOverride | string | `""` |  |
| fullnameOverride | string | `""` |  |
| createNamespace | bool | `false` | Create namespace. Very useful when using helm template. |
| priorityClassName | string | `"system-cluster-critical"` | Controller pods priorityClassName. |
| serviceAccount | object | `{"annotations":{},"create":true,"name":""}` | Pods Service Account. ref: https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/ |
| provisionerName | string | `"csi.proxmox.sinextra.dev"` | CSI Driver provisioner name. Currently, cannot be customized. |
| csidriver.fsGroupPolicy | string | `"None"` | fsGroupPolicy controls whether kubelet recursively applies the pod's fsGroup to the volume. `None` disables it (volumes keep their own permissions); use the `rootDirPermissions` StorageClass parameter to make fresh volumes writable by non-root pods. `ReadWriteOnceWithFSType` (the previous default) breaks workloads such as Postgres that reject group-writable data directories. Note: this field is immutable on an existing CSIDriver; changing it requires recreating the object. |
| clusterID | string | `"kubernetes"` | Cluster name. Currently, cannot be customized. |
| logVerbosityLevel | int | `5` | Log verbosity level. See https://github.com/kubernetes/community/blob/master/contributors/devel/sig-instrumentation/logging.md for description of individual verbosity levels. |
| timeout | string | `"3m"` | Connection timeout between sidecars. |
| options.enableCapacity | bool | `true` | Enable or disable capacity feature. ref: https://github.com/kubernetes-csi/external-provisioner |
| existingConfigSecret | string | `nil` | Proxmox cluster config stored in secrets. |
| existingConfigSecretKey | string | `"config.yaml"` | Proxmox cluster config stored in secrets key. |
| configFile | string | `"/etc/proxmox/config.yaml"` | Proxmox cluster config path. |
| config | object | `{"clusters":[],"features":{"provider":"default"}}` | Proxmox cluster config. ref: https://github.com/CrunchyMonkies/proxmox-csi-plugin/blob/main/docs/install.md |
| storageClass | list | `[]` | Storage class definition. |
| controller.podAnnotations | object | `{}` | Annotations for controller pod. ref: https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/ |
| controller.podLabels | object | `{}` | Labels for controller pod. ref: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/ |
| controller.plugin.image | object | `{"pullPolicy":"IfNotPresent","repository":"ghcr.io/crunchymonkies/proxmox-csi-controller","tag":""}` | Controller CSI Driver. |
| controller.plugin.resources | object | `{"requests":{"cpu":"10m","memory":"16Mi"}}` | Controller resource requests and limits. ref: https://kubernetes.io/docs/user-guide/compute-resources/ |
| controller.attacher.image | object | `{"pullPolicy":"IfNotPresent","repository":"registry.k8s.io/sig-storage/csi-attacher","tag":"v4.10.0"}` | CSI Attacher. ref: https://github.com/kubernetes-csi/external-attacher |
| controller.attacher.args | list | `["--default-fstype=ext4"]` | Attacher arguments. example: --default-fstype=ext4 |
| controller.attacher.resources | object | `{"requests":{"cpu":"10m","memory":"16Mi"}}` | Attacher resource requests and limits. ref: https://kubernetes.io/docs/user-guide/compute-resources/ |
| controller.provisioner.image | object | `{"pullPolicy":"IfNotPresent","repository":"registry.k8s.io/sig-storage/csi-provisioner","tag":"v5.3.0"}` | CSI Provisioner. ref: https://github.com/kubernetes-csi/external-provisioner |
| controller.provisioner.args | list | `["--default-fstype=ext4"]` | Provisioner arguments. example: --feature-gates=VolumeAttributesClass=true |
| controller.provisioner.resources | object | `{"requests":{"cpu":"10m","memory":"16Mi"}}` | Provisioner resource requests and limits. ref: https://kubernetes.io/docs/user-guide/compute-resources/ |
| controller.resizer.image | object | `{"pullPolicy":"IfNotPresent","repository":"registry.k8s.io/sig-storage/csi-resizer","tag":"v1.14.0"}` | CSI Resizer. refs: https://github.com/kubernetes-csi/external-resizer |
| controller.resizer.args | list | `[]` | Resizer arguments. example: --feature-gates=VolumeAttributesClass=true |
| controller.resizer.resources | object | `{"requests":{"cpu":"10m","memory":"16Mi"}}` | Resizer resource requests and limits. ref: https://kubernetes.io/docs/user-guide/compute-resources/ |
| controller.snapshotter.enabled | bool | `false` |  |
| controller.snapshotter.image | object | `{"pullPolicy":"IfNotPresent","repository":"registry.k8s.io/sig-storage/csi-snapshotter","tag":"v8.3.0"}` | CSI Snapshotter. refs: https://github.com/kubernetes-csi/external-snapshotter |
| controller.snapshotter.args | list | `[]` | Snapshotter arguments. example: --feature-gates=CSIVolumeGroupSnapshot=true |
| controller.snapshotter.resources | object | `{"requests":{"cpu":"10m","memory":"16Mi"}}` | Snapshotter resource requests and limits. ref: https://kubernetes.io/docs/user-guide/compute-resources/ |
| node.plugin.image | object | `{"pullPolicy":"IfNotPresent","repository":"ghcr.io/crunchymonkies/proxmox-csi-node","tag":""}` | Node CSI Driver. |
| node.plugin.resources | object | `{}` | Node CSI Driver resource requests and limits. ref: https://kubernetes.io/docs/user-guide/compute-resources/ |
| node.driverRegistrar.image | object | `{"pullPolicy":"IfNotPresent","repository":"registry.k8s.io/sig-storage/csi-node-driver-registrar","tag":"v2.15.0"}` | Node CSI driver registrar. ref: https://github.com/kubernetes-csi/node-driver-registrar |
| node.driverRegistrar.args | list | `[]` | Driver registrar arguments. example: --timeout=60s |
| node.driverRegistrar.resources | object | `{"requests":{"cpu":"10m","memory":"16Mi"}}` | Node registrar resource requests and limits. ref: https://kubernetes.io/docs/user-guide/compute-resources/ |
| node.kubeletDir | string | `"/var/lib/kubelet"` | Location of the /var/lib/kubelet directory as some k8s distribution differ from the standard. Standard: /var/lib/kubelet, k0s: /var/lib/k0s/kubelet, microk8s: /var/snap/microk8s/common/var/lib/kubelet |
| node.nodeSelector | object | `{}` | Node labels for node-plugin assignment. ref: https://kubernetes.io/docs/user-guide/node-selection/ |
| node.tolerations | list | `[{"effect":"NoSchedule","key":"karpenter.sh/disrupted","operator":"Exists"},{"effect":"NoSchedule","key":"node.kubernetes.io/unschedulable","operator":"Exists"},{"effect":"NoSchedule","key":"node.kubernetes.io/disk-pressure","operator":"Exists"}]` | Tolerations for node-plugin assignment. ref: https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/ |
| node.affinity | object | `{}` | Affinity for node-plugin assignment. ref: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity |
| livenessprobe.image | object | `{"pullPolicy":"IfNotPresent","repository":"registry.k8s.io/sig-storage/livenessprobe","tag":"v2.16.0"}` | Common livenessprobe sidecar. |
| livenessprobe.failureThreshold | int | `5` | Failure threshold for livenessProbe |
| livenessprobe.initialDelaySeconds | int | `10` | Initial delay seconds for livenessProbe |
| livenessprobe.timeoutSeconds | int | `10` | Timeout seconds for livenessProbe |
| livenessprobe.periodSeconds | int | `60` | Period seconds for livenessProbe |
| livenessprobe.resources | object | `{"requests":{"cpu":"10m","memory":"16Mi"}}` | Liveness probe resource requests and limits. ref: https://kubernetes.io/docs/user-guide/compute-resources/ |
| initContainers | list | `[]` | Add additional init containers for the CSI controller pods. ref: https://kubernetes.io/docs/concepts/workloads/pods/init-containers/ |
| hostAliases | list | `[]` | hostAliases Deployment pod host aliases ref: https://kubernetes.io/docs/tasks/network/customize-hosts-file-for-pods/ |
| podAnnotations | object | `{}` | Annotations for controller pod. ref: https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/ |
| podLabels | object | `{}` | Labels for controller pod. ref: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/ |
| podSecurityContext | object | `{"fsGroup":65532,"fsGroupChangePolicy":"OnRootMismatch","runAsGroup":65532,"runAsNonRoot":true,"runAsUser":65532}` | Controller Security Context. ref: https://kubernetes.io/docs/tasks/configure-pod-container/security-context/#set-the-security-context-for-a-pod |
| securityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"readOnlyRootFilesystem":true,"seccompProfile":{"type":"RuntimeDefault"}}` | Controller Container Security Context. ref: https://kubernetes.io/docs/tasks/configure-pod-container/security-context/#set-the-security-context-for-a-pod |
| updateStrategy | object | `{"rollingUpdate":{"maxUnavailable":1},"type":"RollingUpdate"}` | Controller deployment update strategy type. ref: https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#updating-a-deployment |
| metrics | object | `{"enabled":false,"port":8080,"type":"annotation"}` | Prometheus metrics |
| metrics.enabled | bool | `false` | Enable Prometheus metrics. |
| metrics.port | int | `8080` | Prometheus metrics port. |
| nodeSelector | object | `{}` | Node labels for controller assignment. ref: https://kubernetes.io/docs/user-guide/node-selection/ |
| tolerations | list | `[]` | Tolerations for controller assignment. ref: https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/ |
| affinity | object | `{}` | Affinity for controller assignment. ref: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity |
| extraVolumes | list | `[]` | Additional volumes for Pods |
| extraVolumeMounts | list | `[]` |  |
| migrator | object | `{"config":{"clusters":[]},"detachTimeout":"5m","drainTimeout":"10m","enabled":false,"existingConfigSecret":null,"existingConfigSecretKey":"config.yaml","extraArgs":[],"helperVMID":9998,"image":{"pullPolicy":"IfNotPresent","repository":"ghcr.io/crunchymonkies/pvecsictl","tag":""},"leaderElection":true,"maxAttempts":5,"metrics":{"enabled":false,"port":8081},"podFollow":false,"reactiveEvacuation":{"enabled":false,"grace":"2m"},"rebalance":{"enabled":false,"extraArgs":[],"highThreshold":0.8,"lowThreshold":0.6,"maxMigrations":2,"schedule":"0 3 * * *","window":"","windowTz":"UTC"},"replicaCount":1,"resources":{"requests":{"cpu":"10m","memory":"32Mi"}},"taskTimeout":10800}` | Volume migration controller (pvecsictl controller). Watches PVCs for proxmox.crunchymonkies.com/migrate-node annotations and Nodes for proxmox.crunchymonkies.com/evacuate annotations, and migrates the backing Proxmox disks automatically. Requires Proxmox root credentials. |
| migrator.enabled | bool | `false` | Enable the migration controller deployment. |
| migrator.replicaCount | int | `1` | Number of replicas; leader election ensures only one acts. |
| migrator.image.repository | string | `"ghcr.io/crunchymonkies/pvecsictl"` | Migrator (pvecsictl) image. |
| migrator.image.tag | string | `""` | Overrides the image tag whose default is the chart appVersion. |
| migrator.image.pullPolicy | string | `"IfNotPresent"` | Always or IfNotPresent. |
| migrator.config | object | `{"clusters":[]}` | Proxmox cluster config with ROOT credentials (username/password), the same schema as `config`. Token auth is not sufficient for disk migration. Stored in a separate secret from the CSI controller config. |
| migrator.existingConfigSecret | string | `nil` | Existing secret name for the migrator cloud config (overrides `migrator.config`). |
| migrator.existingConfigSecretKey | string | `"config.yaml"` | Existing secret key for the migrator cloud config. |
| migrator.leaderElection | bool | `true` | Enable leader election. |
| migrator.podFollow | bool | `false` | Volume follows pods: automatically migrate a volume when every pod mounting it has been scheduled in a different zone (e.g. after VM migration). Storage is matched by name, falling back to the cluster's `primary_storage` map. |
| migrator.reactiveEvacuation | object | `{"enabled":false,"grace":"2m"}` | Reactive node evacuation: make a standard `kubectl drain` transparent for zone-local volumes. When a pod cannot be scheduled because its volume is pinned to a cordoned/tainted node, the controller migrates the volume so the pod can schedule elsewhere. kubectl drain still owns stateless pod eviction and PDBs; this only moves the volume. Opt-in and conservative. |
| migrator.reactiveEvacuation.enabled | bool | `false` | Enable reactive evacuation. |
| migrator.reactiveEvacuation.grace | string | `"2m"` | How long a pod must stay unschedulable before a migration is stamped. Prevents a quick maintenance cordon+reboot from triggering a large copy. |
| migrator.maxAttempts | int | `5` | Maximum migration attempts per PVC before marking it Failed. |
| migrator.helperVMID | int | `9998` | VM ID of the transient helper VM used to convert qcow2/vmdk volumes to raw during migration. Must be a free VM ID and must differ from the controller VMID. |
| migrator.taskTimeout | int | `10800` | Proxmox disk move task timeout in seconds. |
| migrator.drainTimeout | string | `"10m"` | Maximum time to wait for pods to terminate during force-drain. |
| migrator.detachTimeout | string | `"5m"` | Maximum time to wait for a disk to detach from a VM. |
| migrator.metrics | object | `{"enabled":false,"port":8081}` | Prometheus metrics for the migrator. |
| migrator.extraArgs | list | `[]` | Additional migrator container arguments. |
| migrator.resources | object | `{"requests":{"cpu":"10m","memory":"32Mi"}}` | Migrator resource requests and limits. |
| migrator.rebalance | object | `{"enabled":false,"extraArgs":[],"highThreshold":0.8,"lowThreshold":0.6,"maxMigrations":2,"schedule":"0 3 * * *","window":"","windowTz":"UTC"}` | Scheduled rebalancing of idle volumes from overloaded Proxmox nodes. |
| migrator.rebalance.enabled | bool | `false` | Enable the rebalance CronJob. |
| migrator.rebalance.schedule | string | `"0 3 * * *"` | Rebalance schedule (cron format). |
| migrator.rebalance.highThreshold | float | `0.8` | Zones above this used fraction are rebalancing sources. |
| migrator.rebalance.lowThreshold | float | `0.6` | Only zones below this used fraction are rebalancing targets. |
| migrator.rebalance.maxMigrations | int | `2` | Maximum volumes to move per run. |
| migrator.rebalance.window | string | `""` | Maintenance window "HH:MM-HH:MM"; outside it the job exits immediately. |
| migrator.rebalance.windowTz | string | `"UTC"` | IANA time zone for the maintenance window. |
| migrator.rebalance.extraArgs | list | `[]` | Additional rebalance arguments. |
