# Plugin configuration file

This file is used to configure the Proxmox CSI driver plugin.

```yaml
features:
  # Provider type
  provider: default|capmox
  # Controller VM ID. Must be greater than 100.
  # Default is 9999, which is a safe value that is unlikely to conflict with existing VMs.
  # You can change it if needed, but make sure to choose a value that is not used by any existing VM in your Proxmox cluster.
  controllerVMID: 9999
  # Reassign a volume's Proxmox ownership (vmid) to the real VM on attach.
  # Default is false. See docs/reassign-volume-on-attach.md before enabling.
  reassignVolumeOnAttach: false

clusters:
  # List of Proxmox clusters
  - url: https://cluster-api-1.exmple.com:8006/api2/json
    # Skip the certificate verification, if needed
    insecure: false
    # Proxmox api token
    token_id: "kubernetes-csi@pve!csi"
    token_id_file: "/etc/proxmox/token_id"          # Optional, alternative to token_id
    token_secret: "secret"
    token_secret_file: "/etc/proxmox/token_secret"  # Optional, alternative to token_secret
    # Region name, which is cluster name
    region: Region-1

  # Add more clusters if needed
  - url: https://cluster-api-2.exmple.com:8006/api2/json
    insecure: false
    token_id: "kubernetes-csi@pve!csi"
    token_secret: "secret"
    region: Region-2

  # Or reference a Kubernetes Secret containing both the token ID and token secret,
  # instead of token_id/token_secret
  - url: https://cluster-api-3.exmple.com:8006/api2/json
    insecure: false
    token_ref:
      name: proxmox-cluster-3-token
      namespace: kube-system                # Optional, defaults to the controller's own namespace
      tokenIdKey: token_id                  # Optional, default "token_id"
      tokenSecretKey: token_secret          # Optional, default "token_secret"
    region: Region-3
```

## Cluster list

You can define multiple clusters in the `clusters` section.

* `url` - The URL of the Proxmox cluster API.
* `insecure` - Set to `true` to skip TLS certificate verification.
* `token_id` - The Proxmox API token ID.
* `token_id_file` - The path to a file containing the Proxmox API token ID. This is an alternative to `token_id`.
* `token_secret` - The name of the Kubernetes Secret that contains the Proxmox API token.
* `token_secret_file` - The path to a file containing the Proxmox API token secret. This is an alternative to `token_secret`.
* `token_ref` - Reference to a Kubernetes Secret holding both the token ID and token secret. Alternative to `token_id`/`token_id_file` and `token_secret`/`token_secret_file`; cannot be combined with them.
  * `token_ref.name` - Name of the Secret.
  * `token_ref.namespace` - Namespace of the Secret. Optional, defaults to the controller's own namespace.
  * `token_ref.tokenIdKey` - Secret data key holding the token ID. Optional, default `token_id`.
  * `token_ref.tokenSecretKey` - Secret data key holding the token secret. Optional, default `token_secret`.
* `region` - The name of the region, which is also used as `topology.kubernetes.io/region` label.
* `token_copy_endpoint` - Per-cluster override for the server-side volume copy used by volume migration **and by volume snapshots**, routing it through the `pve-csi-copy` package's endpoint so a scoped token is used instead of `root@pam`. See [`docs/migration-controller.md`](migration-controller.md) and [`docs/volumesnapshot.md`](volumesnapshot.md).
* `proxmod_endpoint` - Per-cluster override for the same copy, routing it through the `proxmox-csi-storage` proxmod extension instead. Same purpose and same ACL requirements as `token_copy_endpoint`, different server-side implementation; wins if both are true. See [`docs/migration-controller.md`](migration-controller.md) and [`docs/volumesnapshot.md`](volumesnapshot.md).

## Feature flags

* `provider` - Set the provider type. The default is `default`, which uses provider-id to define the Proxmox VM ID. The `capmox` value is used for working with the Cluster API for Proxmox (CAPMox).
* `controllerVMID` - The placeholder VM ID the controller uses to own CSI volumes before they're attached. Default is `9999`.
* `reassignVolumeOnAttach` - Reassign a volume's Proxmox ownership to the real target VM on attach. Default is `false`. See [`docs/reassign-volume-on-attach.md`](reassign-volume-on-attach.md) — carries a VolumeID-stability risk on LVM/ZFS/directory storage.
