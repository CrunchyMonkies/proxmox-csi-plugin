# Windows nodes

Windows Server worker VMs can mount Proxmox volumes the same way Linux nodes do: the controller
hot-plugs the disk into the VM and the node plugin on the Windows node formats and mounts it. The
Windows node plugin runs as a [HostProcess](https://kubernetes.io/docs/tasks/configure-pod-container/create-hostprocess-pod/)
DaemonSet (Kubernetes 1.26+) and drives the Windows Storage cmdlets directly, so no csi-proxy is
needed on the host.

## Requirements

- Windows Server 2019 or later, with the [virtio-win](https://github.com/virtio-win/virtio-win-pkg-scripts)
  `vioscsi` driver (a guest that boots from virtio-scsi already has it).
- VM `scsihw` virtio-scsi (`virtio-scsi-single` is the default for this driver) and `hotplug`
  including `disk` (the Proxmox default).
- The controller must be able to find the node's VM. Windows nodes rarely carry a `proxmox://`
  providerID, so the controller falls back to matching the node's SMBIOS UUID, which Windows
  reports upper-cased; this release matches it case-insensitively. Setting the
  [instance-id annotation](node-instance-id.md) skips the lookup entirely.

## Enable

```yaml
node:
  windows:
    enabled: true
    # the taint that keeps Linux workloads off the Windows nodes, if any
    tolerations:
      - key: os
        operator: Equal
        value: windows
        effect: NoSchedule
```

The Linux node DaemonSet is pinned to `kubernetes.io/os: linux`, the Windows one to
`kubernetes.io/os: windows`. The namespace must allow privileged pods (HostProcess).

## StorageClass

Windows volumes are NTFS. Use a StorageClass without `csi.storage.k8s.io/fstype`, or with `ntfs`:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: proxmox-ntfs
provisioner: csi.proxmox.sinextra.dev
parameters:
  storage: local
  csi.storage.k8s.io/fstype: ntfs
  # let non-admin container users (nanoserver's ContainerUser) create files in the volume root
  rootDirPermissions: "0777"
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
```

A new NTFS volume root lets `BUILTIN\Users` create folders but not files, the counterpart of a
root-owned `0755` directory on Linux. A `rootDirPermissions` mode that lets group or others write
grants `BUILTIN\Users` Modify on the volume root instead.

Not supported on Windows nodes: ext4/xfs, raw block volumes (`volumeMode: Block`), encrypted
volumes and Kata direct volumes. Requests for them fail with `InvalidArgument`.

## How it works

- **Finding the disk.** The controller sets `wwn=0x…` on every disk it attaches. Windows shows the
  same SCSI identifier, upper-cased, as `Get-Disk`'s `UniqueId`.
- **Staging.** Windows Server brings new SCSI disks up offline (SAN policy *Offline Shared*), so
  the plugin puts the disk online, creates a GPT partition table and one NTFS partition on a new
  disk, and mounts the partition on the staging directory as an NTFS mount point.
- **Publishing** links the pod's target path to the staging directory with a directory symlink.
- **Unstaging** flushes the volume, removes the mount point and takes the disk offline before the
  controller unplugs it.
- **Expansion** rescans the disk and grows the partition online.

The plugin never formats a disk it cannot prove empty. A volume formatted by a Linux node carries
its filesystem without a partition table, which Windows reports exactly like a new disk; before
writing a partition table or a filesystem the plugin reads the first MiB and refuses with
`FailedPrecondition` unless it is all zeros.

## Detaching from Windows guests

With `virtio-scsi-single` every disk has its own virtio-scsi PCI controller, unplugged after the
disk. Windows intermittently ignores the first eject request for that controller and Proxmox
gives up after 15 seconds (`hotplug problem … still busy in guest?`), leaving the removal pending
in the VM config. The controller recognises the error and unlinks a second time, which applies the
pending removal. The empty controller stays in the VM until it reboots and is reused when a disk
is attached to the same slot again.
