# Volumes per node

A Proxmox VM has a fixed number of disk slots per bus (qemu-server `PVE/QemuServer/Drive.pm`):

| Bus | Slots | Hotplug | Used by the driver |
|---|---|---|---|
| SCSI | `scsi0`–`scsi30` (31) | yes | `scsi1`–`scsi30` |
| virtio-blk | `virtio0`–`virtio15` (16) | yes | every free slot, once SCSI is full |
| SATA | `sata0`–`sata5` (6) | no | no |
| IDE | `ide0`–`ide3` (4) | no | no |

The driver attaches volumes to SCSI slots first, exactly as before. Only when every SCSI slot
is taken does it use virtio-blk, so a Linux node can hold up to 46 CSI volumes instead of 29.
SATA and IDE are not used because PVE cannot hot-plug them.

## Raising the limit

The scheduler only places as many volumes on a node as the node reports, 24 by default. To use
more, label the node:

```yaml
metadata:
  labels:
    csi.proxmox.sinextra.dev/max-volume-attachments: "44"
```

Count the VM's other disks when choosing the value: the boot disk, a cloud-init drive on a
SCSI slot, or any disk that is not a CSI volume takes a slot from the same 46. Values above 46
are ignored and the default is used.

## How a virtio-blk volume differs

- **Identity.** virtio-blk has no WWN. The disk is given `serial=PVC-IDnn` and published as
  `/dev/disk/by-id/virtio-PVC-IDnn`. The node finds it by reading `/sys/block/vd*/serial`, then
  falls back to the by-id link.
- **SSD flag.** PVE's `ssd` option is SCSI/SATA/IDE-only (it sets the SCSI rotation rate), and
  Linux reports every virtio-blk disk as rotational. The driver therefore drops `ssd` from the
  virtio disk's config and, when the StorageClass sets `ssd: "true"`, the node writes `0` to
  `/sys/block/vdX/queue/rotational` when it stages the volume, so the guest sees a
  non-rotational disk as it would on SCSI. `ssd` still implies `discard=on`, which virtio-blk
  passes through, so TRIM reaches the backing storage the same way.
- **Other options.** `iothread`, `cache`, `discard`, `aio`, `backup`, `replicate` and the
  bandwidth and IOPS limits apply as usual.
- **PCI addresses.** Each virtio-blk disk is its own PCI device, the same as each disk behind
  `virtio-scsi-single`. PVE reserves a fixed address for every `virtio0`–`virtio15` slot, so
  they do not compete with passthrough or network devices. Hotplug works on both `i440fx` and
  `q35`; on `q35` PVE places them behind its `pci.N` bridges.
- **Guest driver.** The guest needs `virtio_blk`. Every mainstream Linux kernel and Talos ship it.
- **Windows.** Windows nodes find disks by SCSI WWN only, so they stay limited to 30 volumes.

A volume keeps the slot it was attached with until it is detached; existing SCSI attachments
are not moved. Moving a volume to another VM (the volume migrator, or a pod rescheduled to
another node) detaches it, moves or renames it at the storage level, and attaches it to the
next free slot on the target VM, so it may come back on SCSI or virtio regardless of where it
was before.

## Going beyond 46

46 is the most the PVE API can hot-plug into one VM; the slot counts are fixed in qemu-server.
To place more volumes on one Kubernetes node:

- **Network block storage from inside the guest** (for example ceph-csi RBD on a Ceph cluster,
  or iSCSI / NVMe-oF exports from the Proxmox hosts) uses no VM disk slots at all. It is a
  different driver and runs alongside this one.
- **More, smaller nodes** spread the volumes across more VMs.
- **Fewer, larger volumes**, carved up inside the guest (LVM or a local-volume driver on a big
  CSI disk), trade per-volume isolation and snapshots for density.

Hot-adding devices through the QEMU monitor, or patching qemu-server's slot limits, would get
past 46 but leaves PVE's view of the VM wrong: such disks are lost on restart and break
migration and backup.
