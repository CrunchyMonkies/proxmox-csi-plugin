# Kata Containers direct volumes

Pods that run in a [Kata Containers](https://katacontainers.io/) VM normally see a filesystem volume
through **virtio-fs**: the node mounts the volume and every file operation crosses the VM boundary.
With direct volumes enabled, the node plugin hands the Proxmox disk to Kata instead. Kata hot-plugs the
block device into the VM (virtio-scsi on QEMU, virtio-blk on Cloud Hypervisor) and mounts the
filesystem **inside the guest**, so I/O never leaves the VM.

## Enable

```yaml
node:
  kataDirectVolumes:
    enabled: true
    handlerPrefix: kata   # RuntimeClass handlers that run pods in Kata VMs
    root: /run/kata-containers/shared/direct-volumes
```

The node plugin then:

- mounts `root` from the host (Kata reads volume descriptions from there);
- gets `get` on `pods` and `runtimeclasses` to tell whether the pod a volume is published to runs in
  Kata (the CSIDriver already has `podInfoOnMount: true`).

Nothing changes for pods of other runtimes, for raw block volumes (`volumeMode: Block`, which Kata
already passes through as devices), or while the flag is off.

To keep a StorageClass on virtio-fs even for Kata pods, set the parameter:

```yaml
parameters:
  kataDirectVolume: "false"
```

The parameter reaches the node through the volume context, which the controller records when it
provisions the volume. It therefore only affects volumes provisioned by a controller of this version
or later; older volumes (and volumes provisioned before the upgrade) always use direct volumes for
Kata pods while the feature is enabled.

## How it works

A volume is staged (formatted and mounted on the node) as usual. When it is published to a Kata pod,
the node plugin:

1. unmounts the staging path, so the VM is the only one with the filesystem mounted;
2. writes `<root>/<base64url(target path)>/mountInfo.json`
   (`{"volume-type":"directvol","device":"/dev/sdX","fstype":"ext4","options":[...]}`), Kata's
   [direct-assigned volume](https://github.com/kata-containers/kata-containers/blob/main/docs/design/direct-blk-device-assignment.md)
   description;
3. creates the target directory without mounting anything on it.

Kata finds the description when it creates the container and mounts the device in the guest with the
same filesystem type and mount options the node would use (plus `ro` for read-only volumes). When the
pod goes away, unpublish removes the description and mounts the staging path again, so the volume can
be published to another pod or unstaged normally. LUKS-encrypted volumes work the same way: Kata gets
the opened `/dev/mapper/...` device.

## Limits

- **One pod at a time per node.** A filesystem mounted inside a VM must not be mounted anywhere else.
  While a Kata pod holds the volume, publishing it to another pod on the node fails with
  `FailedPrecondition` (and a Kata pod cannot take a volume another pod on the node is using). The pod
  retries until the volume is free.
- **Usage statistics**: the node only knows the disk's size; used bytes and inodes live inside the VM,
  so `NodeGetVolumeStats` reports capacity only.
- **Expansion** finishes when the pod restarts: `NodeExpandVolume` returns `FailedPrecondition` while the
  VM holds the volume, and the kubelet grows the filesystem after the next stage.
- Requires the Kata **runtime-rs** shim (`io.containerd.kata-*.v2` handlers such as
  `qemu-runtime-rs`, `clh-runtime-rs`; tested with Kata 4.1) and `disable_block_device_use = false`
  in its configuration (the default).
