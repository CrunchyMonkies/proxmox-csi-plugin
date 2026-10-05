/*
Copyright 2023 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package proxmox

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	proxmox "github.com/luthermonson/go-proxmox"
	"github.com/siderolabs/go-retry/retry"

	goproxmox "github.com/sergelogvinov/go-proxmox"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/metrics"
	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	volume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"

	"k8s.io/klog/v2"
)

const (
	// DeviceNamePrefix is the disk device prefix used by the CSI driver.
	DeviceNamePrefix = "scsi"

	// TaskStatusCheckInterval is the interval in seconds to check the status of a task.
	TaskStatusCheckInterval = 5
	// TaskTimeout is the timeout in seconds for all tasks.
	TaskTimeout = 30

	// CopyTaskTimeout bounds how long a snapshot copy (or restore-from-snapshot)
	// waits for its Proxmox task. Copying a whole volume between storages is
	// orders of magnitude slower than the config-level operations TaskTimeout is
	// sized for, so it gets its own constant: 1 hour, matching the 240 polls at
	// 15s this code waited before it was routed through MoveQemuDisk.
	CopyTaskTimeout = 3600

	// ErrorNotFound not found error message.
	ErrorNotFound string = "not found"
)

// ErrStorageAbsent is ErrorNotFound raised because the storage itself is absent
// from the node, rather than because the volume is missing from a storage that
// exists. Its message is deliberately ErrorNotFound so the string comparisons
// callers make keep working unchanged; ResolveVolume tells the two apart with
// errors.Is, because searching a storage that does not exist can only produce
// noise — PVE answers a content listing on an unknown storage with a 500.
var ErrStorageAbsent = errors.New(ErrorNotFound)

// GetNodeForVolume finds a Proxmox node that serves the volume's storage.
//
//nolint:unused
func GetNodeForVolume(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume) (node string, err error) {
	node = vol.Node()
	if node == "" {
		nodes, err := cl.GetNodesForStorage(ctx, vol.Storage())
		if err != nil {
			return "", fmt.Errorf("failed to find zones for storage %s: %v", vol.Storage(), err)
		}

		if len(nodes) == 0 {
			return "", fmt.Errorf("failed to find best zone for storage %s", vol.Storage())
		}

		node = nodes[0]
	}

	return
}

// GetVMByAttachedVolume finds the VM whose config references vol, skipping
// skipVMID — the controller's placeholder VM, which owns CSI volumes at rest and
// is never a publish target.
//
// skipVMID is passed in rather than read off the volume: with
// reassignVolumeOnAttach on, an attached volume is named for the VM holding it, so
// deriving the id to skip from the name would skip exactly the VM being looked for.
func GetVMByAttachedVolume(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume, skipVMID int) (int, int, error) {
	var err error

	nodes := []string{}
	if vol.Node() != "" {
		nodes = append(nodes, vol.Node())
	}

	if len(nodes) == 0 {
		nodes, err = cl.GetNodesForStorage(ctx, vol.Storage())
		if err != nil {
			return 0, 0, fmt.Errorf("failed to find zones for storage %s: %v", vol.Storage(), err)
		}
	}

	if len(nodes) == 0 {
		return 0, 0, fmt.Errorf("failed to find best zone: no nodes with the storage %s", vol.Storage())
	}

	lun := 0

	vm, err := cl.GetVMByFilter(ctx, func(rs *proxmox.ClusterResource) (bool, error) {
		if rs.Type != "qemu" {
			return false, nil
		}

		// Skip the storage owner VM (e.g., 9999), as the VM uses for the replications
		if skipVMID != 0 && int(rs.VMID) == skipVMID {
			return false, nil
		}

		if !slices.Contains(nodes, rs.Node) {
			return false, nil
		}

		vm, err := pxpool.GetVMConfigByResource(ctx, cl, rs)
		if err != nil {
			return false, err
		}

		if l, exist := IsVolumeAttached(vm.VirtualMachineConfig, VolumeMatch(vol)); exist {
			lun = l

			return true, nil
		}

		return false, nil
	})
	if err != nil {
		return 0, lun, err
	}

	if vm.VMID != 0 {
		if vol.Node() == "" {
			vol.SetNode(vm.Node)
		}

		return int(vm.VMID), lun, nil
	}

	return 0, 0, goproxmox.ErrVirtualMachineNotFound
}

// GetStorageContent returns the content entry for a volume, or nil if it is not
// on the storage.
func GetStorageContent(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume) (*proxmox.StorageContent, error) {
	if vol.Node() == "" {
		return nil, errors.New("node is required")
	}

	if _, err := cl.GetStorageStatus(ctx, vol.Node(), vol.Storage()); err != nil {
		if strings.Contains(err.Error(), "No such storage") {
			return nil, ErrStorageAbsent
		}

		return nil, err
	}

	contents, err := cl.GetStorageContent(ctx, vol.Node(), vol.Storage())
	if err != nil {
		return nil, err
	}

	for _, content := range contents {
		if content.Volid == vol.VolID() {
			return content, nil
		}
	}

	return nil, nil
}

// GetStorageLevel returns whether a storage is "file" or "block" level.
func GetStorageLevel(storage *proxmox.ClusterResource) string {
	return StorageLevel(storage.PluginType)
}

// StorageLevel returns whether a plugin type is "file" or "block" level.
//
// Extracted so the operator can call it with a plugin type string without
// needing a *proxmox.ClusterResource.
func StorageLevel(pluginType string) string {
	// see https://pve.proxmox.com/wiki/Storage
	switch pluginType {
	case "dir", "nfs", "cifs", "cephfs", "btrfs": // nolint: goconst
		return "file"
	default:
		return "block"
	}
}

// DiskFormat determines the disk format and filename extension for a volume,
// based on the storage plugin type and the requested format from parameters.
//
// This is the single source of truth for both the CSI controller and the
// operator provisioner. If either side builds a volumeID with a different
// format, the PV's immutable volumeHandle won't match what Proxmox stores.
//
// Rules (matching pkg/csi/controller.go CreateVolume ~L351-364):
//   - lvm + qcow2 requested → "qcow2" (LVM snapshot as volume-chain preview)
//   - file-level storage (dir/nfs/cifs/cephfs/btrfs) → "raw" by default,
//     "qcow2" if requested
//   - everything else (lvmthin, zfspool, rbd, etc.) → "" (no format suffix)
func DiskFormat(pluginType, requestedFormat string) string {
	if pluginType == "lvm" && requestedFormat == "qcow2" {
		return "qcow2"
	}

	if StorageLevel(pluginType) == "file" {
		if requestedFormat == "qcow2" {
			return "qcow2"
		}

		return "raw"
	}

	return ""
}

// GetVolumeSize returns the size of a volume on Proxmox storage.
func GetVolumeSize(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume) (int64, error) {
	st, err := GetStorageContent(ctx, cl, vol)
	if err != nil {
		return 0, err
	}

	if st == nil {
		return 0, errors.New(ErrorNotFound)
	}

	return int64(st.Size), nil
}

// VolumeMatch returns the string IsVolumeAttached should look for in a VM config.
//
// With features.reassignVolumeOnAttach on, an attached volume is named for the VM
// that owns it rather than for the vmid the PV's immutable volumeHandle carries,
// so matching on the full disk name would miss it. The suffix ('pvc-<uuid>.raw')
// is the part a rename leaves alone.
//
// Disks with no such suffix — anything not in Proxmox's 'vm-<vmid>-<name>' form —
// cannot be renamed at all, so their full name is already the stable one.
func VolumeMatch(vol *volume.Volume) string {
	if suffix := vol.DiskSuffix(); suffix != "" {
		return suffix
	}

	return vol.Disk()
}

// ResolveVolume returns vol under the name Proxmox currently stores it as.
//
// Every path that addresses the volume as storage rather than through a VM config
// — delete, expand, modify — has to go through here once reassignVolumeOnAttach is
// in play, or it acts on a name that exists only in Kubernetes.
//
// The search is by suffix and adopts whichever vmid it finds, rather than assuming
// either end of the rename: a controller that died between renaming and attaching
// (or between detaching and renaming back) leaves the volume on whichever of the
// two names it reached, and this is what picks it back up.
//
// Returns ErrorNotFound when the volume is on neither name, so callers that treat
// a missing volume as success keep doing so.
func ResolveVolume(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume, reassign bool) (*volume.Volume, int64, error) {
	size, err := GetVolumeSize(ctx, cl, vol)
	if err == nil {
		return vol, size, nil
	}

	// The search runs only with the feature on. Its failure modes are not free: a
	// storage that has been removed from PVE answers a content listing with a 500,
	// which would turn what is otherwise a clean NotFound into an Internal error and
	// leave DeleteVolume retrying forever against a PV that can never be satisfied.
	// With the feature off no volume is ever on a second name, so there is nothing
	// to search for and the pre-existing behavior is kept exactly.
	if !reassign || err.Error() != ErrorNotFound || errors.Is(err, ErrStorageAbsent) || vol.DiskSuffix() == "" {
		return nil, 0, err
	}

	found, err := FindVolumeBySuffix(ctx, cl, vol)
	if err != nil {
		return nil, 0, err
	}

	if found == nil {
		return nil, 0, errors.New(ErrorNotFound)
	}

	size, err = GetVolumeSize(ctx, cl, found)
	if err != nil {
		return nil, 0, err
	}

	klog.V(4).InfoS("resolveVolume: volume is on another vmid", "volumeID", vol.VolumeID(), "disk", found.Disk())

	return found, size, nil
}

// IsVolumeAttached checks whether a volume is attached to a VM by scanning its
// SCSI configuration.
func IsVolumeAttached(vm *proxmox.VirtualMachineConfig, pvc string) (int, bool) {
	if pvc == "" {
		return 0, false
	}

	disks := vm.MergeSCSIs()
	for lun, disk := range disks {
		if strings.Contains(disk, pvc) {
			i, err := strconv.Atoi(strings.TrimPrefix(strings.Split(lun, ":")[0], DeviceNamePrefix))
			if err != nil {
				return 0, false
			}

			return i, true
		}
	}

	return 0, false
}

// CreateVolume creates a new VM disk on Proxmox storage.
func CreateVolume(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume, sizeBytes int64) error {
	if vol.Node() == "" {
		return errors.New("node is required")
	}

	filename := strings.Split(vol.Disk(), "/")

	id, err := strconv.Atoi(vol.VMID())
	if err != nil {
		return fmt.Errorf("failed to parse volume vm id: %v", err)
	}

	disk, err := cl.CreateVMDisk(ctx, id, vol.Node(), vol.Storage(), filename[len(filename)-1], sizeBytes)
	if err != nil {
		return fmt.Errorf("failed to create vm disk: %v", err)
	}

	diskName := strings.Split(disk, ":")
	if len(diskName) > 1 {
		vol.SetDisk(diskName[1])
	}

	return nil
}

// AttachVolume attaches a volume to a VM, returning the device path and lun.
func AttachVolume(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume, options map[string]string) (map[string]string, error) {
	vm, err := cl.GetVMConfig(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get vm config: %v", err)
	}

	wwm := ""

	lun, exist := IsVolumeAttached(vm.VirtualMachineConfig, VolumeMatch(vol))
	if exist {
		wwm = hex.EncodeToString([]byte(fmt.Sprintf("PVC-ID%02d", lun)))
	} else {
		disks := vm.VirtualMachineConfig.MergeSCSIs()

		for lun = 1; lun < 30; lun++ {
			device := DeviceNamePrefix + strconv.Itoa(lun)

			if disks[device] == "" {
				wwm = hex.EncodeToString([]byte(fmt.Sprintf("PVC-ID%02d", lun)))

				options["wwn"] = "0x" + wwm

				opt := make([]string, 0, len(options))
				for k := range options {
					opt = append(opt, fmt.Sprintf("%s=%s", k, options[k]))
				}

				vmOptions := proxmox.VirtualMachineOption{
					Name:  device,
					Value: fmt.Sprintf("%s:%s,%s", vol.Storage(), vol.Disk(), strings.Join(opt, ",")),
				}

				task, err := vm.Config(ctx, vmOptions)
				if err != nil {
					return nil, fmt.Errorf("unable to attach disk: %v, options=%+v", err, vmOptions)
				}

				if task != nil {
					if err := task.WaitFor(ctx, 5*60); err != nil {
						return nil, fmt.Errorf("unable to attach virtual machine disk: %w", err)
					}

					if task.IsFailed {
						return nil, fmt.Errorf("unable to attach virtual machine disk: %s", task.ExitStatus)
					}
				}

				if err := WaitAttachVolume(ctx, cl, id, vol); err != nil {
					return nil, err
				}

				break
			}
		}
	}

	if wwm != "" {
		return map[string]string{
			"DevicePath": "/dev/disk/by-id/wwn-0x" + wwm,
			"lun":        strconv.Itoa(lun),
		}, nil
	}

	return nil, fmt.Errorf("no free lun found")
}

// DetachVolume detaches a volume from a VM.
func DetachVolume(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume) error {
	vm, err := cl.GetVMConfig(ctx, id)
	if err != nil {
		if errors.Is(err, goproxmox.ErrVirtualMachineNotFound) {
			return nil
		}

		return fmt.Errorf("failed to get vm config: %v", err)
	}

	if lun, ok := IsVolumeAttached(vm.VirtualMachineConfig, VolumeMatch(vol)); ok {
		task, err := vm.UnlinkDisk(ctx, fmt.Sprintf("%s%d", DeviceNamePrefix, lun), false)
		if err != nil {
			return fmt.Errorf("failed to unlink disk: %v", err)
		}

		if task != nil {
			if err := task.WaitFor(ctx, 5*60); err != nil {
				return fmt.Errorf("unable to detach virtual machine disk: %w", err)
			}

			if task.IsFailed {
				return fmt.Errorf("unable to detach virtual machine disk: %s", task.ExitStatus)
			}
		}
	}

	return nil
}

// ClearUnusedDisk removes the `unused<n>` key that detaching leaves behind for a
// volume the VM owns. It is safe ONLY once that volume has been renamed away.
//
// UnlinkDisk(force=false) does not deallocate: it moves the drive to an `unused<n>`
// key, because with reassignVolumeOnAttach the volume is genuinely owned by this
// VM. Deleting that key is what PVE treats as the deallocation — try_deallocate_drive
// destroys the volume for real if it is still there. Once the volume has been
// renamed back to the placeholder vmid the key names a path that no longer exists,
// and deleting it removes the config line and nothing else.
//
// The existence check below is the backstop for that: if the referenced volume is
// still on storage, the key is left in place rather than risking a deallocation.
// Errors are the caller's to log and ignore — a stale config line is untidy, not
// harmful.
func ClearUnusedDisk(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume) error {
	vm, err := cl.GetVMConfig(ctx, id)
	if err != nil {
		if errors.Is(err, goproxmox.ErrVirtualMachineNotFound) {
			return nil
		}

		return fmt.Errorf("failed to get vm config: %v", err)
	}

	match := VolumeMatch(vol)

	for key, disk := range vm.VirtualMachineConfig.MergeUnuseds() {
		volid := strings.Split(disk, ",")[0]
		if !strings.Contains(volid, match) {
			continue
		}

		storage, name, ok := strings.Cut(volid, ":")
		if !ok {
			continue
		}

		unused := volume.NewVolume(vol.Region(), vol.Zone(), storage, name)
		unused.SetNode(vol.Node())

		if _, err := GetVolumeSize(ctx, cl, unused); err == nil {
			return fmt.Errorf("refusing to delete %s: volume %s still exists, deleting the key would deallocate it", key, volid)
		} else if err.Error() != ErrorNotFound {
			return fmt.Errorf("failed to check whether %s still exists: %v", volid, err)
		}

		task, err := vm.Config(ctx, proxmox.VirtualMachineOption{Name: "delete", Value: key})
		if err != nil {
			return fmt.Errorf("failed to delete %s: %v", key, err)
		}

		if task != nil {
			if err := task.WaitFor(ctx, 5*60); err != nil {
				return fmt.Errorf("failed to wait for %s removal: %w", key, err)
			}
		}
	}

	return nil
}

// UpdateVolume updates the disk options for a volume attached to a VM.
func UpdateVolume(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume, options map[string]string) error {
	vm, err := cl.GetVMConfig(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to get vm config: %v", err)
	}

	if lun, ok := IsVolumeAttached(vm.VirtualMachineConfig, VolumeMatch(vol)); ok {
		// The volid the VM config already carries, not one rebuilt from vol: with
		// reassignVolumeOnAttach on, an attached volume is named for this VM while
		// vol still carries the name from the PV's immutable volumeHandle, and
		// rebuilding would rewrite the config back to a name that does not exist.
		volid := fmt.Sprintf("%s:%s", vol.Storage(), vol.Disk())

		disks := vm.VirtualMachineConfig.MergeSCSIs()
		if disk := disks[DeviceNamePrefix+strconv.Itoa(lun)]; disk != "" {
			params := strings.Split(disk, ",")
			for i, param := range params {
				if i == 0 {
					volid = param

					continue
				}

				kv := strings.Split(param, "=")
				if len(kv) == 2 && options[kv[0]] == "" {
					options[kv[0]] = kv[1]
				}
			}
		}

		opt := make([]string, 0, len(options))
		for k := range options {
			opt = append(opt, fmt.Sprintf("%s=%s", k, options[k]))
		}

		vmOptions := proxmox.VirtualMachineOption{
			Name:  DeviceNamePrefix + strconv.Itoa(lun),
			Value: fmt.Sprintf("%s,%s", volid, strings.Join(opt, ",")),
		}

		task, err := vm.Config(ctx, vmOptions)
		if err != nil {
			return fmt.Errorf("unable to update disk: %v, options=%+v", err, vmOptions)
		}

		if err := task.WaitFor(ctx, 5*60); err != nil {
			return fmt.Errorf("unable to update virtual machine disk: %w", err)
		}

		return nil
	}

	return fmt.Errorf("volume is not attached to VM %d", id)
}

// CopyVolume copies srcVol to destVol, used by snapshot creation and by
// restore-from-snapshot. endpoint selects the server-side copy implementation:
// the built-in content copy needs root@pam, the other two accept a scoped API
// token. See pkg/tools/proxmox.MoveQemuDisk for the per-endpoint request shapes
// and docs/volumesnapshot.md for the credentials each one needs.
func CopyVolume(
	ctx context.Context,
	cl *goproxmox.APIClient,
	srcVol *volume.Volume,
	destVol *volume.Volume,
	endpoint pxpool.CopyEndpoint,
) error {
	if srcVol.Node() == "" {
		return errors.New("node is required")
	}

	if strings.Contains(destVol.Disk(), ".qcow2") {
		return errors.New("volume disk must not be qcow2 format")
	}

	node := srcVol.Node()
	if destVol.Node() != "" {
		node = destVol.Node()
	}

	return MoveQemuDisk(ctx, cl, srcVol, node, destVol, CopyTaskTimeout, endpoint)
}

// WaitAttachVolume waits for a volume to appear in a VM's config.
func WaitAttachVolume(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume) error {
	err := retry.Constant(TaskTimeout*time.Second, retry.WithUnits(TaskStatusCheckInterval*time.Second)).Retry(func() error {
		vm, err := cl.GetVMConfig(ctx, id)
		if err != nil {
			return fmt.Errorf("failed to get vm config: %v", err)
		}

		if _, ok := IsVolumeAttached(vm.VirtualMachineConfig, VolumeMatch(vol)); ok {
			return nil
		}

		return retry.ExpectedError(fmt.Errorf("volume %s is not attached to VM %d", vol.VolumeID(), id))
	})
	if err != nil {
		if retry.IsTimeout(err) {
			return fmt.Errorf("volume %s is not attached to VM %d", vol.VolumeID(), id)
		}

		return err
	}

	return nil
}

// WaitDetachVolume waits for a volume to disappear from a VM's config.
func WaitDetachVolume(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume) error {
	err := retry.Constant(TaskTimeout*time.Second, retry.WithUnits(TaskStatusCheckInterval*time.Second)).Retry(func() error {
		vm, err := cl.GetVMConfig(ctx, id)
		if err != nil {
			if errors.Is(err, goproxmox.ErrVirtualMachineNotFound) {
				return nil
			}

			return fmt.Errorf("failed to get vm config: %v", err)
		}

		if _, ok := IsVolumeAttached(vm.VirtualMachineConfig, VolumeMatch(vol)); ok {
			return retry.ExpectedError(fmt.Errorf("volume %s still attached to VM %d", vol.VolumeID(), id))
		}

		return nil
	})
	if err != nil {
		if retry.IsTimeout(err) {
			return fmt.Errorf("volume %s still attached to VM %d", vol.VolumeID(), id)
		}

		return err
	}

	return nil
}

// PrepareReplication finds or creates the replication VM.
func PrepareReplication(ctx context.Context, cl *goproxmox.APIClient, node string, name string, vmID int) (int, error) {
	vmr, err := cl.GetVMByFilter(ctx, func(r *proxmox.ClusterResource) (bool, error) {
		return r.Name == name, nil
	})
	if err != nil || vmr.VMID == 0 {
		id, err := cl.GetNextID(ctx, vmID+1)
		if err != nil {
			return 0, err
		}

		vm := DefaultVMConfig()
		vm["name"] = name
		vm["vmid"] = id

		mc := metrics.NewMetricContext("createVm")
		if err = cl.CreateVM(ctx, node, vm); mc.ObserveRequest(err) != nil {
			return 0, err
		}

		return id, nil
	}

	return int(vmr.VMID), nil
}

// CreateReplication sets up volume replication across zones.
func CreateReplication(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume, schedule string, replicateZones string) error {
	cfg := map[string]string{
		"replicate": "1",
		"backup":    "1",
	}
	if _, err := AttachVolume(ctx, cl, id, vol, cfg); err != nil {
		return err
	}

	sched := "*/15"
	if schedule != "" {
		sched = schedule
	}

	for i, z := range strings.Split(replicateZones, ",") {
		if z == vol.Node() {
			continue
		}

		repParams := map[string]interface{}{
			"id":       fmt.Sprintf("%d-%d", id, i),
			"type":     "local",
			"disable":  "0",
			"target":   z,
			"schedule": sched,
			"comment":  "CSI Replication for Persistent Volume",
		}

		if err := cl.Client.Post(ctx, "/cluster/replication", repParams, nil); err != nil {
			return fmt.Errorf("failed to create replication: %v, repParams=%+v", err, repParams)
		}
	}

	return nil
}

// MigrateReplication migrates the replication VM to the target node.
func MigrateReplication(ctx context.Context, cl *goproxmox.APIClient, target int, vol *volume.Volume, vmID int) error {
	volid, err := strconv.Atoi(vol.VMID())
	if err != nil {
		return fmt.Errorf("failed to parse volumeID %s: %v", vol.VolumeID(), err)
	}

	if volid == vmID {
		return nil
	}

	sourceVM, err := cl.GetVMByID(ctx, uint64(volid))
	if err != nil {
		return fmt.Errorf("failed to find vm by id %d: %v", volid, err)
	}

	targetVM, err := cl.GetVMByID(ctx, uint64(target))
	if err != nil {
		return fmt.Errorf("failed to find vm by id %d: %v", target, err)
	}

	if sourceVM.Node == targetVM.Node {
		return nil
	}

	n, err := cl.Node(ctx, sourceVM.Node)
	if err != nil {
		return fmt.Errorf("unable to find node with name %s: %w", sourceVM.Node, err)
	}

	vm, err := n.VirtualMachine(ctx, volid)
	if err != nil {
		return fmt.Errorf("unable to find vm with id %d: %w", volid, err)
	}

	params := &proxmox.VirtualMachineMigrateOptions{
		Target: targetVM.Node,
		Online: false,
	}

	task, err := vm.Migrate(ctx, params)
	if err != nil {
		return fmt.Errorf("failed to migrate vm config: %v", err)
	}

	if task != nil {
		if err = task.WaitFor(ctx, 5*60); err != nil {
			return fmt.Errorf("unable to migrate virtual machine: %w", err)
		}

		if task.IsFailed {
			return fmt.Errorf("unable to migrate virtual machine: %s", task.ExitStatus)
		}
	}

	return nil
}

// DeleteReplication removes the replication configuration and VM.
func DeleteReplication(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume, vmID int) error {
	id, err := strconv.Atoi(vol.VMID())
	if err != nil {
		return fmt.Errorf("failed to parse volumeID %s: %v", vol.VolumeID(), err)
	}

	if id != vmID {
		vmr, err := cl.GetVMByFilter(ctx, func(r *proxmox.ClusterResource) (bool, error) {
			return r.VMID == uint64(id) && r.Name == vol.PV(), nil
		})
		if err != nil {
			return err
		}

		type VirtualMachineReplicationJobs struct {
			ID    string `json:"id"`
			Guest int    `json:"guest"`
		}

		jobs := []VirtualMachineReplicationJobs{}

		if err := cl.Get(ctx, fmt.Sprintf("/nodes/%s/replication?guest=%d", vmr.Node, vmr.VMID), &jobs); err != nil {
			return fmt.Errorf("could not get replication list: %w", err)
		}

		for _, job := range jobs {
			if err := cl.Client.Delete(ctx, fmt.Sprintf("/cluster/replication/%s", job.ID), nil); err != nil {
				if !strings.Contains(err.Error(), "no such job") {
					return fmt.Errorf("failed to delete replication schedule: %v", err)
				}
			}
		}

		err = cl.DeleteVMByID(ctx, vmr.Node, int(vmr.VMID))
		if err != nil {
			return fmt.Errorf("failed to delete replication vm: %v", err)
		}
	}

	return nil
}

// DefaultVMConfig returns the minimal configuration for a replication placeholder VM.
func DefaultVMConfig() map[string]interface{} {
	return map[string]interface{}{
		"boot":    "order=scsi0",
		"agent":   "0",
		"machine": "pc",
		"cores":   "1",
		"memory":  "512",
		"scsihw":  "virtio-scsi-single",
	}
}
