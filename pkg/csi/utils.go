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

package csi

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
	toolsproxmox "github.com/sergelogvinov/proxmox-csi-plugin/pkg/tools/proxmox"
	volume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"

	"k8s.io/klog/v2"
)

const (
	// TaskStatusCheckInterval is the interval in seconds to check the status of a task
	TaskStatusCheckInterval = 5
	// TaskTimeout is the timeout in seconds for all task
	TaskTimeout = 30

	// copyTaskTimeout bounds how long a snapshot copy (or restore-from-snapshot)
	// waits for its Proxmox task. Copying a whole volume between storages is
	// orders of magnitude slower than the config-level operations TaskTimeout is
	// sized for, so it gets its own constant: 1 hour, matching the 240 polls at
	// 15s this code waited before it was routed through MoveQemuDisk.
	copyTaskTimeout = 3600

	// ErrorNotFound not found error message
	ErrorNotFound string = "not found"
)

// errStorageNotFound is ErrorNotFound raised because the storage itself is absent
// from the node, rather than because the volume is missing from a storage that
// exists. Its message is deliberately ErrorNotFound so the string comparisons
// callers make keep working unchanged; resolveVolume tells the two apart with
// errors.Is, because searching a storage that does not exist can only produce
// noise — PVE answers a content listing on an unknown storage with a 500.
var errStorageNotFound = errors.New(ErrorNotFound)

// nolint:unused
func getNodeForVolume(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume) (node string, err error) {
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

// getVMByAttachedVolume finds the VM whose config references vol, skipping
// skipVMID — the controller's placeholder VM, which owns CSI volumes at rest and
// is never a publish target.
//
// skipVMID is passed in rather than read off the volume: with
// reassignVolumeOnAttach on, an attached volume is named for the VM holding it, so
// deriving the id to skip from the name would skip exactly the VM being looked for.
func getVMByAttachedVolume(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume, skipVMID int) (int, int, error) {
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

		vm, err := cl.GetVMConfig(ctx, int(rs.VMID))
		if err != nil {
			return false, err
		}

		if l, exist := isVolumeAttached(vm.VirtualMachineConfig, volumeMatch(vol)); exist {
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

func getStorageContent(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume) (*proxmox.StorageContent, error) {
	if vol.Node() == "" {
		return nil, errors.New("node is required")
	}

	if _, err := cl.GetStorageStatus(ctx, vol.Node(), vol.Storage()); err != nil {
		if strings.Contains(err.Error(), "No such storage") {
			return nil, errStorageNotFound
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

func getStorageLevel(storage *proxmox.ClusterResource) string {
	// see https://pve.proxmox.com/wiki/Storage
	switch storage.PluginType {
	case "dir", "nfs", "cifs", "cephfs", "btrfs": // nolint: goconst
		return "file"
	default:
		return "block"
	}
}

func getVolumeSize(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume) (int64, error) {
	st, err := getStorageContent(ctx, cl, vol)
	if err != nil {
		return 0, err
	}

	if st == nil {
		return 0, errors.New(ErrorNotFound)
	}

	return int64(st.Size), nil
}

// volumeMatch returns the string isVolumeAttached should look for in a VM config.
//
// With features.reassignVolumeOnAttach on, an attached volume is named for the VM
// that owns it rather than for the vmid the PV's immutable volumeHandle carries,
// so matching on the full disk name would miss it. The suffix ('pvc-<uuid>.raw')
// is the part a rename leaves alone.
//
// Disks with no such suffix — anything not in Proxmox's 'vm-<vmid>-<name>' form —
// cannot be renamed at all, so their full name is already the stable one.
func volumeMatch(vol *volume.Volume) string {
	if suffix := vol.DiskSuffix(); suffix != "" {
		return suffix
	}

	return vol.Disk()
}

// resolveVolume returns vol under the name Proxmox currently stores it as.
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
func resolveVolume(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume, reassign bool) (*volume.Volume, int64, error) {
	size, err := getVolumeSize(ctx, cl, vol)
	if err == nil {
		return vol, size, nil
	}

	// The search runs only with the feature on. Its failure modes are not free: a
	// storage that has been removed from PVE answers a content listing with a 500,
	// which would turn what is otherwise a clean NotFound into an Internal error and
	// leave DeleteVolume retrying forever against a PV that can never be satisfied.
	// With the feature off no volume is ever on a second name, so there is nothing
	// to search for and the pre-existing behavior is kept exactly.
	if !reassign || err.Error() != ErrorNotFound || errors.Is(err, errStorageNotFound) || vol.DiskSuffix() == "" {
		return nil, 0, err
	}

	found, err := toolsproxmox.FindVolumeBySuffix(ctx, cl, vol)
	if err != nil {
		return nil, 0, err
	}

	if found == nil {
		return nil, 0, errors.New(ErrorNotFound)
	}

	size, err = getVolumeSize(ctx, cl, found)
	if err != nil {
		return nil, 0, err
	}

	klog.V(4).InfoS("resolveVolume: volume is on another vmid", "volumeID", vol.VolumeID(), "disk", found.Disk())

	return found, size, nil
}

func isVolumeAttached(vm *proxmox.VirtualMachineConfig, pvc string) (int, bool) {
	if pvc == "" {
		return 0, false
	}

	disks := vm.MergeSCSIs()
	for lun, disk := range disks {
		if strings.Contains(disk, pvc) {
			i, err := strconv.Atoi(strings.TrimPrefix(strings.Split(lun, ":")[0], deviceNamePrefix))
			if err != nil {
				return 0, false
			}

			return i, true
		}
	}

	return 0, false
}

func prepareReplication(ctx context.Context, cl *goproxmox.APIClient, node string, name string, vmID int) (int, error) {
	vmr, err := cl.GetVMByFilter(ctx, func(r *proxmox.ClusterResource) (bool, error) {
		return r.Name == name, nil
	})
	if err != nil || vmr.VMID == 0 {
		id, err := cl.GetNextID(ctx, vmID+1)
		if err != nil {
			return 0, err
		}

		vm := defaultVMConfig()
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

func createReplication(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume, params StorageParameters) error {
	cfg := map[string]string{
		"replicate": "1",
		"backup":    "1",
	}
	if _, err := attachVolume(ctx, cl, id, vol, cfg); err != nil {
		return err
	}

	schedule := "*/15"
	if params.ReplicateSchedule != "" {
		schedule = params.ReplicateSchedule
	}

	for i, z := range strings.Split(params.ReplicateZones, ",") {
		if z == vol.Node() {
			continue
		}

		repParams := map[string]interface{}{
			"id":       fmt.Sprintf("%d-%d", id, i),
			"type":     "local",
			"disable":  "0",
			"target":   z,
			"schedule": schedule,
			"comment":  "CSI Replication for Persistent Volume",
		}

		if err := cl.Client.Post(ctx, "/cluster/replication", repParams, nil); err != nil {
			return fmt.Errorf("failed to create replication: %v, repParams=%+v", err, repParams)
		}
	}

	return nil
}

func migrateReplication(ctx context.Context, cl *goproxmox.APIClient, target int, vol *volume.Volume, vmID int) error {
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

func deleteReplication(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume, vmID int) error {
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

func createVolume(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume, sizeBytes int64) error {
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

func attachVolume(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume, options map[string]string) (map[string]string, error) {
	vm, err := cl.GetVMConfig(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get vm config: %v", err)
	}

	wwm := ""

	lun, exist := isVolumeAttached(vm.VirtualMachineConfig, volumeMatch(vol))
	if exist {
		wwm = hex.EncodeToString([]byte(fmt.Sprintf("PVC-ID%02d", lun)))
	} else {
		disks := vm.VirtualMachineConfig.MergeSCSIs()

		for lun = 1; lun < 30; lun++ {
			device := deviceNamePrefix + strconv.Itoa(lun)

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

				if err := task.WaitFor(ctx, 5*60); err != nil {
					return nil, fmt.Errorf("unable to attach virtual machine disk: %w", err)
				}

				if err := waitAttachVolume(ctx, cl, id, vol); err != nil {
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

func detachVolume(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume) error {
	vm, err := cl.GetVMConfig(ctx, id)
	if err != nil {
		if errors.Is(err, goproxmox.ErrVirtualMachineNotFound) {
			return nil
		}

		return fmt.Errorf("failed to get vm config: %v", err)
	}

	if lun, ok := isVolumeAttached(vm.VirtualMachineConfig, volumeMatch(vol)); ok {
		task, err := vm.UnlinkDisk(ctx, fmt.Sprintf("%s%d", deviceNamePrefix, lun), false)
		if err != nil {
			return fmt.Errorf("failed to unlink disk: %v", err)
		}

		if task != nil {
			if err := task.WaitFor(ctx, 5*60); err != nil {
				return fmt.Errorf("unable to detach virtual machine disk: %w", err)
			}
		}
	}

	return nil
}

// clearUnusedDisk removes the `unused<n>` key that detaching leaves behind for a
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
func clearUnusedDisk(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume) error {
	vm, err := cl.GetVMConfig(ctx, id)
	if err != nil {
		if errors.Is(err, goproxmox.ErrVirtualMachineNotFound) {
			return nil
		}

		return fmt.Errorf("failed to get vm config: %v", err)
	}

	match := volumeMatch(vol)

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

		if _, err := getVolumeSize(ctx, cl, unused); err == nil {
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

func updateVolume(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume, options map[string]string) error {
	vm, err := cl.GetVMConfig(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to get vm config: %v", err)
	}

	if lun, ok := isVolumeAttached(vm.VirtualMachineConfig, volumeMatch(vol)); ok {
		// The volid the VM config already carries, not one rebuilt from vol: with
		// reassignVolumeOnAttach on, an attached volume is named for this VM while
		// vol still carries the name from the PV's immutable volumeHandle, and
		// rebuilding would rewrite the config back to a name that does not exist.
		volid := fmt.Sprintf("%s:%s", vol.Storage(), vol.Disk())

		disks := vm.VirtualMachineConfig.MergeSCSIs()
		if disk := disks[deviceNamePrefix+strconv.Itoa(lun)]; disk != "" {
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
			Name:  deviceNamePrefix + strconv.Itoa(lun),
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

// copyVolume copies srcVol to destVol, used by snapshot creation and by
// restore-from-snapshot. endpoint selects the server-side copy implementation:
// the built-in content copy needs root@pam, the other two accept a scoped API
// token. See pkg/tools/proxmox.MoveQemuDisk for the per-endpoint request shapes
// and docs/volumesnapshot.md for the credentials each one needs.
func copyVolume(
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

	return toolsproxmox.MoveQemuDisk(ctx, cl, srcVol, node, destVol, copyTaskTimeout, endpoint)
}

func waitAttachVolume(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume) error {
	err := retry.Constant(TaskTimeout*time.Second, retry.WithUnits(TaskStatusCheckInterval*time.Second)).Retry(func() error {
		vm, err := cl.GetVMConfig(ctx, id)
		if err != nil {
			return fmt.Errorf("failed to get vm config: %v", err)
		}

		if _, ok := isVolumeAttached(vm.VirtualMachineConfig, volumeMatch(vol)); ok {
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

func waitDetachVolume(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume) error {
	err := retry.Constant(TaskTimeout*time.Second, retry.WithUnits(TaskStatusCheckInterval*time.Second)).Retry(func() error {
		vm, err := cl.GetVMConfig(ctx, id)
		if err != nil {
			if errors.Is(err, goproxmox.ErrVirtualMachineNotFound) {
				return nil
			}

			return fmt.Errorf("failed to get vm config: %v", err)
		}

		if _, ok := isVolumeAttached(vm.VirtualMachineConfig, volumeMatch(vol)); ok {
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

func defaultVMConfig() map[string]interface{} {
	return map[string]interface{}{
		"boot":    "order=scsi0",
		"agent":   "0",
		"machine": "pc",
		"cores":   "1",
		"memory":  "512",
		"scsihw":  "virtio-scsi-single",
	}
}
