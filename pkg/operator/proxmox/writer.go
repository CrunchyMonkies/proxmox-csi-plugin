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
	"fmt"
	"strconv"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	toolsproxmox "github.com/sergelogvinov/proxmox-csi-plugin/pkg/tools/proxmox"
	volume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"
)

// AttachResult is what AttachDisk returns: the device path the node plugin
// resolves to, the lun index, and whether the volume was smaller than the
// requested capacity and needs an online resize after attach.
type AttachResult struct {
	// DevicePath is the /dev/disk/by-id/wwn-0x... path.
	DevicePath string
	// Lun is the SCSI lun number (1-29).
	Lun int
}

// Writer mutates Proxmox disks on behalf of the volume reconcilers.
//
// Deliberately a separate type from Pool: the readers are handed to every
// reconciler that needs to look at Proxmox, while the writer is constructed
// explicitly and passed only to the reconcilers that are allowed to write.
// That split is what lets "the adoption reconciler issues zero writes" be
// checked by looking at one constructor argument rather than by reading every
// line of it.
type Writer interface {
	// CreateDisk creates a new disk on the given storage.
	CreateDisk(ctx context.Context, region, zone, storage, diskName string, sizeBytes int64) error

	// CopyDisk copies a source disk to a destination, used for snapshot
	// creation and restore-from-snapshot.
	CopyDisk(ctx context.Context, srcRegion, srcZone, srcStorage, srcDisk string,
		dstZone, dstStorage, dstDisk string) error

	// DeleteDisk removes a disk from the storage.
	DeleteDisk(ctx context.Context, region, zone, storage, disk string) error

	// AttachDisk attaches a disk to a VM and returns the device path and lun.
	// options are the Proxmox disk configuration options (backup, iothread, etc.).
	AttachDisk(ctx context.Context, region string, vmid int, vol *volume.Volume, options map[string]string) (AttachResult, error)

	// DetachDisk detaches a disk from a VM and waits for PVE to confirm
	// the detachment.
	DetachDisk(ctx context.Context, region string, vmid int, vol *volume.Volume) error

	// ResizeDisk expands a disk attached to a VM.
	ResizeDisk(ctx context.Context, region string, vmid int, zone, device, size string) error

	// UpdateDisk changes the options on a disk attached to a VM.
	UpdateDisk(ctx context.Context, region string, vmid int, vol *volume.Volume, options map[string]string) error

	// RenameDisk reassigns an unattached volume to a different VMID.
	RenameDisk(ctx context.Context, region string, vol *volume.Volume, targetVMID int) (*volume.Volume, error)

	// ClearUnusedDisk removes the unused<n> key that detaching a volume leaves
	// behind on the VM config. Must be called AFTER a rename that moved the
	// volume out from under the unused entry.
	ClearUnusedDisk(ctx context.Context, region string, vmid int, vol *volume.Volume) error
}

// DiskWriter implements Writer over the driver's Proxmox client pool.
type DiskWriter struct {
	pool *proxmoxpool.ProxmoxPool
}

// NewDiskWriter creates a Writer that writes to Proxmox through the given pool.
func NewDiskWriter(pool *proxmoxpool.ProxmoxPool) *DiskWriter {
	return &DiskWriter{pool: pool}
}

var _ Writer = (*DiskWriter)(nil)

// CreateDisk creates a new disk on Proxmox storage.
func (w *DiskWriter) CreateDisk(ctx context.Context, region, zone, storage, diskName string, sizeBytes int64) error {
	cl, err := w.pool.GetProxmoxCluster(region)
	if err != nil {
		return fmt.Errorf("region %s: %w", region, err)
	}

	vol := volume.NewVolume(region, zone, storage, diskName)

	return toolsproxmox.CreateVolume(ctx, cl, vol, sizeBytes)
}

// CopyDisk copies a source disk to a destination.
func (w *DiskWriter) CopyDisk(ctx context.Context, srcRegion, srcZone, srcStorage, srcDisk string,
	dstZone, dstStorage, dstDisk string,
) error {
	cl, err := w.pool.GetProxmoxCluster(srcRegion)
	if err != nil {
		return fmt.Errorf("region %s: %w", srcRegion, err)
	}

	srcVol := volume.NewVolume(srcRegion, srcZone, srcStorage, srcDisk)
	dstVol := volume.NewVolume(srcRegion, dstZone, dstStorage, dstDisk)

	endpoint := w.pool.CopyEndpoint(srcRegion, false, false)

	return toolsproxmox.CopyVolume(ctx, cl, srcVol, dstVol, endpoint)
}

// DeleteDisk removes a disk from Proxmox storage.
func (w *DiskWriter) DeleteDisk(ctx context.Context, region, zone, storage, disk string) error {
	cl, err := w.pool.GetProxmoxCluster(region)
	if err != nil {
		return fmt.Errorf("region %s: %w", region, err)
	}

	return cl.DeleteVMDisk(ctx, zone, storage, disk)
}

// AttachDisk attaches a disk to a VM.
func (w *DiskWriter) AttachDisk(ctx context.Context, region string, vmid int, vol *volume.Volume, options map[string]string) (AttachResult, error) {
	cl, err := w.pool.GetProxmoxCluster(region)
	if err != nil {
		return AttachResult{}, fmt.Errorf("region %s: %w", region, err)
	}

	pvInfo, err := toolsproxmox.AttachVolume(ctx, cl, vmid, vol, options)
	if err != nil {
		return AttachResult{}, err
	}

	lun := 0
	if lunStr, ok := pvInfo["lun"]; ok {
		lun, err = strconv.Atoi(lunStr)
		if err != nil {
			return AttachResult{}, fmt.Errorf("invalid lun %q: %w", lunStr, err)
		}
	}

	return AttachResult{
		DevicePath: pvInfo["DevicePath"],
		Lun:        lun,
	}, nil
}

// DetachDisk detaches a disk from a VM and waits for the detachment.
func (w *DiskWriter) DetachDisk(ctx context.Context, region string, vmid int, vol *volume.Volume) error {
	cl, err := w.pool.GetProxmoxCluster(region)
	if err != nil {
		return fmt.Errorf("region %s: %w", region, err)
	}

	if err := toolsproxmox.DetachVolume(ctx, cl, vmid, vol); err != nil {
		return err
	}

	return toolsproxmox.WaitDetachVolume(ctx, cl, vmid, vol)
}

// ResizeDisk expands a disk attached to a VM.
func (w *DiskWriter) ResizeDisk(ctx context.Context, region string, vmid int, zone, device, size string) error {
	cl, err := w.pool.GetProxmoxCluster(region)
	if err != nil {
		return fmt.Errorf("region %s: %w", region, err)
	}

	return cl.ResizeVMDisk(ctx, vmid, zone, device, size)
}

// UpdateDisk changes the options on a disk attached to a VM.
func (w *DiskWriter) UpdateDisk(ctx context.Context, region string, vmid int, vol *volume.Volume, options map[string]string) error {
	cl, err := w.pool.GetProxmoxCluster(region)
	if err != nil {
		return fmt.Errorf("region %s: %w", region, err)
	}

	return toolsproxmox.UpdateVolume(ctx, cl, vmid, vol, options)
}

// RenameDisk reassigns an unattached volume to a different VMID.
func (w *DiskWriter) RenameDisk(ctx context.Context, region string, vol *volume.Volume, targetVMID int) (*volume.Volume, error) {
	cl, err := w.pool.GetProxmoxCluster(region)
	if err != nil {
		return nil, fmt.Errorf("region %s: %w", region, err)
	}

	return toolsproxmox.RenameVolume(ctx, cl, vol, targetVMID)
}

// ClearUnusedDisk removes the unused<n> key that detaching a volume leaves behind.
func (w *DiskWriter) ClearUnusedDisk(ctx context.Context, region string, vmid int, vol *volume.Volume) error {
	cl, err := w.pool.GetProxmoxCluster(region)
	if err != nil {
		return fmt.Errorf("region %s: %w", region, err)
	}

	return toolsproxmox.ClearUnusedDisk(ctx, cl, vmid, vol)
}
