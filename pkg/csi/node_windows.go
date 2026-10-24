//go:build windows

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

// Windows node service.
//
// The node runs as a HostProcess container and drives the Windows Storage cmdlets
// through powershell.exe, so no csi-proxy is needed on the host. Every value reaches a
// script through an environment variable, never through the script text.
//
// A volume is one GPT disk with one NTFS partition. NodeStageVolume mounts the partition
// on the staging directory as an NTFS mount point (Add-PartitionAccessPath) and
// NodePublishVolume links the pod's target path to it with a directory symlink, which
// is how kubelet expects a Windows CSI driver to publish.
//
// Windows Server brings every new SCSI disk up offline (SAN policy "Offline Shared"),
// so staging puts the disk online and unstaging takes it offline again before the
// controller unplugs it.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-lib-utils/protosanitizer"
	"golang.org/x/sys/windows"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/utils/keymutex"
)

const (
	// windowsDiskWait is how long staging waits for a hot-plugged disk to appear in the
	// guest. The controller only waits for the Proxmox config, not for the guest.
	windowsDiskWait = 60 * time.Second

	// exit code of a script that did not find the disk it was asked for.
	psExitNotFound = 3
)

// NodeService is the node service for the CSI driver
type NodeService struct {
	csi.UnimplementedNodeServer

	nodeID  string
	kclient kubernetes.Interface

	// volumeLocks serializes operations on the SAME volume only.
	volumeLocks keymutex.KeyMutex
}

// NewNodeService returns a new NodeService
func NewNodeService(nodeID string, clientSet kubernetes.Interface) *NodeService {
	return &NodeService{
		nodeID:      nodeID,
		kclient:     clientSet,
		volumeLocks: keymutex.NewHashed(64),
	}
}

// SetKataDirectVolumes is not supported on Windows; Kata Containers runs on Linux only.
func (n *NodeService) SetKataDirectVolumes(cfg KataDirectVolumes) {
	if cfg.Enabled {
		klog.InfoS("Kata direct-assigned volumes are not supported on Windows nodes, ignored")
	}
}

type winPartition struct {
	PartitionNumber int
	Offset          int64
	Size            int64
	Type            string
	AccessPaths     []string
	FileSystem      string
}

type winDisk struct {
	Number         int
	PartitionStyle string
	Partitions     []winPartition
}

// dataPartition is the partition holding the volume's filesystem: "Basic" on GPT, "IFS"
// on MBR. The Microsoft Reserved partition Windows adds to GPT disks is skipped.
func (d *winDisk) dataPartition() *winPartition {
	for i := range d.Partitions {
		switch d.Partitions[i].Type {
		case "Basic", "IFS":
			return &d.Partitions[i]
		}
	}

	return nil
}

// onlyReservedPartitions reports whether the disk holds nothing but partitions Windows
// creates by itself, which is the state Initialize-Disk leaves a disk in.
func (d *winDisk) onlyReservedPartitions() bool {
	for _, p := range d.Partitions {
		if p.Type != "Reserved" {
			return false
		}
	}

	return true
}

// NodeStageVolume is called by the CO when a workload that wants to use the specified volume is placed (scheduled) on a node.
//
//nolint:cyclop,gocyclo
func (n *NodeService) NodeStageVolume(ctx context.Context, request *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	klog.V(4).InfoS("NodeStageVolume: called", "args", protosanitizer.StripSecrets(request))

	volumeID := request.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "VolumeID must be provided")
	}

	stagingTarget := request.GetStagingTargetPath()
	if len(stagingTarget) == 0 {
		return nil, status.Error(codes.InvalidArgument, "StagingTargetPath must be provided")
	}

	publishContext := request.GetPublishContext()
	if len(publishContext) == 0 {
		return nil, status.Error(codes.InvalidArgument, "PublishContext must be provided")
	}

	volumeCapability := request.GetVolumeCapability()
	if volumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "VolumeCapability must be provided")
	}

	if volumeCapability.GetBlock() != nil {
		return nil, status.Error(codes.InvalidArgument, "raw block volumes are not supported on Windows nodes")
	}

	if _, ok := request.GetSecrets()[EncryptionPassphraseKey]; ok {
		return nil, status.Error(codes.InvalidArgument, "encrypted volumes are not supported on Windows nodes")
	}

	if _, err := windowsFSType(volumeCapability.GetMount().GetFsType()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	params, err := ExtractParameters(request.GetVolumeContext())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	usersWritable, err := windowsUsersWritable(params.RootDirPermissions)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	diskID, err := diskUniqueIDFromPublishContext(publishContext)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	n.volumeLocks.LockKey(volumeID)
	defer n.volumeLocks.UnlockKey(volumeID) //nolint:errcheck

	disk, err := waitForDisk(ctx, diskID)
	if err != nil {
		return nil, err
	}

	part, err := ensurePartition(ctx, diskID, disk)
	if err != nil {
		// kubelet does not unstage a volume that never staged; hand a refused disk back
		// offline, the way it arrived.
		if status.Code(err) == codes.FailedPrecondition {
			if _, oerr := runPowerShell(ctx, `Set-Disk -Number $env:CSI_DISK -IsOffline $true`, "CSI_DISK="+strconv.Itoa(disk.Number)); oerr != nil {
				klog.ErrorS(oerr, "NodeStageVolume: failed to take refused disk offline", "disk", disk.Number)
			}
		}

		return nil, err
	}

	mountPoint := accessPath(stagingTarget)
	if !containsFold(part.AccessPaths, mountPoint) {
		if err := emptyDir(stagingTarget); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}

		if _, err := runPowerShell(ctx,
			`Add-PartitionAccessPath -DiskNumber $env:CSI_DISK -PartitionNumber $env:CSI_PARTITION -AccessPath $env:CSI_PATH`,
			"CSI_DISK="+strconv.Itoa(disk.Number), "CSI_PARTITION="+strconv.Itoa(part.PartitionNumber), "CSI_PATH="+mountPoint,
		); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to mount disk %d partition %d at %s: %v", disk.Number, part.PartitionNumber, stagingTarget, err)
		}
	}

	if usersWritable {
		if err := grantUsersModify(ctx, stagingTarget); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to set root dir permissions on %s: %v", stagingTarget, err)
		}
	}

	requiredResize, _ := strconv.ParseBool(publishContext[resizeRequired]) // nolint:errcheck
	if requiredResize {
		if err := resizePartition(ctx, stagingTarget); err != nil {
			return nil, status.Errorf(codes.Internal, "Could not resize volume %q: %v", volumeID, err)
		}
	}

	klog.V(3).InfoS("NodeStageVolume: volume mounted", "disk", disk.Number, "uniqueID", diskID, "path", stagingTarget, "resized", requiredResize)

	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume is called by the CO when a workload that was using the specified volume is being moved to a different node.
func (n *NodeService) NodeUnstageVolume(ctx context.Context, request *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	klog.V(4).InfoS("NodeUnstageVolume: called", "args", protosanitizer.StripSecrets(request))

	volumeID := request.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "VolumeID must be provided")
	}

	stagingTargetPath := request.GetStagingTargetPath()
	if len(stagingTargetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "StagingTargetPath must be provided")
	}

	n.volumeLocks.LockKey(volumeID)
	defer n.volumeLocks.UnlockKey(volumeID) //nolint:errcheck

	// Flush, unmount, and take the disk offline once nothing else on it is mounted, so the
	// guest has let go of it before the controller unplugs it. NTFS on a disk that
	// advertises UNMAP returns freed space online, so no trim pass is needed here.
	out, err := runPowerShell(ctx, `
$p = Get-Partition | Where-Object { $_.AccessPaths -contains $env:CSI_PATH } | Select-Object -First 1
if (-not $p) { exit 0 }
$p | Get-Volume | Write-VolumeCache
$p | Remove-PartitionAccessPath -AccessPath $env:CSI_PATH
$mounted = Get-Partition -DiskNumber $p.DiskNumber | ForEach-Object { $_.AccessPaths } | Where-Object { $_ -and $_ -notlike '\\?\*' }
if (-not $mounted) { Set-Disk -Number $p.DiskNumber -IsOffline $true }
"disk $($p.DiskNumber)"`,
		"CSI_PATH="+accessPath(stagingTargetPath),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Unmount of targetPath %s failed with error %v", stagingTargetPath, err)
	}

	if err := os.Remove(stagingTargetPath); err != nil && !os.IsNotExist(err) {
		klog.InfoS("NodeUnstageVolume: failed to remove staging directory, ignored", "path", stagingTargetPath, "err", err)
	}

	klog.V(3).InfoS("NodeUnstageVolume: volume unmounted", "path", stagingTargetPath, "device", out)

	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume mounts the volume on the node.
func (n *NodeService) NodePublishVolume(_ context.Context, request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	klog.V(4).InfoS("NodePublishVolume: called", "args", protosanitizer.StripSecrets(request))

	stagingTargetPath := request.GetStagingTargetPath()
	if len(stagingTargetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "StagingTargetPath must be provided")
	}

	targetPath := request.GetTargetPath()
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "TargetPath must be provided")
	}

	volumeCapability := request.GetVolumeCapability()
	if volumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "VolumeCapability must be provided")
	}

	if !isValidVolumeCapabilities([]*csi.VolumeCapability{volumeCapability}) {
		return nil, status.Error(codes.InvalidArgument, "VolumeCapability not supported")
	}

	if volumeCapability.GetBlock() != nil {
		return nil, status.Error(codes.InvalidArgument, "raw block volumes are not supported on Windows nodes")
	}

	if _, err := os.Stat(stagingTargetPath); err != nil {
		return nil, status.Errorf(codes.NotFound, "staging path %s is not available: %v", stagingTargetPath, err)
	}

	if request.GetReadonly() {
		klog.V(3).InfoS("NodePublishVolume: read-only publish is not enforced on Windows nodes", "path", targetPath)
	}

	if dest, err := os.Readlink(targetPath); err == nil {
		if strings.EqualFold(filepath.Clean(dest), filepath.Clean(stagingTargetPath)) {
			return &csi.NodePublishVolumeResponse{}, nil
		}

		if err := os.Remove(targetPath); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to remove stale link %s: %v", targetPath, err)
		}
	} else if err := removeEmptyDir(targetPath); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := os.MkdirAll(filepath.Dir(targetPath), 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "Could not create dir %q: %v", filepath.Dir(targetPath), err)
	}

	if err := os.Symlink(stagingTargetPath, targetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "Could not link %q to %q: %v", targetPath, stagingTargetPath, err)
	}

	klog.V(3).InfoS("NodePublishVolume: volume published for pod", "path", targetPath,
		"pod", klog.KRef(request.GetVolumeContext()["csi.storage.k8s.io/pod.namespace"], request.GetVolumeContext()["csi.storage.k8s.io/pod.name"]),
	)

	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmount the volume from the target path
func (n *NodeService) NodeUnpublishVolume(_ context.Context, request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	klog.V(4).InfoS("NodeUnpublishVolume: called", "args", protosanitizer.StripSecrets(request))

	targetPath := request.GetTargetPath()
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "TargetPath must be provided")
	}

	if _, err := os.Readlink(targetPath); err == nil {
		if err := os.Remove(targetPath); err != nil {
			return nil, status.Errorf(codes.Internal, "Unmount of targetpath %s failed with error %v", targetPath, err)
		}
	} else if err := removeEmptyDir(targetPath); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.V(3).InfoS("NodeUnpublishVolume: volume unpublished", "path", targetPath)

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetVolumeStats get the volume stats
func (n *NodeService) NodeGetVolumeStats(_ context.Context, request *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	klog.V(5).InfoS("NodeGetVolumeStats: called", "args", protosanitizer.StripSecrets(request))

	volumePath := request.GetVolumePath()
	if len(volumePath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "VolumePath must be provided")
	}

	if _, err := os.Stat(volumePath); err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, "target: %s not found", volumePath)
		}

		return nil, status.Errorf(codes.Internal, "failed to check whether volumePath exists: %s", err)
	}

	dir, err := windows.UTF16PtrFromString(accessPath(volumePath))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var available, total, free uint64
	if err := windows.GetDiskFreeSpaceEx(dir, &available, &total, &free); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get stats by path: %s", err)
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{Total: int64(total), Available: int64(available), Used: int64(total - free), Unit: csi.VolumeUsage_BYTES}, //nolint:gosec
		},
	}, nil
}

// NodeExpandVolume expand the volume
func (n *NodeService) NodeExpandVolume(ctx context.Context, request *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	klog.V(4).InfoS("NodeExpandVolume: called", "args", protosanitizer.StripSecrets(request))

	volumeID := request.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "VolumeID must be provided")
	}

	volumePath := request.GetVolumePath()
	if len(volumePath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "VolumePath must be provided")
	}

	if request.GetVolumeCapability().GetBlock() != nil {
		return &csi.NodeExpandVolumeResponse{}, nil
	}

	// The partition is mounted on the staging path; a published path is a link to it.
	mountPath := request.GetStagingTargetPath()
	if mountPath == "" {
		mountPath = volumePath
		if dest, err := os.Readlink(volumePath); err == nil {
			mountPath = dest
		}
	}

	if err := resizePartition(ctx, mountPath); err != nil {
		return nil, status.Errorf(codes.Internal, "Could not resize volume %q: %v", volumeID, err)
	}

	klog.V(3).InfoS("NodeExpandVolume: resized volume", "path", mountPath)

	return &csi.NodeExpandVolumeResponse{}, nil
}

// waitForDisk finds the disk carrying diskID, brings it online and writable, and returns
// its layout. A disk the controller just hot-plugged can take a moment to enumerate.
func waitForDisk(ctx context.Context, diskID string) (*winDisk, error) {
	deadline := time.Now().Add(windowsDiskWait)

	for {
		disk, err := getDisk(ctx, diskID)
		if err == nil {
			return disk, nil
		}

		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != psExitNotFound {
			return nil, status.Errorf(codes.Internal, "failed to read disk %s: %v", diskID, err)
		}

		if time.Now().After(deadline) {
			return nil, status.Errorf(codes.NotFound, "disk with UniqueId %s did not appear on the node", diskID)
		}

		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func getDisk(ctx context.Context, diskID string) (*winDisk, error) {
	out, err := runPowerShell(ctx, `
$d = Get-Disk | Where-Object { $_.UniqueId -eq $env:CSI_DISK_ID } | Select-Object -First 1
if (-not $d) { Update-HostStorageCache; exit `+strconv.Itoa(psExitNotFound)+` }
if ($d.IsOffline) { Set-Disk -Number $d.Number -IsOffline $false }
if ($d.IsReadOnly) { Set-Disk -Number $d.Number -IsReadOnly $false }
$d = Get-Disk -Number $d.Number
$parts = @(Get-Partition -DiskNumber $d.Number -ErrorAction SilentlyContinue | ForEach-Object {
  $v = $_ | Get-Volume -ErrorAction SilentlyContinue
  [pscustomobject]@{
    PartitionNumber = $_.PartitionNumber; Offset = [int64]$_.Offset; Size = [int64]$_.Size; Type = [string]$_.Type
    AccessPaths = @($_.AccessPaths | Where-Object { $_ }); FileSystem = [string]$v.FileSystem
  }
})
[pscustomobject]@{ Number = $d.Number; PartitionStyle = [string]$d.PartitionStyle; Partitions = $parts } | ConvertTo-Json -Depth 4 -Compress`,
		"CSI_DISK_ID="+diskID,
	)
	if err != nil {
		return nil, err
	}

	disk := &winDisk{}
	if err := json.Unmarshal([]byte(out), disk); err != nil {
		return nil, fmt.Errorf("failed to parse disk layout %q: %v", out, err)
	}

	return disk, nil
}

// ensurePartition returns the disk's NTFS data partition, creating and formatting it on
// a new disk. Anything it cannot recognize as empty is refused, never formatted: a disk
// a Linux node formatted carries its filesystem without a partition table and looks
// exactly like a new disk to Windows.
func ensurePartition(ctx context.Context, diskID string, disk *winDisk) (*winPartition, error) {
	part := disk.dataPartition()

	if part == nil {
		switch {
		case disk.PartitionStyle == "RAW":
			if err := requireZeroed(disk.Number, 0); err != nil {
				return nil, err
			}

			if _, err := runPowerShell(ctx, `Initialize-Disk -Number $env:CSI_DISK -PartitionStyle GPT`, "CSI_DISK="+strconv.Itoa(disk.Number)); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to initialize disk %d: %v", disk.Number, err)
			}
		case disk.PartitionStyle == "GPT" && disk.onlyReservedPartitions():
		default:
			return nil, status.Errorf(codes.FailedPrecondition, "disk %d (%s) has no data partition Windows can use; refusing to repartition it", disk.Number, disk.PartitionStyle)
		}

		if _, err := runPowerShell(ctx, `New-Partition -DiskNumber $env:CSI_DISK -UseMaximumSize | Out-Null`, "CSI_DISK="+strconv.Itoa(disk.Number)); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create a partition on disk %d: %v", disk.Number, err)
		}

		var err error
		if disk, err = getDisk(ctx, diskID); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to read disk %s: %v", diskID, err)
		}

		if part = disk.dataPartition(); part == nil {
			return nil, status.Errorf(codes.Internal, "disk %d has no data partition after creating one", disk.Number)
		}
	}

	switch {
	case part.FileSystem == "":
		if err := requireZeroed(disk.Number, part.Offset); err != nil {
			return nil, err
		}

		if _, err := runPowerShell(ctx,
			`Format-Volume -Partition (Get-Partition -DiskNumber $env:CSI_DISK -PartitionNumber $env:CSI_PARTITION) -FileSystem NTFS -Confirm:$false | Out-Null`,
			"CSI_DISK="+strconv.Itoa(disk.Number), "CSI_PARTITION="+strconv.Itoa(part.PartitionNumber),
		); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to format disk %d partition %d: %v", disk.Number, part.PartitionNumber, err)
		}

		klog.V(3).InfoS("NodeStageVolume: formatted volume", "disk", disk.Number, "partition", part.PartitionNumber, "fsType", FSTypeNTFS)
	case !strings.EqualFold(part.FileSystem, FSTypeNTFS):
		return nil, status.Errorf(codes.FailedPrecondition, "disk %d partition %d holds %s, only NTFS is supported", disk.Number, part.PartitionNumber, part.FileSystem)
	}

	return part, nil
}

// requireZeroed refuses to write a partition table or filesystem over data: the first
// windowsBlankProbeBytes at offset must read back as zeros.
func requireZeroed(diskNumber int, offset int64) error {
	f, err := os.Open(`\\.\PhysicalDrive` + strconv.Itoa(diskNumber))
	if err != nil {
		return status.Errorf(codes.Internal, "failed to open disk %d: %v", diskNumber, err)
	}
	defer f.Close() //nolint:errcheck

	buf := make([]byte, windowsBlankProbeBytes)

	cnt, err := f.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return status.Errorf(codes.Internal, "failed to read disk %d: %v", diskNumber, err)
	}

	if !isZeroed(buf[:cnt]) {
		return status.Errorf(codes.FailedPrecondition,
			"disk %d holds data Windows cannot read (a filesystem from a Linux node?); refusing to format it", diskNumber)
	}

	return nil
}

// grantUsersModify lets BUILTIN\Users modify everything on the volume mounted at
// mountPath (see windowsUsersWritable). The ACL is read and written through the volume
// path: the mount point directory has an ACL of its own.
func grantUsersModify(ctx context.Context, mountPath string) error {
	_, err := runPowerShell(ctx, `
$p = Get-Partition | Where-Object { $_.AccessPaths -contains $env:CSI_PATH } | Select-Object -First 1
if (-not $p) { throw "no partition is mounted at $($env:CSI_PATH)" }
$root = $p.AccessPaths | Where-Object { $_ -like '\\?\*' } | Select-Object -First 1
$acl = Get-Acl -LiteralPath $root
$users = New-Object System.Security.Principal.SecurityIdentifier('S-1-5-32-545')
$rule = New-Object System.Security.AccessControl.FileSystemAccessRule($users, 'Modify', 'ContainerInherit,ObjectInherit', 'None', 'Allow')
$acl.AddAccessRule($rule)
Set-Acl -LiteralPath $root -AclObject $acl`,
		"CSI_PATH="+accessPath(mountPath),
	)

	return err
}

// resizePartition grows the partition mounted at mountPath to the end of its disk.
func resizePartition(ctx context.Context, mountPath string) error {
	_, err := runPowerShell(ctx, `
Update-HostStorageCache
$p = Get-Partition | Where-Object { $_.AccessPaths -contains $env:CSI_PATH } | Select-Object -First 1
if (-not $p) { throw "no partition is mounted at $($env:CSI_PATH)" }
$max = ($p | Get-PartitionSupportedSize).SizeMax
if ($max - $p.Size -gt 1MB) { $p | Resize-Partition -Size $max }`,
		"CSI_PATH="+accessPath(mountPath),
	)

	return err
}

// runPowerShell runs script and returns its standard output. Inputs are passed as
// NAME=value environment variables and read in the script as $env:NAME.
func runPowerShell(ctx context.Context, script string, env ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command",
		"$ErrorActionPreference = 'Stop'; $ProgressPreference = 'SilentlyContinue'; "+script)

	cmd.Env = append(os.Environ(), env...)

	var stdout, stderr bytes.Buffer

	cmd.Stdout, cmd.Stderr = &stdout, &stderr

	if err := cmd.Run(); err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return "", fmt.Errorf("%w: %s", err, msg)
		}

		return "", err
	}

	return strings.TrimSpace(stdout.String()), nil
}

// accessPath is how Get-Partition lists p as a mount point. kubelet hands out paths
// without a drive letter (\var\lib\kubelet\...), Windows records them with one.
func accessPath(p string) string {
	if abs, err := filepath.Abs(p); err == nil {
		p = abs
	}

	return windowsAccessPath(p)
}

// emptyDir makes sure p is an empty directory, which is what an NTFS mount point needs.
func emptyDir(p string) error {
	if err := os.MkdirAll(p, 0o750); err != nil {
		return fmt.Errorf("failed to create directory %s: %v", p, err)
	}

	entries, err := os.ReadDir(p)
	if err != nil {
		return fmt.Errorf("failed to read directory %s: %v", p, err)
	}

	if len(entries) > 0 {
		return fmt.Errorf("directory %s is not empty, refusing to mount over it", p)
	}

	return nil
}

// removeEmptyDir removes p if it is an empty directory, and does nothing if p does not
// exist. A non-empty directory is an error: it is not ours to delete.
func removeEmptyDir(p string) error {
	fi, err := os.Lstat(p)
	if os.IsNotExist(err) {
		return nil
	}

	if err != nil {
		return err
	}

	if !fi.IsDir() {
		return fmt.Errorf("%s exists and is not a directory", p)
	}

	if err := os.Remove(p); err != nil {
		return fmt.Errorf("failed to remove %s: %v", p, err)
	}

	return nil
}

func containsFold(list []string, s string) bool {
	for _, v := range list {
		if strings.EqualFold(v, s) {
			return true
		}
	}

	return false
}
