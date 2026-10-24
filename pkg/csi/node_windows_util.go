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

// OS-independent pieces of the Windows node service, kept out of node_windows.go so
// they are unit tested on every platform.

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	// FSTypeNTFS is the only filesystem the Windows node formats and mounts.
	FSTypeNTFS = "ntfs"

	// windowsBlankProbeBytes is how much of a disk, or of a partition, must read back as
	// zeros before the Windows node partitions or formats it. A filesystem written by a
	// Linux node lives on the bare disk without a partition table, so Windows reports
	// such a disk as RAW, exactly like a new one. 1 MiB covers the GPT/MBR headers and the
	// ext4 (1 KiB), xfs (0), LUKS (0) and btrfs (64 KiB) superblocks.
	windowsBlankProbeBytes = 1 << 20

	devicePathWWNPrefix = "/dev/disk/by-id/wwn-0x"
)

// diskUniqueIDFromPublishContext returns the disk UniqueId Windows reports for the
// volume. The controller sets the SCSI WWN as `wwn=0x<hex>` on the Proxmox disk and
// publishes it as a Linux by-id path; Windows shows the same NAA identifier, upper-cased,
// as Get-Disk's UniqueId.
func diskUniqueIDFromPublishContext(publishContext map[string]string) (string, error) {
	devicePath := publishContext["DevicePath"]
	if devicePath == "" {
		return "", fmt.Errorf("DevicePath must be provided")
	}

	wwn, ok := strings.CutPrefix(devicePath, devicePathWWNPrefix)
	if !ok || wwn == "" {
		return "", fmt.Errorf("DevicePath %q does not carry a WWN", devicePath)
	}

	for _, c := range wwn {
		if !strings.ContainsRune("0123456789abcdefABCDEF", c) {
			return "", fmt.Errorf("DevicePath %q does not carry a hex WWN", devicePath)
		}
	}

	return strings.ToUpper(wwn), nil
}

// windowsFSType validates the requested filesystem for a Windows node. An empty type
// means the StorageClass did not ask for one.
func windowsFSType(fsType string) (string, error) {
	switch strings.ToLower(fsType) {
	case "", FSTypeNTFS:
		return FSTypeNTFS, nil
	default:
		return "", fmt.Errorf("filesystem %q is not supported on Windows nodes, use %q", fsType, FSTypeNTFS)
	}
}

// isZeroed reports whether buf holds only zero bytes.
func isZeroed(buf []byte) bool {
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}

	return true
}

// windowsAccessPath is how Get-Partition lists a directory mount point: the path with a
// trailing backslash.
func windowsAccessPath(p string) string {
	return strings.TrimRight(strings.ReplaceAll(p, "/", `\`), `\`) + `\`
}

// windowsUsersWritable maps the rootDirPermissions StorageClass parameter onto the volume
// root ACL. A new NTFS root lets BUILTIN\Users create folders but not files, which, like
// a root-owned 0755 directory on Linux, keeps a non-admin container user (nanoserver's
// ContainerUser) from writing. A mode that lets group or others write grants Users
// Modify instead; there are no owner or group bits to carry over.
func windowsUsersWritable(rootDirPermissions string) (bool, error) {
	if rootDirPermissions == "" {
		return false, nil
	}

	mode, err := strconv.ParseUint(rootDirPermissions, 8, 32)
	if err != nil {
		return false, fmt.Errorf("invalid rootDirPermissions %q: %v", rootDirPermissions, err)
	}

	return mode&0o022 != 0, nil
}
