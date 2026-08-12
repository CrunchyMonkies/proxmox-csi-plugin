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

package volume_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"
)

func TestNewVolume(t *testing.T) {
	v := volume.NewVolume("region", "zone", "storage", "disk")
	assert.NotNil(t, v)
	assert.Equal(t, "region", v.Cluster())
	assert.Equal(t, "zone", v.Node())
	assert.Equal(t, "region", v.Region())
	assert.Equal(t, "zone", v.Zone())
	assert.Equal(t, "zone", v.Node())
	assert.Equal(t, "storage", v.Storage())
	assert.Equal(t, "disk", v.Disk())
	assert.Equal(t, "region/zone/storage/disk", v.VolumeID())
}

func TestNewVolumeFormat(t *testing.T) {
	v := volume.NewVolume("region", "zone", "storage", "vm-100-disk-0", "raw")
	assert.NotNil(t, v)
	assert.Equal(t, "region", v.Cluster())
	assert.Equal(t, "zone", v.Node())
	assert.Equal(t, "region", v.Region())
	assert.Equal(t, "zone", v.Zone())
	assert.Equal(t, "zone", v.Node())
	assert.Equal(t, "storage", v.Storage())
	assert.Equal(t, "100/vm-100-disk-0.raw", v.Disk())
	assert.Equal(t, "region/zone/storage/100/vm-100-disk-0.raw", v.VolumeID())
}

func TestNewVolumeFromVolumeID(t *testing.T) {
	v, err := volume.NewVolumeFromVolumeID("region/zone/storage/vm-1000-disk")
	assert.Nil(t, err)
	assert.NotNil(t, v)
	assert.Equal(t, "region", v.Cluster())
	assert.Equal(t, "zone", v.Zone())
	assert.Equal(t, "zone", v.Node())
	assert.Equal(t, "storage", v.Storage())
	assert.Equal(t, "vm-1000-disk", v.Disk())
	assert.Equal(t, "1000", v.VMID())
	assert.Equal(t, "storage:vm-1000-disk", v.VolID())
}

func TestNewVolumeFromSharedVolumeID(t *testing.T) {
	v, err := volume.NewVolumeFromVolumeID("region//storage/vm-1000-disk")
	assert.Nil(t, err)
	assert.NotNil(t, v)

	v.SetNode("node")

	assert.Equal(t, "region", v.Cluster())
	assert.Equal(t, "", v.Zone())
	assert.Equal(t, "node", v.Node())
	assert.Equal(t, "storage", v.Storage())
	assert.Equal(t, "vm-1000-disk", v.Disk())
	assert.Equal(t, "1000", v.VMID())
	assert.Equal(t, "storage:vm-1000-disk", v.VolID())
	assert.Equal(t, "region//storage/vm-1000-disk", v.VolumeID())
}

func TestNewVolumeFromVolumeIDWithFolder(t *testing.T) {
	v, err := volume.NewVolumeFromVolumeID("region/zone/storage/1000/folder/vm-1000-disk.raw")
	assert.Nil(t, err)
	assert.NotNil(t, v)
	assert.Equal(t, "region", v.Cluster())
	assert.Equal(t, "zone", v.Zone())
	assert.Equal(t, "zone", v.Node())
	assert.Equal(t, "storage", v.Storage())
	assert.Equal(t, "1000/folder/vm-1000-disk.raw", v.Disk())
	assert.Equal(t, "storage:1000/folder/vm-1000-disk.raw", v.VolID())
}

func TestNewVolumeFromSharedVolumeIDWithFolder(t *testing.T) {
	v, err := volume.NewVolumeFromVolumeID("region//storage/1000/folder/vm-1000-disk.raw")
	assert.Nil(t, err)
	assert.NotNil(t, v)

	v.SetNode("node")

	assert.Equal(t, "region", v.Cluster())
	assert.Equal(t, "", v.Zone())
	assert.Equal(t, "node", v.Node())
	assert.Equal(t, "storage", v.Storage())
	assert.Equal(t, "1000/folder/vm-1000-disk.raw", v.Disk())
	assert.Equal(t, "storage:1000/folder/vm-1000-disk.raw", v.VolID())
	assert.Equal(t, "region//storage/1000/folder/vm-1000-disk.raw", v.VolumeID())
}

func TestNewVolumeFromVolumeIDError(t *testing.T) {
	_, err := volume.NewVolumeFromVolumeID("region/storage/disk")
	assert.NotNil(t, err)
	assert.Equal(t, "VolumeID must be in the format of region/zone/storageName/diskName", err.Error())
}

func TestDiskName(t *testing.T) {
	for _, tt := range []struct {
		disk     string
		expected string
	}{
		{"9999/vm-9999-pvc-abc.raw", "vm-9999-pvc-abc.raw"},
		{"vm-9999-disk-0", "vm-9999-disk-0"},
		{"1000/folder.dev/vm-1000-disk.raw", "vm-1000-disk.raw"},
	} {
		v := volume.NewVolume("region", "zone", "storage", tt.disk)
		assert.Equal(t, tt.expected, v.DiskName(), tt.disk)
	}
}

func TestDiskSuffix(t *testing.T) {
	for _, tt := range []struct {
		disk     string
		expected string
	}{
		// the shapes a CSI volume actually takes
		{"9999/vm-9999-pvc-abc-123.raw", "pvc-abc-123.raw"},
		{"3021/vm-3021-pvc-abc-123.raw", "pvc-abc-123.raw"},
		{"vm-9999-disk-0", "disk-0"},

		// no vm-<vmid>- prefix: not trackable across a rename
		{"base-100-disk-0", ""},
		{"vm-99-disk-0", ""}, // below Proxmox's minimum vmid
		{"1000/folder.dev/vm-1000-disk.raw", ""},
		{"", ""},
	} {
		v := volume.NewVolume("region", "zone", "storage", tt.disk)
		assert.Equal(t, tt.expected, v.DiskSuffix(), tt.disk)
	}
}

// The suffix is the same on both sides of a rename - that is the whole reason
// it is used to find a volume whose name no longer matches its VolumeID.
func TestDiskSuffixIsStableAcrossRename(t *testing.T) {
	v := volume.NewVolume("region", "zone", "storage", "9999/vm-9999-pvc-abc-123.raw")

	assert.Equal(t, v.DiskSuffix(), v.WithVMID(3021).DiskSuffix())
}

func TestWithVMID(t *testing.T) {
	for _, tt := range []struct {
		disk     string
		vmid     int
		expected string
	}{
		// the directory plugin's '<vmid>/' component is rewritten with the name
		{"9999/vm-9999-pvc-abc.raw", 3021, "3021/vm-3021-pvc-abc.raw"},
		{"3021/vm-3021-pvc-abc.raw", 9999, "9999/vm-9999-pvc-abc.raw"},

		// flat storages (lvm, lvm-thin, zfspool) keep their flat shape
		{"vm-9999-pvc-abc", 3021, "vm-3021-pvc-abc"},
	} {
		v := volume.NewVolume("region", "zone", "storage", tt.disk).WithVMID(tt.vmid)
		assert.NotNil(t, v, tt.disk)
		assert.Equal(t, tt.expected, v.Disk(), tt.disk)
		assert.Equal(t, "storage:"+tt.expected, v.VolID(), tt.disk)
		assert.Equal(t, strconv.Itoa(tt.vmid), v.VMID(), tt.disk)
	}
}

// nil rather than an unchanged copy: a rename that silently renames nothing is
// worse than one that refuses.
func TestWithVMIDUnnameable(t *testing.T) {
	for _, disk := range []string{"base-100-disk-0", "1000/folder.dev/vm-1000-disk.raw", ""} {
		assert.Nil(t, volume.NewVolume("region", "zone", "storage", disk).WithVMID(3021), disk)
	}
}

func TestWithVMIDPreservesLocation(t *testing.T) {
	v, err := volume.NewVolumeFromVolumeID("region/zone/storage/9999/vm-9999-pvc-abc.raw")
	assert.Nil(t, err)
	v.SetNode("node")

	renamed := v.WithVMID(3021)
	assert.NotNil(t, renamed)
	assert.Equal(t, "region", renamed.Region())
	assert.Equal(t, "zone", renamed.Zone())
	assert.Equal(t, "node", renamed.Node())
	assert.Equal(t, "storage", renamed.Storage())
	assert.Equal(t, "pvc-abc", renamed.PV())

	// the original is untouched: callers hold the VolumeID's name and the
	// renamed name at the same time, and must not have the two alias.
	assert.Equal(t, "9999/vm-9999-pvc-abc.raw", v.Disk())
}

func TestCopyVolume(t *testing.T) {
	v, err := volume.NewVolumeFromVolumeID("region//storage/1000/folder.dev/vm-1000-disk.raw")
	assert.Nil(t, err)
	assert.NotNil(t, v)
	v.SetNode("node")

	copied := v.CopyVolume("vm-1000-disk-snap1")
	assert.NotNil(t, copied)
	assert.Equal(t, "region", copied.Cluster())
	assert.Equal(t, "", copied.Zone())
	assert.Equal(t, "node", copied.Node())
	assert.Equal(t, "storage", copied.Storage())
	assert.Equal(t, "1000/vm-1000-disk-snap1.raw", copied.Disk())
	assert.Equal(t, "storage:1000/vm-1000-disk-snap1.raw", copied.VolID())
	assert.Equal(t, "region//storage/1000/vm-1000-disk-snap1.raw", copied.VolumeID())
}
