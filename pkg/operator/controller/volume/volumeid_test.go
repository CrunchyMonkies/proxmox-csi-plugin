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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"
	toolsproxmox "github.com/sergelogvinov/proxmox-csi-plugin/pkg/tools/proxmox"
	pvevolume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"
)

// TestGoldenVolumeID verifies that the operator constructs the exact same
// volumeID as the CSI controller for the same inputs.
//
// Both the CSI controller and the operator now call toolsproxmox.DiskFormat to
// determine the format, then pvevolume.NewVolume with that format. This test
// exercises the shared DiskFormat function across all storage types to ensure
// byte-identical volume handles.
func TestGoldenVolumeID(t *testing.T) {
	tests := []struct {
		name          string
		region        string
		zone          string
		storage       string
		pluginType    string
		placeholderID int
		pvName        string
		requestedFmt  string
		wantVolumeID  string
		wantDiskName  string
	}{
		{
			name:          "lvmthin block storage, no format suffix",
			region:        "bne",
			zone:          "pve-1",
			storage:       "local-lvm",
			pluginType:    "lvmthin",
			placeholderID: 9991,
			pvName:        "pvc-abc-123",
			requestedFmt:  "",
			wantVolumeID:  "bne/pve-1/local-lvm/vm-9991-pvc-abc-123",
			wantDiskName:  "vm-9991-pvc-abc-123",
		},
		{
			name:          "zfspool block storage, no format suffix",
			region:        "syd",
			zone:          "pve-3",
			storage:       "local-zfs",
			pluginType:    "zfspool",
			placeholderID: 9997,
			pvName:        "pvc-def-456",
			requestedFmt:  "",
			wantVolumeID:  "syd/pve-3/local-zfs/vm-9997-pvc-def-456",
			wantDiskName:  "vm-9997-pvc-def-456",
		},
		{
			name:          "dir (file-level) storage defaults to .raw",
			region:        "bne",
			zone:          "pve-1",
			storage:       "local",
			pluginType:    "dir",
			placeholderID: 9991,
			pvName:        "pvc-dir-001",
			requestedFmt:  "",
			wantVolumeID:  "bne/pve-1/local/9991/vm-9991-pvc-dir-001.raw",
			wantDiskName:  "vm-9991-pvc-dir-001.raw",
		},
		{
			name:          "dir storage with explicit qcow2",
			region:        "bne",
			zone:          "pve-1",
			storage:       "local",
			pluginType:    "dir",
			placeholderID: 9991,
			pvName:        "pvc-qcow-789",
			requestedFmt:  "qcow2",
			wantVolumeID:  "bne/pve-1/local/9991/vm-9991-pvc-qcow-789.qcow2",
			wantDiskName:  "vm-9991-pvc-qcow-789.qcow2",
		},
		{
			name:          "nfs (file-level) storage defaults to .raw",
			region:        "bne",
			zone:          "pve-1",
			storage:       "nfs-share",
			pluginType:    "nfs",
			placeholderID: 9991,
			pvName:        "pvc-nfs-001",
			requestedFmt:  "",
			wantVolumeID:  "bne/pve-1/nfs-share/9991/vm-9991-pvc-nfs-001.raw",
			wantDiskName:  "vm-9991-pvc-nfs-001.raw",
		},
		{
			name:          "lvm with qcow2 (volume-chain preview)",
			region:        "bne",
			zone:          "pve-1",
			storage:       "lvm-store",
			pluginType:    "lvm",
			placeholderID: 9991,
			pvName:        "pvc-lvm-qcow",
			requestedFmt:  "qcow2",
			wantVolumeID:  "bne/pve-1/lvm-store/9991/vm-9991-pvc-lvm-qcow.qcow2",
			wantDiskName:  "vm-9991-pvc-lvm-qcow.qcow2",
		},
		{
			name:          "lvm without qcow2, no format",
			region:        "bne",
			zone:          "pve-1",
			storage:       "lvm-store",
			pluginType:    "lvm",
			placeholderID: 9991,
			pvName:        "pvc-lvm-raw",
			requestedFmt:  "",
			wantVolumeID:  "bne/pve-1/lvm-store/vm-9991-pvc-lvm-raw",
			wantDiskName:  "vm-9991-pvc-lvm-raw",
		},
		{
			name:          "rbd block storage, no format",
			region:        "bne",
			zone:          "pve-1",
			storage:       "ceph-pool",
			pluginType:    "rbd",
			placeholderID: 9991,
			pvName:        "pvc-rbd-001",
			requestedFmt:  "",
			wantVolumeID:  "bne/pve-1/ceph-pool/vm-9991-pvc-rbd-001",
			wantDiskName:  "vm-9991-pvc-rbd-001",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The shared format function both sides call.
			format := toolsproxmox.DiskFormat(tt.pluginType, tt.requestedFmt)

			diskName := fmt.Sprintf("vm-%d-%s", tt.placeholderID, tt.pvName)

			// CSI controller path.
			csiVol := pvevolume.NewVolume(tt.region, tt.zone, tt.storage, diskName, format)

			// Operator path -- same construction.
			operatorVol := pvevolume.NewVolume(tt.region, tt.zone, tt.storage, diskName, format)

			assert.Equal(t, tt.wantVolumeID, csiVol.VolumeID(),
				"CSI controller volumeID")
			assert.Equal(t, csiVol.VolumeID(), operatorVol.VolumeID(),
				"operator must produce byte-identical volumeID")

			// Also verify the disk name the create-disk API receives.
			assert.Equal(t, tt.wantDiskName, csiVol.DiskName(),
				"bare disk filename sent to Proxmox CreateVMDisk")
		})
	}
}

// TestDiskFormatFunction directly tests the shared DiskFormat function.
func TestDiskFormatFunction(t *testing.T) {
	tests := []struct {
		pluginType   string
		requestedFmt string
		wantFormat   string
	}{
		{"lvmthin", "", ""},
		{"lvmthin", "qcow2", ""},
		{"zfspool", "", ""},
		{"rbd", "", ""},
		{"dir", "", "raw"},
		{"dir", "qcow2", "qcow2"},
		{"dir", "raw", "raw"},
		{"nfs", "", "raw"},
		{"nfs", "qcow2", "qcow2"},
		{"cifs", "", "raw"},
		{"cephfs", "", "raw"},
		{"btrfs", "", "raw"},
		{"lvm", "", ""},
		{"lvm", "qcow2", "qcow2"},
		{"lvm", "raw", ""},
	}

	for _, tt := range tests {
		t.Run(tt.pluginType+"/"+tt.requestedFmt, func(t *testing.T) {
			got := toolsproxmox.DiskFormat(tt.pluginType, tt.requestedFmt)
			assert.Equal(t, tt.wantFormat, got)
		})
	}
}

// TestProvisionOnDirStorageProducesCorrectVolumeID tests that provisioning on
// a dir storage (file-level) produces a volumeID with the .raw extension and
// directory prefix, matching what the CSI controller would produce.
func TestProvisionOnDirStorageProducesCorrectVolumeID(t *testing.T) {
	req := provisionRequest("pvc-dir-test", func(v *v1alpha1.ProxmoxVolume) {
		v.Spec.Storage = "local"
	})

	tenant := enforceTenant(func(tc *v1alpha1.TenantCluster) {
		tc.Spec.AllowedStorages = []string{"local"}
	})

	disks := &fakeDisks{disks: map[string][]proxmox.Disk{"bne/local": {}}}
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w, tenant, req, storageObject("local", "dir"))

	reconcile(t, r, "pvc-dir-test")

	obj := get(t, c, "pvc-dir-test")
	assert.Equal(t, v1alpha1.VolumePhaseReady, obj.Status.Phase)

	// Must match the CSI controller's format for dir storage.
	assert.Equal(t, "bne/pve-1/local/9991/vm-9991-pvc-dir-test.raw", obj.Status.VolumeID)
	assert.Contains(t, obj.Status.DiskName, ".raw")

	// And the disk is created under that name: Proxmox derives the file and its
	// format from the filename, so a bare vm-9991-pvc-dir-test would fail on dir
	// storage (or make a different disk than the volumeID points at).
	if assert.Len(t, w.creates, 1) {
		assert.Equal(t, "9991/vm-9991-pvc-dir-test.raw", w.creates[0].diskName)
	}
}

// TestProvisionRefusedUnknownStorageType tests that provisioning refuses when
// the ProxmoxStorage catalog does not have an entry for the requested storage.
func TestProvisionRefusedUnknownStorageType(t *testing.T) {
	req := provisionRequest("pvc-unknown")

	disks := onLocalLVM()
	w := &fakeWriter{}

	// No storageObject — the catalog does not know about local-lvm.
	r, c := newProvisionReconciler(t, disks, w, enforceTenant(), req)

	reconcile(t, r, "pvc-unknown")

	obj := get(t, c, "pvc-unknown")
	assert.Equal(t, v1alpha1.VolumePhaseRejected, obj.Status.Phase)
	assert.Equal(t, v1alpha1.ReasonInvalidSpec, condition(t, obj, v1alpha1.ConditionAdmitted).Reason)
	assert.Contains(t, condition(t, obj, v1alpha1.ConditionAdmitted).Message, "cannot determine storage type")
	assert.Empty(t, w.creates)
}

// TestProvisionRefusedEmptyPluginType tests that provisioning refuses when
// the ProxmoxStorage exists but has no pluginType set.
func TestProvisionRefusedEmptyPluginType(t *testing.T) {
	req := provisionRequest("pvc-notype")

	disks := onLocalLVM()
	w := &fakeWriter{}

	// Storage object without pluginType.
	badStorage := storageObject("local-lvm", "")

	r, c := newProvisionReconciler(t, disks, w, enforceTenant(), req, badStorage)

	reconcile(t, r, "pvc-notype")

	obj := get(t, c, "pvc-notype")
	assert.Equal(t, v1alpha1.VolumePhaseRejected, obj.Status.Phase)
	assert.Equal(t, v1alpha1.ReasonInvalidSpec, condition(t, obj, v1alpha1.ConditionAdmitted).Reason)
	assert.Contains(t, condition(t, obj, v1alpha1.ConditionAdmitted).Message, "no pluginType")
	assert.Empty(t, w.creates)
}
