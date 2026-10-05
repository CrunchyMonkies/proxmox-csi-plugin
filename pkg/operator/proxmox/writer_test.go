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

package proxmox_test

import (
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"
	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	volume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"
	testcluster "github.com/sergelogvinov/proxmox-csi-plugin/test/cluster"
)

func newDiskWriter(t *testing.T) *proxmox.DiskWriter {
	t.Helper()

	// Use the same pool layout as newPool: cluster-1 on 127.0.0.2, so the mock
	// responders registered by testcluster.SetupMockResponders match.
	pool, err := pxpool.NewProxmoxPool([]*pxpool.ProxmoxCluster{
		{
			URL:         "https://127.0.0.2:8006/api2/json",
			TokenID:     "user!token-id",
			TokenSecret: "secret",
			Region:      "cluster-1",
		},
	})
	require.NoError(t, err)

	return proxmox.NewDiskWriter(pool)
}

func TestDiskWriterUnknownRegion(t *testing.T) {
	w := newDiskWriter(t)

	t.Run("CreateDisk", func(t *testing.T) {
		err := w.CreateDisk(t.Context(), "nowhere", "pve-1", "local-lvm", "vm-9999-pvc-test", 1024*1024*1024)
		require.Error(t, err)
		assert.ErrorIs(t, err, pxpool.ErrRegionNotFound)
	})

	t.Run("DeleteDisk", func(t *testing.T) {
		err := w.DeleteDisk(t.Context(), "nowhere", "pve-1", "local-lvm", "vm-9999-pvc-test")
		require.Error(t, err)
		assert.ErrorIs(t, err, pxpool.ErrRegionNotFound)
	})

	t.Run("AttachDisk", func(t *testing.T) {
		vol := volume.NewVolume("nowhere", "pve-1", "local-lvm", "vm-9999-pvc-test")
		_, err := w.AttachDisk(t.Context(), "nowhere", 100, vol, map[string]string{})
		require.Error(t, err)
		assert.ErrorIs(t, err, pxpool.ErrRegionNotFound)
	})

	t.Run("DetachDisk", func(t *testing.T) {
		vol := volume.NewVolume("nowhere", "pve-1", "local-lvm", "vm-9999-pvc-test")
		err := w.DetachDisk(t.Context(), "nowhere", 100, vol)
		require.Error(t, err)
		assert.ErrorIs(t, err, pxpool.ErrRegionNotFound)
	})

	t.Run("ResizeDisk", func(t *testing.T) {
		err := w.ResizeDisk(t.Context(), "nowhere", 100, "pve-1", "scsi1", "2G")
		require.Error(t, err)
		assert.ErrorIs(t, err, pxpool.ErrRegionNotFound)
	})

	t.Run("UpdateDisk", func(t *testing.T) {
		vol := volume.NewVolume("nowhere", "pve-1", "local-lvm", "vm-9999-pvc-test")
		err := w.UpdateDisk(t.Context(), "nowhere", 100, vol, map[string]string{})
		require.Error(t, err)
		assert.ErrorIs(t, err, pxpool.ErrRegionNotFound)
	})

	t.Run("RenameDisk", func(t *testing.T) {
		vol := volume.NewVolume("nowhere", "pve-1", "local-lvm", "vm-9999-pvc-test")
		_, err := w.RenameDisk(t.Context(), "nowhere", vol, 100)
		require.Error(t, err)
		assert.ErrorIs(t, err, pxpool.ErrRegionNotFound)
	})

	t.Run("CopyDisk", func(t *testing.T) {
		err := w.CopyDisk(t.Context(), "nowhere", "pve-1", "local-lvm", "vm-9999-pvc-test",
			"pve-2", "local-lvm", "vm-9999-pvc-test-snap")
		require.Error(t, err)
		assert.ErrorIs(t, err, pxpool.ErrRegionNotFound)
	})
}

func TestDiskWriterAttachDisk(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	testcluster.SetupMockResponders()

	w := newDiskWriter(t)

	vol := volume.NewVolume("cluster-1", "pve-1", "local-lvm", "vm-9999-pvc-unpublished")

	result, err := w.AttachDisk(t.Context(), "cluster-1", 100, vol, map[string]string{
		"backup":   "0",
		"iothread": "1",
	})
	require.NoError(t, err)

	assert.NotEmpty(t, result.DevicePath)
	assert.Greater(t, result.Lun, 0)

	// The attach must have issued a POST to the VM config.
	reqs := testcluster.AttachRequests()
	require.NotEmpty(t, reqs)
	assert.Equal(t, 100, reqs[0].VMID)
}

func TestDiskWriterDetachDisk(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	testcluster.SetupMockResponders()

	w := newDiskWriter(t)

	vol := volume.NewVolume("cluster-1", "pve-2", "local-lvm", "vm-101-pvc-reassigned")

	err := w.DetachDisk(t.Context(), "cluster-1", 101, vol)
	require.NoError(t, err)
}

func TestDiskWriterResizeDisk(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	testcluster.SetupMockResponders()

	// The shared fixture registers resize on 127.0.0.1; register one for the
	// writer's pool address so the call reaches a responder.
	httpmock.RegisterResponder("PUT", `=~127\.0\.0\.2.*/qemu/100/resize`,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{"data": ""}))

	w := newDiskWriter(t)

	err := w.ResizeDisk(t.Context(), "cluster-1", 100, "pve-1", "scsi1", "2048M")
	require.NoError(t, err)
}

func TestDiskWriterUpdateDisk(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	testcluster.SetupMockResponders()

	w := newDiskWriter(t)

	vol := volume.NewVolume("cluster-1", "pve-1", "local-lvm", "vm-9999-pvc-123")

	err := w.UpdateDisk(t.Context(), "cluster-1", 100, vol, map[string]string{
		"backup":   "1",
		"iothread": "1",
	})
	require.NoError(t, err)
}

func TestDiskWriterRenameDisk(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	testcluster.SetupMockResponders()

	w := newDiskWriter(t)

	vol := volume.NewVolume("cluster-1", "pve-1", "local-lvm", "vm-9999-pvc-123")

	renamed, err := w.RenameDisk(t.Context(), "cluster-1", vol, 100)
	require.NoError(t, err)
	require.NotNil(t, renamed)

	reqs := testcluster.RenameRequests()
	require.Len(t, reqs, 1)
	assert.Equal(t, "local-lvm", reqs[0].Storage)
	assert.Equal(t, "vm-9999-pvc-123", reqs[0].Volume)
	assert.Equal(t, 100, reqs[0].TargetVMID)
}
