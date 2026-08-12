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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	proxmox "github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	goproxmox "github.com/sergelogvinov/go-proxmox"
	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	volume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"
)

func TestIsVolumeAttached(t *testing.T) {
	t.Parallel()

	tests := []struct {
		msg           string
		vmConfig      *proxmox.VirtualMachineConfig
		pvc           string
		expectedLun   int
		expectedExist bool
	}{
		{
			msg:           "Empty VM config",
			vmConfig:      &proxmox.VirtualMachineConfig{},
			pvc:           "",
			expectedLun:   0,
			expectedExist: false,
		},
		{
			msg: "Empty PVC",
			vmConfig: &proxmox.VirtualMachineConfig{
				IDE2:  "local:iso/ubuntu-20.04.1-live-server-amd64.iso,media=cdrom",
				SCSI0: "local-lvm:vm-100-disk-0,size=8G",
				SCSI5: "local-lvm:vm-100-pvc-123,size=8G",
			},
			pvc:           "",
			expectedLun:   0,
			expectedExist: false,
		},
		{
			msg: "LUN 5",
			vmConfig: &proxmox.VirtualMachineConfig{
				IDE2:  "local:iso/ubuntu-20.04.1-live-server-amd64.iso,media=cdrom",
				SCSI0: "local-lvm:vm-100-disk-0,size=8G",
				SCSI5: "local-lvm:vm-100-pvc-123,size=8G",
			},
			pvc:           "pvc-123",
			expectedLun:   5,
			expectedExist: true,
		},
	}

	for _, testCase := range tests {
		t.Run(fmt.Sprint(testCase.msg), func(t *testing.T) {
			t.Parallel()

			lun, exist := isVolumeAttached(testCase.vmConfig, testCase.pvc)

			if testCase.expectedExist {
				assert.True(t, exist)
				assert.Equal(t, testCase.expectedLun, lun)
			} else {
				assert.False(t, exist)
				assert.Equal(t, 0, lun)
			}
		})
	}
}

// TestCopyVolume covers the two things copyVolume still owns now that the
// request building lives in toolsproxmox.MoveQemuDisk: its guards, and that the
// caller's chosen endpoint actually reaches the wire. The per-endpoint request
// shapes are pinned in pkg/tools/proxmox's TestMoveQemuDiskRouting.
func TestCopyVolume(t *testing.T) {
	t.Parallel()

	src := volume.NewVolume("cluster-1", "pve-1", "local-lvm", "vm-9999-disk-0")

	t.Run("source node is required", func(t *testing.T) {
		t.Parallel()

		nodeless := volume.NewVolume("cluster-1", "", "local-lvm", "vm-9999-disk-0")
		err := copyVolume(context.Background(), nil, nodeless, src, pxpool.CopyEndpointBuiltin)
		assert.EqualError(t, err, "node is required")
	})

	t.Run("qcow2 destination is rejected", func(t *testing.T) {
		t.Parallel()

		dst := volume.NewVolume("cluster-1", "pve-2", "datastore1", "9999/vm-9999-disk-0.qcow2")
		err := copyVolume(context.Background(), nil, src, dst, pxpool.CopyEndpointBuiltin)
		assert.EqualError(t, err, "volume disk must not be qcow2 format")
	})

	// The endpoint the controller resolves from the cluster's proxmod_endpoint /
	// token_copy_endpoint settings has to survive the hop into MoveQemuDisk;
	// hardcoding the builtin here is exactly the regression that would silently
	// put the root@pam requirement back.
	endpoints := []struct {
		name     string
		endpoint pxpool.CopyEndpoint
		wantPath string
	}{
		{"builtin", pxpool.CopyEndpointBuiltin, "/api2/json/nodes/pve-1/storage/local-lvm/content/vm-9999-disk-0"},
		{"csi-copy", pxpool.CopyEndpointCSICopy, "/api2/json/nodes/pve-1/storage/local-lvm/csi-copy"},
		{"proxmod", pxpool.CopyEndpointProxmod, "/api2/json/nodes/pve-1/proxmod/csi-storage/copy"},
	}

	for _, tt := range endpoints {
		t.Run(tt.name+" endpoint is passed through", func(t *testing.T) {
			t.Parallel()

			const upid = "UPID:pve-1:00001234:0000ABCD:00000000:imgcopy:9999:root@pam:"

			var gotPath string

			mux := http.NewServeMux()
			mux.HandleFunc("/api2/json/nodes/pve-1/tasks/"+upid+"/status", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"data":{"status":"stopped","exitstatus":"OK","upid":%q}}`, upid)
			})
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.EscapedPath()

				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"data":%q}`, upid)
			})

			srv := httptest.NewServer(mux)
			defer srv.Close()

			cl, err := goproxmox.NewAPIClient(srv.URL + "/api2/json")
			require.NoError(t, err)

			dst := volume.NewVolume("cluster-1", "pve-2", "datastore1", "vm-9999-disk-0")

			require.NoError(t, copyVolume(context.Background(), cl, src, dst, tt.endpoint))
			assert.Equal(t, tt.wantPath, gotPath)
		})
	}
}

// TestCopyTaskTimeout guards the one number the delegation to MoveQemuDisk
// could quietly lose. MoveQemuDisk derives its poll count as taskTimeout/15,
// and copyVolume previously waited 240 polls x 15s by hand, so anything other
// than 3600 shortens a snapshot copy's budget — passing the old literal 4*60
// would cut a 60-minute wait to 4 minutes.
func TestCopyTaskTimeout(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 240*15, copyTaskTimeout)
}
