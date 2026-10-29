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
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

			lun, exist := IsVolumeAttached(testCase.vmConfig, testCase.pvc)

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

// TestCopyVolume covers the two things CopyVolume still owns now that the
// request building lives in MoveQemuDisk: its guards, and that the
// caller's chosen endpoint actually reaches the wire. The per-endpoint request
// shapes are pinned in TestMoveQemuDiskRouting.
func TestCopyVolume(t *testing.T) {
	t.Parallel()

	src := volume.NewVolume("cluster-1", "pve-1", "local-lvm", "vm-9999-disk-0")

	t.Run("source node is required", func(t *testing.T) {
		t.Parallel()

		nodeless := volume.NewVolume("cluster-1", "", "local-lvm", "vm-9999-disk-0")
		err := CopyVolume(context.Background(), nil, nodeless, src, pxpool.CopyEndpointBuiltin)
		assert.EqualError(t, err, "node is required")
	})

	t.Run("qcow2 destination is rejected", func(t *testing.T) {
		t.Parallel()

		dst := volume.NewVolume("cluster-1", "pve-2", "datastore1", "9999/vm-9999-disk-0.qcow2")
		err := CopyVolume(context.Background(), nil, src, dst, pxpool.CopyEndpointBuiltin)
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

			require.NoError(t, CopyVolume(context.Background(), cl, src, dst, tt.endpoint))
			assert.Equal(t, tt.wantPath, gotPath)
		})
	}
}

// TestCopyTaskTimeout guards the one number the delegation to MoveQemuDisk
// could quietly lose. MoveQemuDisk derives its poll count as taskTimeout/15,
// and CopyVolume previously waited 240 polls x 15s by hand, so anything other
// than 3600 shortens a snapshot copy's budget — passing the old literal 4*60
// would cut a 60-minute wait to 4 minutes.
func TestCopyTaskTimeout(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 240*15, CopyTaskTimeout)
}

// TestDetachVolumeHotUnplugTimeout replays what PVE does when a Windows guest is slow to
// release the virtio-scsi controller of a disk: the first unlink fails with "still busy
// in guest?" and leaves the removal pending, the second applies it.
func TestDetachVolumeHotUnplugTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		firstError string
		wantErr    bool
		wantCalls  int
	}{
		{
			name:       "controller still busy",
			firstError: `{"errors":{"scsi5":"hotplug problem - error on hot-unplugging device 'virtioscsi5' - still busy in guest?\n"},"data":null}`,
			wantCalls:  2,
		},
		{
			name:       "other failure is not retried",
			firstError: `{"errors":{"scsi5":"can't unplug bootdisk 'scsi5'\n"},"data":null}`,
			wantErr:    true,
			wantCalls:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			unlinks := 0

			mux := http.NewServeMux()
			mux.HandleFunc("/api2/json/cluster/resources", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"data":[{"type":"qemu","vmid":100,"node":"pve-1","name":"win-1","status":"running"}]}`)
			})
			mux.HandleFunc("/api2/json/nodes/pve-1/qemu/100/status/current", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"data":{"vmid":100,"status":"running"}}`)
			})
			mux.HandleFunc("/api2/json/nodes/pve-1/qemu/100/config", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"data":{"scsi0":"local:100/vm-100-disk-0.qcow2,size=100G","scsi5":"local:vm-9999-pvc-123,size=1G,wwn=0x5056432d49443035"}}`)
			})
			mux.HandleFunc("/api2/json/nodes/pve-1/qemu/100/unlink", func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPut, r.Method)

				unlinks++

				w.Header().Set("Content-Type", "application/json")

				if unlinks == 1 {
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(w, tt.firstError)

					return
				}

				fmt.Fprint(w, `{"data":null}`)
			})

			srv := httptest.NewServer(mux)
			defer srv.Close()

			cl, err := goproxmox.NewAPIClient(srv.URL + "/api2/json")
			require.NoError(t, err)

			vol := volume.NewVolume("cluster-1", "pve-1", "local", "vm-9999-pvc-123")

			err = DetachVolume(context.Background(), cl, 100, vol)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.wantCalls, unlinks)
		})
	}
}

func TestDeviceNameLun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lun    int
		device string
	}{
		{lun: 0, device: "scsi0"},
		{lun: 1, device: "scsi1"},
		{lun: 30, device: "scsi30"},
		{lun: 31, device: "virtio0"},
		{lun: 46, device: "virtio15"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.device, DeviceName(tt.lun))

		lun, ok := DeviceLun(tt.device)
		assert.True(t, ok, tt.device)
		assert.Equal(t, tt.lun, lun, tt.device)
	}

	for _, device := range []string{"scsi31", "virtio16", "sata0", "ide2", "scsi", "virtio-1", "efidisk0"} {
		_, ok := DeviceLun(device)
		assert.False(t, ok, device)
	}
}

func TestDevicePath(t *testing.T) {
	t.Parallel()

	// SCSI paths are unchanged: "PVC-ID05" hex-encoded as the disk's WWN.
	assert.Equal(t, "/dev/disk/by-id/wwn-0x5056432d49443035", DevicePath(5))
	assert.Equal(t, "/dev/disk/by-id/wwn-0x5056432d49443330", DevicePath(30))
	assert.Equal(t, "/dev/disk/by-id/virtio-PVC-ID31", DevicePath(31))
	assert.Equal(t, "/dev/disk/by-id/virtio-PVC-ID46", DevicePath(46))
}

func TestIsVolumeAttachedVirtIO(t *testing.T) {
	t.Parallel()

	vm := &proxmox.VirtualMachineConfig{
		SCSI0:   "local-lvm:vm-100-disk-0,size=8G",
		VirtIO3: "local-lvm:vm-9999-pvc-123,serial=PVC-ID34",
	}

	lun, ok := IsVolumeAttached(vm, "pvc-123")
	assert.True(t, ok)
	assert.Equal(t, 34, lun)
}

// fullSCSIConfig returns a VM config map with every SCSI slot taken.
func fullSCSIConfig() map[string]string {
	config := map[string]string{}
	for i := range MaxSCSIDisks {
		config[fmt.Sprintf("scsi%d", i)] = fmt.Sprintf("local-lvm:vm-100-disk-%d,size=1G", i)
	}

	return config
}

func vmConfigFrom(t *testing.T, config map[string]string) *proxmox.VirtualMachineConfig {
	t.Helper()

	b, err := json.Marshal(config)
	require.NoError(t, err)

	vm := &proxmox.VirtualMachineConfig{}
	require.NoError(t, json.Unmarshal(b, vm))

	return vm
}

func TestFreeLun(t *testing.T) {
	t.Parallel()

	t.Run("boot disk only", func(t *testing.T) {
		t.Parallel()

		lun, ok := freeLun(vmConfigFrom(t, map[string]string{"scsi0": "local-lvm:vm-100-disk-0"}))
		assert.True(t, ok)
		assert.Equal(t, 1, lun)
	})

	t.Run("scsi30 is used", func(t *testing.T) {
		t.Parallel()

		config := fullSCSIConfig()
		delete(config, "scsi30")

		lun, ok := freeLun(vmConfigFrom(t, config))
		assert.True(t, ok)
		assert.Equal(t, 30, lun)
	})

	t.Run("falls back to virtio once scsi is full", func(t *testing.T) {
		t.Parallel()

		config := fullSCSIConfig()
		config["virtio0"] = "local-lvm:vm-100-disk-boot"

		lun, ok := freeLun(vmConfigFrom(t, config))
		assert.True(t, ok)
		assert.Equal(t, "virtio1", DeviceName(lun))
	})

	t.Run("every slot taken", func(t *testing.T) {
		t.Parallel()

		config := fullSCSIConfig()
		for i := range MaxVirtIODisks {
			config[fmt.Sprintf("virtio%d", i)] = "local-lvm:vm-100-disk-x"
		}

		_, ok := freeLun(vmConfigFrom(t, config))
		assert.False(t, ok)
	})
}

func TestDeviceOptions(t *testing.T) {
	t.Parallel()

	in := map[string]string{"backup": "0", "iothread": "1", "ssd": "1", "wwn": "0xdead"}

	scsi := deviceOptions(5, in)
	assert.Equal(t, "0x5056432d49443035", scsi["wwn"])
	assert.Equal(t, "1", scsi["ssd"])
	assert.NotContains(t, scsi, "serial")

	virtio := deviceOptions(31, in)
	assert.Equal(t, "PVC-ID31", virtio["serial"])
	assert.NotContains(t, virtio, "wwn")
	assert.NotContains(t, virtio, "ssd")
	assert.Equal(t, "1", virtio["iothread"])

	// The caller's map is left as it was.
	assert.Equal(t, "0xdead", in["wwn"])
	assert.NotContains(t, in, "serial")
}

// TestAttachVolumeVirtIOFallback attaches to a VM whose SCSI slots are all taken
// and checks the disk lands on the first free virtio slot with a serial, not a
// WWN, and that the published path is the matching virtio by-id link.
func TestAttachVolumeVirtIOFallback(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex

	config := fullSCSIConfig()
	config["virtio0"] = "local-lvm:vm-100-disk-boot,size=10G"

	var posted map[string]string

	mux := http.NewServeMux()
	mux.HandleFunc("/api2/json/cluster/resources", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":[{"type":"qemu","vmid":100,"node":"pve-1","name":"worker-1","status":"running"}]}`)
	})
	mux.HandleFunc("/api2/json/nodes/pve-1/qemu/100/status/current", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":{"vmid":100,"status":"running"}}`)
	})
	mux.HandleFunc("/api2/json/nodes/pve-1/qemu/100/config", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost {
			posted = map[string]string{}

			body, err := io.ReadAll(r.Body)
			assert.NoError(t, err)
			assert.NoError(t, json.Unmarshal(body, &posted))
			maps.Copy(config, posted)

			fmt.Fprint(w, `{"data":null}`)

			return
		}

		b, err := json.Marshal(config)
		assert.NoError(t, err)
		fmt.Fprintf(w, `{"data":%s}`, b)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	cl, err := goproxmox.NewAPIClient(srv.URL + "/api2/json")
	require.NoError(t, err)

	vol := volume.NewVolume("cluster-1", "pve-1", "local-lvm", "vm-9999-pvc-123")

	pvInfo, err := AttachVolume(context.Background(), cl, 100, vol, map[string]string{"backup": "0", "ssd": "1"})
	require.NoError(t, err)

	assert.Equal(t, "/dev/disk/by-id/virtio-PVC-ID32", pvInfo["DevicePath"])
	assert.Equal(t, "32", pvInfo["lun"])

	mu.Lock()
	attached := posted["virtio1"]
	mu.Unlock()

	assert.True(t, strings.HasPrefix(attached, "local-lvm:vm-9999-pvc-123,"), attached)
	assert.Contains(t, attached, "serial=PVC-ID32")
	assert.NotContains(t, attached, "wwn=")
	assert.NotContains(t, attached, "ssd=")

	// A second publish finds the disk where it is rather than taking another slot.
	pvInfo, err = AttachVolume(context.Background(), cl, 100, vol, map[string]string{})
	require.NoError(t, err)
	assert.Equal(t, "32", pvInfo["lun"])
}
