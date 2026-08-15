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

package cluster

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/jarcoal/httpmock"
	"github.com/luthermonson/go-proxmox"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
)

// RenameRequest is one call the driver made to the proxmod rename endpoint.
type RenameRequest struct {
	Node          string
	Storage       string
	Volume        string
	TargetVMID    int
	TargetVolname string
}

// AttachRequest is one drive line the driver has asked a VM config POST to add.
type AttachRequest struct {
	VMID   int
	Device string
	// Value is the raw drive line, ie 'local-lvm:vm-9999-pvc-x,backup=0,iops_rd=3000'.
	Value string
}

// Options splits the drive line's comma-separated options off the volid.
func (r AttachRequest) Options() map[string]string {
	options := map[string]string{}

	parts := strings.Split(r.Value, ",")
	for _, part := range parts[1:] {
		k, v, found := strings.Cut(part, "=")
		if found {
			options[k] = v
		}
	}

	return options
}

var (
	mockMu         sync.Mutex
	renameRequests []RenameRequest
	// vm101Unlinked tracks whether the CSI volume attached to VM 101 has been
	// unlinked, so the config responder can answer the way PVE does afterwards:
	// the drive key gone and the volume parked under unused0.
	vm101Unlinked bool
	// attachRequests records the drives added to VM 100 by a config POST, and
	// vm100Attached replays them from the config GET so the attach's own
	// wait-until-visible check can observe what it just wrote.
	attachRequests []AttachRequest
	vm100Attached  map[string]string
	// renameFailure, when non-empty, makes the rename endpoint answer with it
	// instead of renaming. See FailRenames.
	renameFailure string
	// readFailures counts down the GETs that answer badly instead of answering.
	// See FailNextReads.
	readFailures int
)

// FailNextReads makes the next n GETs of the Proxmox API answer with an
// empty-bodied 596, the way pveproxy does when it cannot reach pvedaemon.
//
// This is the shape of the failure that produced 33 "unexpected end of JSON
// input" errors on a live cluster in a fourteen-second window: nothing wrong
// with the volume, just an API answering badly under a burst. Reset by
// SetupMockResponders.
func FailNextReads(n int) {
	mockMu.Lock()
	defer mockMu.Unlock()

	readFailures = n
}

// readFails reports whether this GET is one of the ones FailNextReads asked for,
// consuming it.
func readFails() bool {
	mockMu.Lock()
	defer mockMu.Unlock()

	if readFailures <= 0 {
		return false
	}

	readFailures--

	return true
}

// registerGet registers a GET responder that FailNextReads can make flaky.
func registerGet(path string, responder httpmock.Responder) {
	httpmock.RegisterResponder(http.MethodGet, path, func(req *http.Request) (*http.Response, error) {
		if readFails() {
			res := httpmock.NewStringResponse(596, "")
			res.Status = "596 Connection error"
			res.ContentLength = 0

			return res, nil
		}

		return responder(req)
	})
}

// FailRenames makes the proxmod rename endpoint refuse every call with a 500
// carrying the given message, which is how a node running an extension that
// cannot see the volume answers. Pass "" to restore the successful behavior.
// Reset by SetupMockResponders.
func FailRenames(message string) {
	mockMu.Lock()
	defer mockMu.Unlock()

	renameFailure = message
}

// AttachRequests returns the drives the driver has attached to VM 100 since the
// last SetupMockResponders, in the order it asked for them.
func AttachRequests() []AttachRequest {
	mockMu.Lock()
	defer mockMu.Unlock()

	return append([]AttachRequest(nil), attachRequests...)
}

// RenameRequests returns the proxmod renames the driver has asked for since the
// last ResetRenameRequests. A rename leaves no trace in the RPC response it
// happens under — publish and unpublish both treat it as best-effort — so this
// recording is the only way a test can tell which vmid a volume was renamed to.
func RenameRequests() []RenameRequest {
	mockMu.Lock()
	defer mockMu.Unlock()

	return append([]RenameRequest(nil), renameRequests...)
}

// ResetRenameRequests clears the recorded renames.
func ResetRenameRequests() {
	mockMu.Lock()
	defer mockMu.Unlock()

	renameRequests = nil
}

// SetupMockResponders sets up the HTTP mock responders for Proxmox API calls.
func SetupMockResponders() {
	ResetRenameRequests()

	mockMu.Lock()
	vm101Unlinked = false
	attachRequests = nil
	vm100Attached = map[string]string{}
	renameFailure = ""
	readFailures = 0
	mockMu.Unlock()

	registerGet(`=~/version$`,
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": proxmox.Version{Version: "8.4"},
			})
		})
	registerGet(`=~/cluster/status`,
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": proxmox.NodeStatuses{{Name: "pve-1"}, {Name: "pve-2"}, {Name: "pve-3"}},
			})
		})
	registerGet("=~/cluster/resources",
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": proxmox.ClusterResources{
					&proxmox.ClusterResource{
						Node:   "pve-1",
						Type:   "qemu",
						VMID:   100,
						Name:   "cluster-1-node-1",
						MaxCPU: 4,
						MaxMem: 10 * 1024 * 1024 * 1024,
					},
					&proxmox.ClusterResource{
						Node:   "pve-2",
						Type:   "qemu",
						VMID:   101,
						Name:   "cluster-1-node-2",
						MaxCPU: 2,
						MaxMem: 5 * 1024 * 1024 * 1024,
					},

					&proxmox.ClusterResource{
						ID:         "storage/smb",
						Type:       "storage",
						PluginType: "cifs",
						Node:       "pve-1",
						Storage:    "smb",
						Content:    "rootdir,images",
						Shared:     1,
						Status:     "available",
					},
					&proxmox.ClusterResource{
						ID:         "storage/rbd",
						Type:       "storage",
						PluginType: "dir",
						Node:       "pve-1",
						Storage:    "rbd",
						Content:    "images",
						Shared:     1,
						Status:     "available",
					},
					&proxmox.ClusterResource{
						ID:         "storage/rbd",
						Type:       "storage",
						PluginType: "dir",
						Node:       "pve-2",
						Storage:    "rbd",
						Content:    "images",
						Shared:     1,
						Status:     "available",
					},
					&proxmox.ClusterResource{
						ID:         "storage/zfs",
						Type:       "storage",
						PluginType: "zfspool",
						Node:       "pve-1",
						Storage:    "zfs",
						Content:    "images",
						Status:     "available",
					},
					&proxmox.ClusterResource{
						ID:         "storage/zfs",
						Type:       "storage",
						PluginType: "zfspool",
						Node:       "pve-2",
						Storage:    "zfs",
						Content:    "images",
						Status:     "available",
					},
					&proxmox.ClusterResource{
						ID:         "storage/lvm",
						Type:       "storage",
						PluginType: "lvm",
						Node:       "pve-1",
						Storage:    "local-lvm",
						Content:    "images",
						Status:     "available",
					},
					&proxmox.ClusterResource{
						ID:         "storage/lvm",
						Type:       "storage",
						PluginType: "lvm",
						Node:       "pve-2",
						Storage:    "local-lvm",
						Content:    "images",
						Status:     "available",
					},
				},
			})
		},
	)

	registerGet(`=~/nodes/pve-1/status`,
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": proxmox.Node{},
			})
		})
	registerGet(`=~/nodes/pve-2/status`,
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": proxmox.Node{},
			})
		})
	registerGet(`=~/nodes/pve-3/status`,
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": proxmox.Node{},
			})
		})

	registerGet("=~/nodes$",
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": []proxmox.NodeStatus{
					{
						Node:   "pve-1",
						Status: "online",
					},
					{
						Node:   "pve-2",
						Status: "online",
					},
					{
						Node:   "pve-3",
						Status: "online",
					},
				},
			})
		})

	registerGet(`=~/storage/rbd$`,
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": proxmox.ClusterStorage{
					Type:    "dir",
					Storage: "rbd",
					Shared:  1,
					Content: "images",
				},
			})
		},
	)
	registerGet(`=~/nodes/\S+/storage/rbd/status`,
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": proxmox.Storage{
					Type:    "dir",
					Enabled: 1,
					Active:  1,
					Shared:  1,
					Content: "images",
					Total:   100 * 1024 * 1024 * 1024,
					Used:    50 * 1024 * 1024 * 1024,
					Avail:   50 * 1024 * 1024 * 1024,
				},
			})
		},
	)
	registerGet(`=~/nodes/\S+/storage/zfs/status`,
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": proxmox.Storage{
					Type:    "zfspool",
					Enabled: 1,
					Active:  1,
					Content: "images",
					Total:   100 * 1024 * 1024 * 1024,
					Used:    50 * 1024 * 1024 * 1024,
					Avail:   50 * 1024 * 1024 * 1024,
				},
			})
		},
	)
	registerGet(`=~/nodes/\S+/storage/local-lvm/status`,
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": proxmox.Storage{
					Type:    "lvmthin",
					Enabled: 1,
					Active:  1,
					Content: "images",
					Total:   100 * 1024 * 1024 * 1024,
					Used:    50 * 1024 * 1024 * 1024,
					Avail:   50 * 1024 * 1024 * 1024,
				},
			})
		},
	)
	registerGet(`=~/nodes/\S+/storage/\S+/status`,
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(400, map[string]any{
				"data":    nil,
				"message": "Parameter verification failed",
				"errors": map[string]string{
					"storage": "No such storage.",
				},
			})
		},
	)

	registerGet(`=~/nodes/\S+/storage/smb/content`,
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": []proxmox.StorageContent{
					{
						Format: "raw",
						Volid:  "smb:9999/vm-9999-volume-smb.raw",
						VMID:   9999,
						Size:   1024 * 1024 * 1024,
					},
				},
			})
		},
	)
	registerGet(`=~/nodes/\S+/storage/rbd/content`,
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": []proxmox.StorageContent{
					{
						Format: "raw",
						Volid:  "rbd:9999/vm-9999-volume-rbd.raw",
						VMID:   9999,
						Size:   1024 * 1024 * 1024,
					},
				},
			})
		},
	)
	registerGet(`=~/nodes/\S+/storage/local-lvm/content`,
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": []proxmox.StorageContent{
					{
						Format: "raw",
						Size:   uint64(csi.MinChunkSizeBytes),
						Volid:  "local-lvm:vm-9999-pvc-123",
					},
					{
						Format: "raw",
						Size:   5 * 1024 * 1024 * 1024,
						Volid:  "local-lvm:vm-9999-pvc-exist",
					},
					{
						Format: "raw",
						Size:   uint64(csi.MinChunkSizeBytes),
						Volid:  "local-lvm:vm-9999-pvc-exist-same-size",
					},
					{
						Format: "raw",
						Size:   1024 * 1024 * 1024,
						Volid:  "local-lvm:vm-9999-pvc-error",
					},
					{
						Format: "raw",
						Size:   1024 * 1024 * 1024,
						Volid:  "local-lvm:vm-9999-pvc-unpublished",
					},
					{
						// Attached to VM 101 and already renamed for it, so it appears
						// here under 101 and not under the 9999 its volumeHandle carries.
						Format: "raw",
						Size:   uint64(csi.MinChunkSizeBytes),
						Volid:  "local-lvm:vm-101-pvc-reassigned",
						VMID:   101,
					},
				},
			})
		},
	)
	registerGet(`=~/nodes/\S+/storage/\S+/content`,
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(500, map[string]any{
				"data":    nil,
				"message": "storage does not exist",
			})
		},
	)

	registerGet(`=~/nodes/pve-1/qemu$`,
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": []proxmox.VirtualMachine{
					{
						VMID:   100,
						Status: "running",
						Name:   "cluster-1-node-1",
						Node:   "pve-1",
					},
				},
			})
		})
	registerGet(`=~/nodes/pve-2/qemu$`,
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": []proxmox.VirtualMachine{
					{
						VMID:   101,
						Status: "running",
						Name:   "cluster-1-node-2",
						Node:   "pve-2",
					},
				},
			})
		})
	registerGet(`=~/nodes/\S+/qemu/100/status/current`,
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": proxmox.VirtualMachine{
					VMID:   100,
					Name:   "cluster-1-node-1",
					Node:   "pve-1",
					Status: "running",
				},
			})
		},
	)
	registerGet(`=~/nodes/\S+/qemu/100/config`,
		func(_ *http.Request) (*http.Response, error) {
			config := map[string]interface{}{
				"vmid":    100,
				"scsi0":   "local-lvm:vm-100-disk-0,size=10G",
				"scsi1":   "local-lvm:vm-9999-pvc-123,backup=0,iothread=1,wwn=0x5056432d49443031",
				"smbios1": "uuid=11833f4c-341f-4bd3-aad7-f7abed000000",
			}

			mockMu.Lock()
			for device, value := range vm100Attached {
				config[device] = value
			}
			mockMu.Unlock()

			return httpmock.NewJsonResponse(200, map[string]interface{}{"data": config})
		},
	)

	// Attaching a disk to VM 100. Records the drive line and replays it from the
	// config GET above, which is what lets a test observe the options the driver
	// chose — they appear nowhere in the RPC response.
	taskPve1Config := &proxmox.Task{
		UPID:       "UPID:pve-1:003B4237:1DF4ABCC:667C1C47:qmconfig:100:root@pam:",
		Type:       "qmconfig",
		User:       "root",
		Status:     "stopped",
		ExitStatus: "OK",
		Node:       "pve-1",
		IsRunning:  false,
	}

	httpmock.RegisterResponder(http.MethodPost, `=~/nodes/\S+/qemu/100/config`,
		func(req *http.Request) (*http.Response, error) {
			params := map[string]interface{}{}
			if err := json.NewDecoder(req.Body).Decode(&params); err != nil {
				return httpmock.NewJsonResponse(400, map[string]any{"data": nil})
			}

			mockMu.Lock()

			for device, value := range params {
				line, ok := value.(string)
				if !ok || !strings.HasPrefix(device, "scsi") {
					continue
				}

				vm100Attached[device] = line
				attachRequests = append(attachRequests, AttachRequest{VMID: 100, Device: device, Value: line})
			}
			mockMu.Unlock()

			return httpmock.NewJsonResponse(200, map[string]any{"data": taskPve1Config.UPID})
		},
	)
	registerGet(fmt.Sprintf(`=~/nodes/%s/tasks/%s/status`, "pve-1", string(taskPve1Config.UPID)),
		httpmock.NewJsonResponderOrPanic(200, map[string]any{"data": taskPve1Config}))

	registerGet(`=~/nodes/\S+/qemu/101/status/current`,
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": proxmox.VirtualMachine{
					VMID:   101,
					Name:   "cluster-1-node-2",
					Node:   "pve-2",
					Status: "running",
				},
			})
		},
	)
	registerGet(`=~/nodes/\S+/qemu/101/config`,
		func(_ *http.Request) (*http.Response, error) {
			config := map[string]interface{}{
				"vmid":  101,
				"scsi0": "local-lvm:vm-101-disk-0,size=10G",
				"scsi1": "local-lvm:vm-101-disk-1,size=1G",
				"scsi2": "rbd:9999/vm-9999-volume-rbd.raw,backup=0,iothread=1",
				"scsi3": "local-lvm:vm-101-disk-2,size=1G",
				// A CSI volume that reassignVolumeOnAttach has already renamed onto
				// this VM: its name carries 101, while the PV's volumeHandle still
				// says 9999.
				"scsi4":   "local-lvm:vm-101-pvc-reassigned,backup=0,iothread=1",
				"smbios1": "uuid=11833f4c-341f-4bd3-aad7-f7abed000001",
			}

			mockMu.Lock()
			unlinked := vm101Unlinked
			mockMu.Unlock()

			if unlinked {
				// Unlinking a volume the VM owns does not remove it, it parks it.
				delete(config, "scsi4")
				config["unused0"] = "local-lvm:vm-101-pvc-reassigned"
			}

			return httpmock.NewJsonResponse(200, map[string]interface{}{"data": config})
		},
	)

	registerGet("https://127.0.0.2:8006/api2/json/nodes/pve-3/qemu/100/config",
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]interface{}{
				"data": map[string]interface{}{
					"vmid":    100,
					"smbios1": "uuid=11833f4c-341f-4bd3-aad7-f7abea000000",
				},
			})
		},
	)

	httpmock.RegisterResponder("PUT", "https://127.0.0.1:8006/api2/json/nodes/pve-1/qemu/100/resize",
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": "",
			})
		},
	)

	httpmock.RegisterResponder("PUT", "https://127.0.0.1:8006/api2/json/nodes/pve-2/qemu/101/resize",
		func(_ *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]any{
				"data": "",
			})
		},
	)

	task := &proxmox.Task{
		UPID:      "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:csi:103:root@pam:",
		Type:      "delete",
		User:      "root",
		Status:    "completed",
		Node:      "pve-1",
		IsRunning: false,
	}

	taskErr := &proxmox.Task{
		UPID:       "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:csi:104:root@pam:",
		Type:       "delete",
		User:       "root",
		Status:     "stopped",
		ExitStatus: "ERROR",
		Node:       "pve-1",
		IsRunning:  false,
	}

	registerGet(fmt.Sprintf(`=~/nodes/%s/tasks/%s/status`, "pve-1", string(task.UPID)),
		httpmock.NewJsonResponderOrPanic(200, map[string]any{"data": task}))
	registerGet(fmt.Sprintf(`=~/nodes/%s/tasks/%s/status`, "pve-1", string(taskErr.UPID)),
		httpmock.NewJsonResponderOrPanic(200, map[string]any{"data": taskErr}))

	// Detaching a disk from VM 101, for the reassign paths that have to unlink a
	// volume before they can rename it back to the placeholder vmid.
	taskPve2 := &proxmox.Task{
		UPID:       "UPID:pve-2:003B4236:1DF4ABCB:667C1C46:qmconfig:101:root@pam:",
		Type:       "qmconfig",
		User:       "root",
		Status:     "stopped",
		ExitStatus: "OK",
		Node:       "pve-2",
		IsRunning:  false,
	}

	httpmock.RegisterResponder(http.MethodPut, `=~/nodes/pve-2/qemu/101/unlink`,
		func(_ *http.Request) (*http.Response, error) {
			mockMu.Lock()
			vm101Unlinked = true
			mockMu.Unlock()

			return httpmock.NewJsonResponse(200, map[string]any{"data": taskPve2.UPID})
		})
	registerGet(fmt.Sprintf(`=~/nodes/%s/tasks/%s/status`, "pve-2", string(taskPve2.UPID)),
		httpmock.NewJsonResponderOrPanic(200, map[string]any{"data": taskPve2}))

	httpmock.RegisterResponder(http.MethodDelete, `=~/nodes/pve-1/storage/local-lvm/content/vm-9999-pvc-123`,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{"data": task.UPID}).Times(1))
	httpmock.RegisterResponder(http.MethodDelete, `=~/nodes/pve-1/storage/local-lvm/content/vm-9999-pvc-error`,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{"data": taskErr.UPID}).Times(1))

	// The proxmod rename endpoint: records the call and answers with the volid the
	// real endpoint would return, so the driver's post-rename bookkeeping runs.
	httpmock.RegisterResponder(http.MethodPost, `=~/nodes/(\S+)/proxmod/csi-storage/rename`,
		func(req *http.Request) (*http.Response, error) {
			var params struct {
				Storage       string `json:"storage"`
				Volume        string `json:"volume"`
				TargetVMID    int    `json:"target_vmid"`
				TargetVolname string `json:"target_volname"`
			}

			if err := json.NewDecoder(req.Body).Decode(&params); err != nil {
				return httpmock.NewJsonResponse(400, map[string]any{"data": nil})
			}

			node, err := httpmock.GetSubmatch(req, 1)
			if err != nil {
				return httpmock.NewJsonResponse(500, map[string]any{"data": nil})
			}

			mockMu.Lock()

			renameRequests = append(renameRequests, RenameRequest{
				Node:          node,
				Storage:       params.Storage,
				Volume:        params.Volume,
				TargetVMID:    params.TargetVMID,
				TargetVolname: params.TargetVolname,
			})
			failure := renameFailure
			mockMu.Unlock()

			if failure != "" {
				// PVE reports an endpoint's die() in the HTTP status line, which is
				// where the client reads it from — hence the overwrite rather than an
				// error body.
				resp, err := httpmock.NewJsonResponse(500, map[string]any{"data": nil})
				if err != nil {
					return nil, err
				}

				resp.Status = fmt.Sprintf("500 %s", failure)

				return resp, nil
			}

			return httpmock.NewJsonResponse(200,
				map[string]any{"data": fmt.Sprintf("%s:%s", params.Storage, params.TargetVolname)})
		})
}
