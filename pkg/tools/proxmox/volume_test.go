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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	goproxmox "github.com/sergelogvinov/go-proxmox"
	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	volume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"
)

func TestEscapeVolumePath(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		// Normal PVE volume names are unchanged (no-op for the safe charset).
		{"block volname", "vm-9999-disk-0", "vm-9999-disk-0"},
		{"pvc uuid", "vm-9999-pvc-1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d", "vm-9999-pvc-1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d"},
		// Directory-storage names keep their '/' separators, segments escaped individually.
		{"dir storage path preserved", "9999/vm-9999-disk-0.raw", "9999/vm-9999-disk-0.raw"},
		// Injection attempts are neutralized so they cannot alter the API URL.
		{"query injection", "vm-0?delete=1", "vm-0%3Fdelete=1"},
		{"fragment truncation", "vm-0#x", "vm-0%23x"},
		{"space", "vm 0", "vm%200"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, escapeVolumePath(tt.in))
		})
	}
}

// TestMoveQemuDiskRouting pins the request each CopyEndpoint produces: the URL
// it posts to and the body it sends. The three endpoints take the same four
// logical arguments but shape them differently, and two of the differences are
// load bearing in ways a compiler cannot catch:
//
//   - `target` must be the full volid (storage:disk). The token endpoints
//     parse_volume_id it and reject a bare disk name.
//   - `storage` is a body property for proxmod only; the other two carry it in
//     the URI, and PVE rejects a body that duplicates a URI parameter.
//
// The dir-storage rows use the '<vmid>/<name>.<ext>' volname that
// volume.CopyVolume emits for every snapshot on a file-backed storage. They are
// the reason the Perl endpoints' volname allow-list had to admit a leading
// vmid component; see hack/*/t/volname.t.
func TestMoveQemuDiskRouting(t *testing.T) {
	tests := []struct {
		name     string
		endpoint pxpool.CopyEndpoint
		disk     string
		wantPath string
		wantBody map[string]interface{}
	}{
		{
			name:     "builtin block volname",
			endpoint: pxpool.CopyEndpointBuiltin,
			disk:     "vm-9999-disk-0",
			wantPath: "/api2/json/nodes/pve-1/storage/local-lvm/content/vm-9999-disk-0",
			wantBody: map[string]interface{}{
				"node":        "pve-1",
				"target":      "datastore1:vm-9999-disk-0",
				"target_node": "pve-2",
				"volume":      "vm-9999-disk-0",
			},
		},
		{
			name:     "builtin dir-storage volname",
			endpoint: pxpool.CopyEndpointBuiltin,
			disk:     "9999/vm-9999-snapshot-foo.raw",
			wantPath: "/api2/json/nodes/pve-1/storage/local-lvm/content/9999/vm-9999-snapshot-foo.raw",
			wantBody: map[string]interface{}{
				"node":        "pve-1",
				"target":      "datastore1:9999/vm-9999-snapshot-foo.raw",
				"target_node": "pve-2",
				"volume":      "9999/vm-9999-snapshot-foo.raw",
			},
		},
		{
			name:     "csi-copy block volname",
			endpoint: pxpool.CopyEndpointCSICopy,
			disk:     "vm-9999-disk-0",
			// Storage level, NOT under content/ — the content subtree's {volume}
			// matches greedily across '/' and would swallow the method name.
			wantPath: "/api2/json/nodes/pve-1/storage/local-lvm/csi-copy",
			wantBody: map[string]interface{}{
				"target":      "datastore1:vm-9999-disk-0",
				"target_node": "pve-2",
				"volume":      "vm-9999-disk-0",
			},
		},
		{
			name:     "csi-copy dir-storage volname",
			endpoint: pxpool.CopyEndpointCSICopy,
			disk:     "9999/vm-9999-snapshot-foo.raw",
			wantPath: "/api2/json/nodes/pve-1/storage/local-lvm/csi-copy",
			wantBody: map[string]interface{}{
				"target":      "datastore1:9999/vm-9999-snapshot-foo.raw",
				"target_node": "pve-2",
				"volume":      "9999/vm-9999-snapshot-foo.raw",
			},
		},
		{
			name:     "proxmod block volname",
			endpoint: pxpool.CopyEndpointProxmod,
			disk:     "vm-9999-disk-0",
			wantPath: "/api2/json/nodes/pve-1/proxmod/csi-storage/copy",
			wantBody: map[string]interface{}{
				"storage":     "local-lvm",
				"target":      "datastore1:vm-9999-disk-0",
				"target_node": "pve-2",
				"volume":      "vm-9999-disk-0",
			},
		},
		{
			name:     "proxmod dir-storage volname",
			endpoint: pxpool.CopyEndpointProxmod,
			disk:     "9999/vm-9999-snapshot-foo.raw",
			wantPath: "/api2/json/nodes/pve-1/proxmod/csi-storage/copy",
			wantBody: map[string]interface{}{
				"storage":     "local-lvm",
				"target":      "datastore1:9999/vm-9999-snapshot-foo.raw",
				"target_node": "pve-2",
				"volume":      "9999/vm-9999-snapshot-foo.raw",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, reqs := copyRecorder(t, "OK")
			defer srv.Close()

			client, err := goproxmox.NewAPIClient(srv.URL + "/api2/json")
			require.NoError(t, err)

			srcVol := volume.NewVolume("cluster-1", "pve-1", "local-lvm", tt.disk)
			dstVol := volume.NewVolume("cluster-1", "pve-2", "datastore1", tt.disk)

			err = MoveQemuDisk(context.Background(), client, srcVol, "pve-2", dstVol, 60, tt.endpoint)
			require.NoError(t, err)

			require.Len(t, *reqs, 1)
			assert.Equal(t, tt.wantPath, (*reqs)[0].path)
			assert.Equal(t, tt.wantBody, (*reqs)[0].body)
		})
	}
}

// TestMoveQemuDiskFailedTask asserts a task that finishes *with an error* is
// reported as a failure. The pre-delegation copyVolume in pkg/csi discarded the
// status bool and returned nil here, silently reporting a failed snapshot copy
// as a success.
func TestMoveQemuDiskFailedTask(t *testing.T) {
	srv, _ := copyRecorder(t, "unable to create image: got lock timeout")
	defer srv.Close()

	client, err := goproxmox.NewAPIClient(srv.URL + "/api2/json")
	require.NoError(t, err)

	vol := volume.NewVolume("cluster-1", "pve-1", "local-lvm", "vm-9999-disk-0")

	err = MoveQemuDisk(context.Background(), client, vol, "pve-1", vol, 60, pxpool.CopyEndpointBuiltin)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "disk move task failed")
	assert.Contains(t, err.Error(), "got lock timeout")
}

type recordedRequest struct {
	path string
	body map[string]interface{}
}

// copyRecorder stands in for a Proxmox node: it records the copy POST, hands
// back a UPID, and reports that task as already stopped with exitStatus. Since
// WaitForCompleteStatus pings before its first sleep, a completed-on-first-ping
// task keeps the test instant.
func copyRecorder(t *testing.T, exitStatus string) (*httptest.Server, *[]recordedRequest) {
	t.Helper()

	const upid = "UPID:pve-1:00001234:0000ABCD:00000000:imgcopy:9999:root@pam:"

	reqs := &[]recordedRequest{}

	mux := http.NewServeMux()
	mux.HandleFunc("/api2/json/nodes/pve-1/tasks/"+upid+"/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"data":{"status":"stopped","exitstatus":%q,"upid":%q}}`, exitStatus, upid)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)

			return
		}

		raw, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		body := map[string]interface{}{}
		require.NoError(t, json.Unmarshal(raw, &body))

		// r.URL.Path is the decoded path; EscapedPath() is what actually went on
		// the wire, so escaping bugs cannot hide behind the decode.
		*reqs = append(*reqs, recordedRequest{path: r.URL.EscapedPath(), body: body})

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"data":%q}`, upid)
	})

	return httptest.NewServer(mux), reqs
}
