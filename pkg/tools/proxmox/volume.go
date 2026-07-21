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
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/luthermonson/go-proxmox"

	goproxmox "github.com/sergelogvinov/go-proxmox"
	volume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"
)

// escapeVolumePath percent-escapes each '/'-separated segment of a Proxmox volume or
// disk identifier so a crafted value cannot inject extra path segments or a query
// string into the API URL (the REST client concatenates paths without escaping). It
// preserves the '/' separators used by directory-storage volume names; for the normal
// PVE volume-name character set it is a no-op.
func escapeVolumePath(s string) string {
	parts := strings.Split(s, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}

	return strings.Join(parts, "/")
}

// WaitForVolumeDetach waits for the volume to be detached from the VM.
// vmID is the Proxmox VM ID of the Kubernetes node that was using the volume.
// If vmID is 0, the check is skipped (volume was not in use).
func WaitForVolumeDetach(ctx context.Context, client *goproxmox.APIClient, vmID int, pvc string) error {
	if vmID == 0 {
		return nil
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		vmConfig, err := client.GetVMConfig(ctx, vmID)
		if err != nil {
			if errors.Is(err, goproxmox.ErrVirtualMachineNotFound) {
				return nil
			}

			return fmt.Errorf("failed to get vm config for VMID %d: %v", vmID, err)
		}

		found := false

		disks := vmConfig.VirtualMachineConfig.MergeSCSIs()
		for _, disk := range disks {
			if strings.Contains(disk, pvc) {
				found = true

				break
			}
		}

		if !found {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// DiskOnNode checks whether the volume's disk exists in the storage content
// of the given Proxmox node, returning its size when found. The size lets
// callers distinguish a fully transferred disk from a partial file left by
// an interrupted move.
func DiskOnNode(ctx context.Context, cluster *goproxmox.APIClient, vol *volume.Volume, node string) (bool, int64, error) {
	content := []struct {
		Volid string `json:"volid"`
		Size  int64  `json:"size"`
	}{}

	if err := cluster.Client.Get(ctx, fmt.Sprintf("/nodes/%s/storage/%s/content", url.PathEscape(node), url.PathEscape(vol.Storage())), &content); err != nil {
		return false, 0, fmt.Errorf("failed to list storage content on node %s: %v", node, err)
	}

	for _, item := range content {
		if item.Volid == vol.VolID() || strings.HasSuffix(item.Volid, "/"+vol.Disk()) {
			return true, item.Size, nil
		}
	}

	return false, 0, nil
}

// DeleteDisk removes the volume's disk from the given Proxmox node, waiting
// for the deletion task. Used to clean up partial files left by interrupted
// moves before retrying.
func DeleteDisk(ctx context.Context, cluster *goproxmox.APIClient, vol *volume.Volume, node string, taskTimeout int) error {
	var upid proxmox.UPID
	if err := cluster.Client.Delete(ctx, fmt.Sprintf("/nodes/%s/storage/%s/content/%s", url.PathEscape(node), url.PathEscape(vol.Storage()), escapeVolumePath(vol.Disk())), &upid); err != nil {
		return fmt.Errorf("failed to delete disk %s on node %s: %v", vol.Disk(), node, err)
	}

	task := proxmox.NewTask(upid, cluster.Client)
	if task != nil {
		status, completed, err := task.WaitForCompleteStatus(ctx, taskTimeout/5, 5)
		if err != nil {
			return fmt.Errorf("failed to wait for disk delete task: %w", err)
		}

		if !completed || !status {
			return fmt.Errorf("disk delete task failed, exit status: %s", task.ExitStatus)
		}
	}

	return nil
}

// MoveQemuDisk moves the volume to the given node, into the storage and disk
// name carried by targetVol (the API's target parameter accepts a full volume
// identifier in storage:disk form, allowing cross-storage moves and renames).
// With tokenEndpoint it posts to the pve-csi-copy package's token-authorized
// /nodes/{node}/storage/{storage}/csi-copy method instead of the root@pam-only
// built-in content copy.
func MoveQemuDisk(ctx context.Context, cluster *goproxmox.APIClient, vol *volume.Volume, node string, targetVol *volume.Volume, taskTimeout int, tokenEndpoint bool) error {
	// Copy a volume to another storage/node.
	//
	// By default this uses PVE's built-in content "copy" method:
	//   POST /nodes/{node}/storage/{storage}/content/{volume}
	// which has no permissions block and is therefore restricted to root@pam
	// ("experimental code - do not use" upstream).
	//
	// When tokenEndpoint is set, it targets the permission-gated method added by
	// hack/pve-token-copy (POST .../storage/{storage}/csi-copy, the source volume in
	// the body). It lives at the storage level — NOT under content/ — because PVE's
	// router matches the content subtree's {volume} parameter greedily across '/'
	// (fragmentDelimiter), so a content/{volume}/copy path can never route and falls
	// through to the root-only built-in with volume='<vol>/copy'. The body carries
	// ONLY volume/target/target_node: the URI supplies node (and storage), and PVE
	// rejects a request whose body duplicates a URI parameter with a different
	// value. The method is authorized by PVE ACL — Datastore.Audit on the source and
	// Datastore.AllocateSpace on the target — so a scoped API token performs the
	// copy and no root@pam credential is needed. It must be installed on the Proxmox
	// nodes (the pve-csi-copy package, >= 0.2.0); see hack/pve-token-copy/README.md.
	params := map[string]interface{}{
		"node":        vol.Node(),
		"target":      targetVol.VolID(),
		"target_node": node,
		"volume":      vol.Disk(),
	}
	path := fmt.Sprintf("/nodes/%s/storage/%s/content/%s", url.PathEscape(vol.Node()), url.PathEscape(vol.Storage()), escapeVolumePath(vol.Disk()))

	if tokenEndpoint {
		params = map[string]interface{}{
			"target":      targetVol.VolID(),
			"target_node": node,
			"volume":      vol.Disk(),
		}
		path = fmt.Sprintf("/nodes/%s/storage/%s/csi-copy", url.PathEscape(vol.Node()), url.PathEscape(vol.Storage()))
	}

	var upid proxmox.UPID
	if err := cluster.Client.Post(ctx, path, params, &upid); err != nil {
		return fmt.Errorf("failed to copy pvc: %v, params=%+v", err, params)
	}

	task := proxmox.NewTask(upid, cluster.Client)
	if task != nil {
		status, completed, err := task.WaitForCompleteStatus(ctx, taskTimeout/15, 15)
		if err != nil {
			return fmt.Errorf("failed to wait for disk move task: %w", err)
		}

		if !completed {
			return fmt.Errorf("disk move task did not complete within %d seconds", taskTimeout)
		}

		// A completed task is not necessarily a successful one: status is
		// false when the task finished with an error.
		if !status {
			return fmt.Errorf("disk move task failed, exit status: %s", task.ExitStatus)
		}
	}

	return nil
}
