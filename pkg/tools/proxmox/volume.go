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
	"regexp"
	"strings"
	"time"

	"github.com/luthermonson/go-proxmox"

	goproxmox "github.com/sergelogvinov/go-proxmox"
	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
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
// endpoint selects which server-side copy implementation to post to; the two
// non-builtin choices accept a scoped API token instead of root@pam.
func MoveQemuDisk(
	ctx context.Context,
	cluster *goproxmox.APIClient,
	vol *volume.Volume,
	node string,
	targetVol *volume.Volume,
	taskTimeout int,
	endpoint pxpool.CopyEndpoint,
) error {
	// Copy a volume to another storage/node. Three server-side implementations,
	// all taking the same four logical arguments but shaped differently by where
	// each one is mounted in the API tree.
	var (
		params map[string]interface{}
		path   string
	)

	switch endpoint {
	case pxpool.CopyEndpointProxmod:
		// The proxmod extension from hack/proxmod-csi-storage:
		//   POST /nodes/{node}/proxmod/csi-storage/copy
		// Mounted at node scope inside proxmod's own subtree, where a collision with
		// a PVE-owned route is structurally impossible — so unlike csi-copy below it
		// needs no workaround for the content subtree's greedy {volume} match, and
		// `storage` is an ordinary body property rather than a URI segment. `node`
		// stays out of the body: the URI supplies it, and PVE rejects a request whose
		// body duplicates a URI parameter with a different value.
		//
		// ACL-gated: Datastore.Audit plus check_volume_access on the source,
		// Datastore.AllocateSpace on the target. Requires the proxmox-csi-storage
		// package (and proxmod) on the Proxmox nodes; see
		// hack/proxmod-csi-storage/README.md.
		params = map[string]interface{}{
			"storage":     vol.Storage(),
			"target":      targetVol.VolID(),
			"target_node": node,
			"volume":      vol.Disk(),
		}
		path = fmt.Sprintf("/nodes/%s/proxmod/csi-storage/copy", url.PathEscape(vol.Node()))

	case pxpool.CopyEndpointCSICopy:
		// The permission-gated method added by hack/pve-token-copy:
		//   POST /nodes/{node}/storage/{storage}/csi-copy
		// It lives at the storage level — NOT under content/ — because PVE's router
		// matches the content subtree's {volume} parameter greedily across '/'
		// (fragmentDelimiter), so a content/{volume}/copy path can never route and
		// falls through to the root-only built-in with volume='<vol>/copy'. The body
		// carries ONLY volume/target/target_node: the URI supplies node and storage.
		//
		// Same ACL model as proxmod above. Requires the pve-csi-copy package
		// (>= 0.2.0) on the Proxmox nodes; see hack/pve-token-copy/README.md.
		params = map[string]interface{}{
			"target":      targetVol.VolID(),
			"target_node": node,
			"volume":      vol.Disk(),
		}
		path = fmt.Sprintf("/nodes/%s/storage/%s/csi-copy", url.PathEscape(vol.Node()), url.PathEscape(vol.Storage()))

	case pxpool.CopyEndpointBuiltin:
		fallthrough
	default:
		// PVE's built-in content "copy" method:
		//   POST /nodes/{node}/storage/{storage}/content/{volume}
		// which has no permissions block and is therefore restricted to root@pam
		// ("experimental code - do not use" upstream).
		params = map[string]interface{}{
			"node":        vol.Node(),
			"target":      targetVol.VolID(),
			"target_node": node,
			"volume":      vol.Disk(),
		}
		path = fmt.Sprintf("/nodes/%s/storage/%s/content/%s", url.PathEscape(vol.Node()), url.PathEscape(vol.Storage()), escapeVolumePath(vol.Disk()))
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

// RenameVolume reassigns an unattached volume to targetVMID, renaming it to the
// name that vmid owns (9999/vm-9999-pvc-<uuid>.raw -> 3021/vm-3021-pvc-<uuid>.raw),
// and returns the volume under its new name.
//
// It posts to the proxmod extension from hack/proxmod-csi-storage:
//
//	POST /nodes/{node}/proxmod/csi-storage/rename
//
// which wraps PVE::Storage::rename_volume — the only mechanism PVE has for
// reassigning an *unattached* volume, and one with no REST endpoint of its own.
// PVE's move_disk cannot substitute for it: that method moves a disk key between
// two REAL VMs, and the placeholder vmid a CSI volume carries at rest is not a VM.
// See docs/reassign-volume-on-attach.md.
//
// The call is synchronous — a rename is an in-directory rename(2) on dir/LVM/ZFS,
// so there is no worker to fork and no UPID to poll — and the endpoint refuses a
// volume any VM config still references. Callers must therefore detach before
// renaming back, and must not clear the resulting `unused<n>` key until the rename
// has succeeded: deleting an `unused<n>` whose volume still exists makes PVE
// deallocate it for real.
//
// Requires the proxmox-csi-storage package (>= 0.3.1) and proxmod on the node.
// 0.3.0 will not do: its endpoint does not proxy to the node in the path, so it
// answers about the host the cluster URL points at and reports every volume
// elsewhere as not found.
func RenameVolume(ctx context.Context, cluster *goproxmox.APIClient, vol *volume.Volume, targetVMID int) (*volume.Volume, error) {
	target := vol.WithVMID(targetVMID)
	if target == nil {
		return nil, fmt.Errorf("failed to rename volume: %s is not in the vm-<vmid>-<name> form", vol.VolumeID())
	}

	// `storage` is an ordinary body property; `node` stays out of the body, since
	// the URI supplies it and PVE rejects a body that redeclares a URI parameter.
	// target_volname is the bare filename: rename_volume composes the destination
	// itself as <basedir>/<target_vmid>/<target_volname>.
	params := map[string]interface{}{
		"storage":        vol.Storage(),
		"volume":         vol.Disk(),
		"target_vmid":    targetVMID,
		"target_volname": target.DiskName(),
	}
	path := fmt.Sprintf("/nodes/%s/proxmod/csi-storage/rename", url.PathEscape(vol.Node()))

	var volid string
	if err := cluster.Client.Post(ctx, path, params, &volid); err != nil {
		return nil, fmt.Errorf("failed to rename volume: %v, params=%+v", err, params)
	}

	// Prefer the name PVE reports over the one computed above. A mismatch is not an
	// error to raise: the rename has already happened, and reporting failure for a
	// completed rename would strand the volume under a name no caller expects.
	if storage, disk, ok := strings.Cut(volid, ":"); ok && storage == vol.Storage() && disk != "" {
		target.SetDisk(disk)
	}

	return target, nil
}

// FindVolumeBySuffix locates vol on its node by the part of its name that a
// reassignment does not change, for the case where the volume has been renamed
// for another vmid and no longer matches the name the VolumeID carries.
//
// It returns nil without an error when nothing matches, in the same spirit as
// DiskOnNode's found flag: a volume that is simply absent is an ordinary answer.
//
// The match is anchored on the whole volname rather than a substring, so a
// snapshot carrying the same PVC uuid (vm-9999-pvc-<uuid>-snap1.raw) can never be
// mistaken for the volume itself.
func FindVolumeBySuffix(ctx context.Context, cluster *goproxmox.APIClient, vol *volume.Volume) (*volume.Volume, error) {
	suffix := vol.DiskSuffix()
	if suffix == "" {
		return nil, fmt.Errorf("failed to find volume: %s is not in the vm-<vmid>-<name> form", vol.VolumeID())
	}

	re, err := regexp.Compile(`^(?:[1-9][0-9]{2,8}/)?vm-[1-9][0-9]{2,8}-` + regexp.QuoteMeta(suffix) + `$`)
	if err != nil {
		return nil, fmt.Errorf("failed to find volume: %v", err)
	}

	content := []struct {
		Volid string `json:"volid"`
	}{}

	node := vol.Node()
	if err := cluster.Client.Get(ctx, fmt.Sprintf("/nodes/%s/storage/%s/content", url.PathEscape(node), url.PathEscape(vol.Storage())), &content); err != nil {
		return nil, fmt.Errorf("failed to list storage content on node %s: %v", node, err)
	}

	for _, item := range content {
		storage, disk, ok := strings.Cut(item.Volid, ":")
		if !ok || storage != vol.Storage() || !re.MatchString(disk) {
			continue
		}

		found := volume.NewVolume(vol.Region(), vol.Zone(), vol.Storage(), disk)
		found.SetNode(node)

		return found, nil
	}

	return nil, nil
}
