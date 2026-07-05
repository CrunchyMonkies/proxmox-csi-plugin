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
	"fmt"
	"strings"

	"github.com/luthermonson/go-proxmox"

	goproxmox "github.com/sergelogvinov/go-proxmox"
)

// HelperVMName is the name of the transient VM used to convert qcow2/vmdk
// volumes to raw before migration. The helper VM ID intentionally differs
// from the volume's owner VM ID: Proxmox only ever frees volumes OWNED by a
// VM being destroyed, so destroying the helper can never delete the original
// volume — only its own disposable conversion copy.
const HelperVMName = "csi-migration-helper"

// helperDiskSlot is the config slot the volume is attached to on the helper
// VM. The helper is always created diskless, so the slot is always free.
const helperDiskSlot = "scsi0"

// FindVMNode returns the Proxmox node hosting the given VM ID and the VM's
// name, if it exists.
func FindVMNode(ctx context.Context, cluster *goproxmox.APIClient, vmid int) (node, name string, found bool, err error) {
	resources := []struct {
		Type string `json:"type"`
		VMID int    `json:"vmid"`
		Node string `json:"node"`
		Name string `json:"name"`
	}{}

	if err := cluster.Client.Get(ctx, "/cluster/resources?type=vm", &resources); err != nil {
		return "", "", false, fmt.Errorf("failed to list cluster resources: %v", err)
	}

	for _, r := range resources {
		if r.Type == "qemu" && r.VMID == vmid {
			return r.Node, r.Name, true, nil
		}
	}

	return "", "", false, nil
}

// CreateHelperVM creates a minimal, diskless, stopped VM used only to carry
// a volume through a format conversion.
func CreateHelperVM(ctx context.Context, cluster *goproxmox.APIClient, node string, vmid int) error {
	params := map[string]interface{}{
		"vmid":   vmid,
		"name":   HelperVMName,
		"memory": 32,
		"cores":  1,
		"start":  0,
	}

	var upid proxmox.UPID
	if err := cluster.Client.Post(ctx, fmt.Sprintf("/nodes/%s/qemu", node), params, &upid); err != nil {
		return fmt.Errorf("failed to create helper VM %d on %s: %v", vmid, node, err)
	}

	return waitTask(ctx, cluster, upid, 60, "create helper VM")
}

// DeleteVM deletes a VM. Proxmox frees only volumes OWNED by the VM
// (vm-<vmid>-… names); volumes owned by other VMs are never touched, even
// when referenced in the config.
func DeleteVM(ctx context.Context, cluster *goproxmox.APIClient, node string, vmid int) error {
	var upid proxmox.UPID
	if err := cluster.Client.Delete(ctx, fmt.Sprintf("/nodes/%s/qemu/%d", node, vmid), &upid); err != nil {
		return fmt.Errorf("failed to delete VM %d on %s: %v", vmid, node, err)
	}

	return waitTask(ctx, cluster, upid, 60, "delete VM")
}

// AttachDisk attaches an existing volume to the helper VM's disk slot.
func AttachDisk(ctx context.Context, cluster *goproxmox.APIClient, node string, vmid int, volid string) error {
	params := map[string]interface{}{
		helperDiskSlot: volid,
	}

	if err := cluster.Client.Put(ctx, fmt.Sprintf("/nodes/%s/qemu/%d/config", node, vmid), params, nil); err != nil {
		return fmt.Errorf("failed to attach %s to VM %d on %s: %v", volid, vmid, node, err)
	}

	return nil
}

// ConvertDiskToRaw converts the helper VM's attached disk to raw format on
// the given storage via the move_disk API (file-to-file qemu-img conversion,
// which works where the streaming copy endpoint does not). The source volume
// is kept (delete=0) and, being foreign to the helper, is dropped from the
// config without any unused entry. Returns the volid of the converted copy.
func ConvertDiskToRaw(ctx context.Context, cluster *goproxmox.APIClient, node string, vmid int, storage string, taskTimeout int) (string, error) {
	params := map[string]interface{}{
		"disk":    helperDiskSlot,
		"storage": storage,
		"format":  "raw",
		"delete":  0,
	}

	var upid proxmox.UPID
	if err := cluster.Client.Post(ctx, fmt.Sprintf("/nodes/%s/qemu/%d/move_disk", node, vmid), params, &upid); err != nil {
		return "", fmt.Errorf("failed to start disk conversion on VM %d: %v", vmid, err)
	}

	if err := waitTask(ctx, cluster, upid, taskTimeout, "convert disk"); err != nil {
		return "", err
	}

	// The converted copy's volid is whatever the slot points at now.
	config := struct {
		SCSI0 string `json:"scsi0"`
	}{}

	if err := cluster.Client.Get(ctx, fmt.Sprintf("/nodes/%s/qemu/%d/config", node, vmid), &config); err != nil {
		return "", fmt.Errorf("failed to read VM %d config after conversion: %v", vmid, err)
	}

	volid := strings.SplitN(config.SCSI0, ",", 2)[0]
	if volid == "" {
		return "", fmt.Errorf("no disk found on VM %d after conversion", vmid)
	}

	return volid, nil
}

// waitTask waits for a Proxmox task and fails when the task completed with
// an error (a completed task is not necessarily a successful one).
func waitTask(ctx context.Context, cluster *goproxmox.APIClient, upid proxmox.UPID, taskTimeout int, what string) error {
	task := proxmox.NewTask(upid, cluster.Client)
	if task == nil {
		return nil
	}

	interval := 5
	if taskTimeout > 300 {
		interval = 15
	}

	status, completed, err := task.WaitForCompleteStatus(ctx, taskTimeout/interval, interval)
	if err != nil {
		return fmt.Errorf("failed to wait for %s task: %w", what, err)
	}

	if !completed {
		return fmt.Errorf("%s task did not complete within %d seconds", what, taskTimeout)
	}

	if !status {
		return fmt.Errorf("%s task failed, exit status: %s", what, task.ExitStatus)
	}

	return nil
}
