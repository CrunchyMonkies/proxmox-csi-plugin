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

	goproxmox "github.com/sergelogvinov/go-proxmox"
	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	toolsproxmox "github.com/sergelogvinov/proxmox-csi-plugin/pkg/tools/proxmox"
	volume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"
)

const (
	// TaskStatusCheckInterval is the interval in seconds to check the status of a task
	TaskStatusCheckInterval = toolsproxmox.TaskStatusCheckInterval
	// TaskTimeout is the timeout in seconds for all task
	TaskTimeout = toolsproxmox.TaskTimeout

	// ErrorNotFound not found error message
	ErrorNotFound string = toolsproxmox.ErrorNotFound
)

// nolint:unused
func getNodeForVolume(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume) (node string, err error) {
	return toolsproxmox.GetNodeForVolume(ctx, cl, vol)
}

func getVMByAttachedVolume(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume, skipVMID int) (int, int, error) {
	return toolsproxmox.GetVMByAttachedVolume(ctx, cl, vol, skipVMID)
}

func getVolumeSize(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume) (int64, error) {
	return toolsproxmox.GetVolumeSize(ctx, cl, vol)
}

func resolveVolume(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume, reassign bool) (*volume.Volume, int64, error) {
	return toolsproxmox.ResolveVolume(ctx, cl, vol, reassign)
}

func prepareReplication(ctx context.Context, cl *goproxmox.APIClient, node string, name string, vmID int) (int, error) {
	return toolsproxmox.PrepareReplication(ctx, cl, node, name, vmID)
}

func createReplication(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume, params StorageParameters) error {
	return toolsproxmox.CreateReplication(ctx, cl, id, vol, params.ReplicateSchedule, params.ReplicateZones)
}

func migrateReplication(ctx context.Context, cl *goproxmox.APIClient, target int, vol *volume.Volume, vmID int) error {
	return toolsproxmox.MigrateReplication(ctx, cl, target, vol, vmID)
}

func deleteReplication(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume, vmID int) error {
	return toolsproxmox.DeleteReplication(ctx, cl, vol, vmID)
}

func createVolume(ctx context.Context, cl *goproxmox.APIClient, vol *volume.Volume, sizeBytes int64) error {
	return toolsproxmox.CreateVolume(ctx, cl, vol, sizeBytes)
}

func attachVolume(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume, options map[string]string) (map[string]string, error) {
	return toolsproxmox.AttachVolume(ctx, cl, id, vol, options)
}

func detachVolume(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume) error {
	return toolsproxmox.DetachVolume(ctx, cl, id, vol)
}

func clearUnusedDisk(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume) error {
	return toolsproxmox.ClearUnusedDisk(ctx, cl, id, vol)
}

func updateVolume(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume, options map[string]string) error {
	return toolsproxmox.UpdateVolume(ctx, cl, id, vol, options)
}

func copyVolume(
	ctx context.Context,
	cl *goproxmox.APIClient,
	srcVol *volume.Volume,
	destVol *volume.Volume,
	endpoint pxpool.CopyEndpoint,
) error {
	return toolsproxmox.CopyVolume(ctx, cl, srcVol, destVol, endpoint)
}

func waitDetachVolume(ctx context.Context, cl *goproxmox.APIClient, id int, vol *volume.Volume) error {
	return toolsproxmox.WaitDetachVolume(ctx, cl, id, vol)
}
