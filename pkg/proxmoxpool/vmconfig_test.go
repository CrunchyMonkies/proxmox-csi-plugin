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

package proxmoxpool_test

import (
	"context"
	"testing"

	"github.com/jarcoal/httpmock"
	proxmox "github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	goproxmox "github.com/sergelogvinov/go-proxmox"
	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
)

const (
	vmUUID100 = "11111111-1111-1111-1111-111111111111"
	vmUUID101 = "22222222-2222-2222-2222-222222222222"
)

// mockVMCluster registers a two-VM cluster and returns the pool addressing it.
func mockVMCluster(t *testing.T) *pxpool.ProxmoxPool {
	t.Helper()

	httpmock.Activate()
	t.Cleanup(httpmock.DeactivateAndReset)

	httpmock.RegisterResponder("GET", "=~/version$",
		httpmock.NewJsonResponderOrPanic(200, map[string]any{"data": proxmox.Version{Version: "8.4"}}))

	httpmock.RegisterResponder("GET", "=~/cluster/resources",
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": proxmox.ClusterResources{
				{Node: "pve-1", Type: "qemu", VMID: 100, Name: "worker-1", Status: "running"},
				{Node: "pve-2", Type: "qemu", VMID: 101, Name: "worker-2", Status: "running"},
			},
		}))

	for vmid, uuid := range map[int]string{100: vmUUID100, 101: vmUUID101} {
		httpmock.RegisterResponder("GET", regexpForVMConfig(vmid),
			httpmock.NewJsonResponderOrPanic(200, map[string]any{
				"data": proxmox.VirtualMachineConfig{SMBios1: "uuid=" + uuid},
			}))
	}

	pool, err := pxpool.NewProxmoxPool([]*pxpool.ProxmoxCluster{{
		URL:         "https://127.0.0.1:8006/api2/json",
		TokenID:     "user!token-id",
		TokenSecret: "secret",
		Region:      "cluster-1",
	}})
	require.NoError(t, err)

	return pool
}

func regexpForVMConfig(vmid int) string {
	switch vmid {
	case 100:
		return `=~/nodes/pve-1/qemu/100/config$`
	default:
		return `=~/nodes/pve-2/qemu/101/config$`
	}
}

func TestGetVMConfigByResource(t *testing.T) {
	pool := mockVMCluster(t)

	cl, err := pool.GetProxmoxCluster("cluster-1")
	require.NoError(t, err)

	rs := &proxmox.ClusterResource{Node: "pve-1", Type: "qemu", VMID: 100, Status: "running"}

	httpmock.ZeroCallCounters()

	vm, err := pxpool.GetVMConfigByResource(context.Background(), cl, rs)
	require.NoError(t, err)

	assert.Equal(t, vmUUID100, goproxmox.GetVMUUID(vm))
	assert.Equal(t, "pve-1", vm.Node)

	// The point of the helper: GetVMConfig costs three GETs per VM, this costs one.
	assert.Equal(t, 1, httpmock.GetTotalCallCount())
}

func TestGetVMConfigByResourceUnreachable(t *testing.T) {
	pool := mockVMCluster(t)

	cl, err := pool.GetProxmoxCluster("cluster-1")
	require.NoError(t, err)

	rs := &proxmox.ClusterResource{Node: "pve-1", Type: "qemu", VMID: 100, Status: "unknown"}

	httpmock.ZeroCallCounters()

	_, err = pxpool.GetVMConfigByResource(context.Background(), cl, rs)

	// Same error GetVMConfig raised, and still without touching the network — a
	// node Proxmox cannot reach is not worth a request that has to time out.
	require.ErrorIs(t, err, goproxmox.ErrVirtualMachineUnreachable)
	assert.Equal(t, 0, httpmock.GetTotalCallCount())
}

func TestFindVMByUUIDRequestCount(t *testing.T) {
	pool := mockVMCluster(t)

	httpmock.ZeroCallCounters()

	vmID, region, err := pool.FindVMByUUID(context.Background(), vmUUID101)
	require.NoError(t, err)
	assert.Equal(t, 101, vmID)
	assert.Equal(t, "cluster-1", region)

	// FindVMByUUID has no name prefilter, so it reads every qemu VM in the
	// cluster. One cluster/resources listing plus one config GET per VM up to the
	// match — nine before this change, because each VM also cost a
	// /status/current and a duplicate /config.
	counts := httpmock.GetCallCountInfo()
	assert.Equal(t, 1, counts["GET =~/nodes/pve-1/qemu/100/config$"])
	assert.Equal(t, 1, counts["GET =~/nodes/pve-2/qemu/101/config$"])
	assert.Equal(t, 3, httpmock.GetTotalCallCount())
}
