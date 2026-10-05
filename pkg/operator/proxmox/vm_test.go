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
	testcluster "github.com/sergelogvinov/proxmox-csi-plugin/test/cluster"
	test "github.com/sergelogvinov/proxmox-csi-plugin/test/operator"
)

func TestListVMsUnknownRegion(t *testing.T) {
	_, err := newPool(t).ListVMs(t.Context(), "nowhere")

	require.Error(t, err)
	assert.ErrorIs(t, err, pxpool.ErrRegionNotFound)
}

func TestListVMs(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	testcluster.SetupMockResponders()

	vms, err := newPool(t).ListVMs(t.Context(), "cluster-1")
	require.NoError(t, err)

	// The shared fixture's VMs carry no pool, which is itself the interesting
	// case: a hypervisor with no pools configured -- which is where this estate
	// starts -- must resolve to VMs with an empty Pool rather than to an error,
	// so a tenant registered with an explicit allowlist still works.
	assert.Equal(t, []proxmox.VM{
		{Region: "cluster-1", VMID: 100, Node: "pve-1", Name: "cluster-1-node-1"},
		{Region: "cluster-1", VMID: 101, Node: "pve-2", Name: "cluster-1-node-2"},
	}, vms)

	// The reason this adapter exists in the shape it does.
	test.AssertNoWrites(t)
}

func TestListVMsUnreachable(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	// No responders, so the call fails. The error has to surface: the tenant
	// reconciler keeps its previous VMID resolution on this path and lets it go
	// stale, and it can only tell that apart from "the pool is empty" if an
	// unreachable hypervisor is an error rather than an empty slice.
	_, err := newPool(t).ListVMs(t.Context(), "cluster-1")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "listing VMs")
}

func TestListVMsEmptyCluster(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "https://127.0.0.2:8006/api2/json/cluster/resources",
		httpmock.NewJsonResponderOrPanic(200, map[string]any{"data": []any{}}))

	vms, err := newPool(t).ListVMs(t.Context(), "cluster-1")

	// The client reports an empty result as ErrVirtualMachineNotFound. Letting
	// that through would make a cluster with no VMs look like a cluster that
	// could not be read, which is the difference between a tenant resolving to
	// no VMIDs and a tenant keeping whatever it resolved to last time.
	require.NoError(t, err)
	assert.Empty(t, vms)
}

func TestListVMsSurvivesAFlakyAPI(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	testcluster.SetupMockResponders()

	// The operator retries because the driver's transport does, not because
	// anything in this package asks it to -- the client pool is the driver's and
	// its retrying transport comes with it. That inheritance is invisible here,
	// so this pins it: a refactor giving the operator its own http.Client would
	// still pass every other test in this suite.
	//
	// Two bad answers is inside the transport's three attempts, so this proves
	// the retry is spent rather than merely tolerated. An empty-bodied 596 is
	// what pveproxy answers when it cannot reach pvedaemon, which on this estate
	// meant 33 "unexpected end of JSON input" errors in a fourteen-second window
	// with nothing actually wrong.
	//
	// No cleanup: SetupMockResponders zeroes the counter, and every test in this
	// package calls it.
	testcluster.FailNextReads(2)

	vms, err := newPool(t).ListVMs(t.Context(), "cluster-1")
	require.NoError(t, err)

	assert.Equal(t, []proxmox.VM{
		{Region: "cluster-1", VMID: 100, Node: "pve-1", Name: "cluster-1-node-1"},
		{Region: "cluster-1", VMID: 101, Node: "pve-2", Name: "cluster-1-node-2"},
	}, vms)

	test.AssertNoWrites(t)
}
