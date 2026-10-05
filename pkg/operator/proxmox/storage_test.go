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

const gib = 1024 * 1024 * 1024

// newPool builds a reader over the shared mock hypervisor. Both regions point at
// the same fixture; only cluster-1 is exercised, cluster-2 is there so the
// region-ordering assertion has something to order.
func newPool(t *testing.T) *proxmox.Pool {
	t.Helper()

	pool, err := pxpool.NewProxmoxPool([]*pxpool.ProxmoxCluster{
		{
			URL:         "https://127.0.0.1:8006/api2/json",
			TokenID:     "user!token-id",
			TokenSecret: "secret",
			Region:      "cluster-2",
		},
		{
			URL:         "https://127.0.0.2:8006/api2/json",
			TokenID:     "user!token-id",
			TokenSecret: "secret",
			Region:      "cluster-1",
		},
	})
	require.NoError(t, err)

	return proxmox.NewPool(pool)
}

func TestRegionsAreSorted(t *testing.T) {
	// Deterministic order is what makes the published catalog diffable between
	// syncs; the pool itself makes no promise about map iteration order.
	assert.Equal(t, []string{"cluster-1", "cluster-2"}, newPool(t).Regions())
}

func TestListStoragesUnknownRegion(t *testing.T) {
	_, err := newPool(t).ListStorages(t.Context(), "nowhere")

	require.Error(t, err)
	assert.ErrorIs(t, err, pxpool.ErrRegionNotFound)
}

func TestListStorages(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	testcluster.SetupMockResponders()

	storages, err := newPool(t).ListStorages(t.Context(), "cluster-1")
	require.NoError(t, err)

	// The whole catalog, in the order it is published. Asserting the full slice
	// rather than picking at fields is the point: this is the list a human
	// compares to `pvesm status`, so a storage silently appearing or vanishing
	// has to fail the test.
	expected := []proxmox.Storage{
		{
			Region: "cluster-1", Zone: "pve-1", Name: "local-lvm",
			PluginType: "lvm",
			Types:      []string{"images"}, Active: true,
			TotalBytes: 100 * gib, AvailableBytes: 50 * gib, UsedBytes: 50 * gib,
		},
		{
			Region: "cluster-1", Zone: "pve-2", Name: "local-lvm",
			PluginType: "lvm",
			Types:      []string{"images"}, Active: true,
			TotalBytes: 100 * gib, AvailableBytes: 50 * gib, UsedBytes: 50 * gib,
		},
		{
			// Shared, so one entry with no zone even though the cluster listing
			// reports it once per node.
			Region: "cluster-1", Zone: "", Name: "rbd", Shared: true,
			PluginType: "dir",
			Types:      []string{"images"}, Active: true,
			TotalBytes: 100 * gib, AvailableBytes: 50 * gib, UsedBytes: 50 * gib,
		},
		{
			// Listed by the cluster but with no status endpoint, which is how the
			// mock reproduces a storage type PVE will not answer for. It stays in
			// the catalog, inactive and unmeasured, rather than disappearing.
			Region: "cluster-1", Zone: "", Name: "smb", Shared: true,
			PluginType: "cifs",
			Types:      []string{"images", "rootdir"}, Active: false,
		},
		{
			Region: "cluster-1", Zone: "pve-1", Name: "zfs",
			PluginType: "zfspool",
			Types:      []string{"images"}, Active: true,
			TotalBytes: 100 * gib, AvailableBytes: 50 * gib, UsedBytes: 50 * gib,
		},
		{
			Region: "cluster-1", Zone: "pve-2", Name: "zfs",
			PluginType: "zfspool",
			Types:      []string{"images"}, Active: true,
			TotalBytes: 100 * gib, AvailableBytes: 50 * gib, UsedBytes: 50 * gib,
		},
	}

	require.Len(t, storages, len(expected))

	for i := range expected {
		actual := storages[i]

		if expected[i].Name == "smb" {
			// The error text is the hypervisor's and not worth pinning; that it is
			// set, and that it is what turned Active off, is the contract.
			assert.Error(t, actual.StatusErr, "smb should report why it could not be measured")

			actual.StatusErr = nil
		} else {
			assert.NoError(t, actual.StatusErr, actual.Name)
		}

		assert.Equal(t, expected[i], actual)
	}

	// The reason this adapter exists in the shape it does.
	test.AssertNoWrites(t)
}

func TestListStoragesUnreachable(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	// No responders registered, so every call fails. The listing error must
	// surface: the publisher distinguishes "the region reported nothing" from
	// "the region could not be asked", and it can only do that if this returns
	// an error rather than an empty slice.
	_, err := newPool(t).ListStorages(t.Context(), "cluster-1")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "listing storages")
}

func TestAStorageANodeDoesNotServeIsClassifiedAsAbsent(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	testcluster.SetupMockResponders()

	storages, err := newPool(t).ListStorages(t.Context(), "cluster-1")
	require.NoError(t, err)

	// smb is listed by the cluster, but its per-node status answers "No such
	// storage" -- Proxmox saying this node does not serve it, as opposed to this
	// node not answering. Classified so a caller can tell the two apart, the
	// same distinction the driver draws in pkg/csi.
	found := false

	for _, storage := range storages {
		if storage.Name != "smb" {
			assert.NotErrorIs(t, storage.StatusErr, proxmox.ErrStorageNotFound, storage.Name)

			continue
		}

		found = true

		assert.ErrorIs(t, storage.StatusErr, proxmox.ErrStorageNotFound)
	}

	require.True(t, found, "the fixture should still carry a storage no node serves")

	test.AssertNoWrites(t)
}

func TestListStoragesSurvivesAFlakyAPI(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	testcluster.SetupMockResponders()

	clean, err := newPool(t).ListStorages(t.Context(), "cluster-1")
	require.NoError(t, err)

	// See TestListVMsSurvivesAFlakyAPI for why this is asserted at all. Here the
	// consequence is capacity: readiness tracks this catalog, so a storage lost
	// to one bad answer is a storage tenants stop being able to schedule onto.
	testcluster.FailNextReads(2)

	flaky, err := newPool(t).ListStorages(t.Context(), "cluster-1")
	require.NoError(t, err)

	// StatusErr is blanked before comparing: smb's is the hypervisor's own text
	// wrapped afresh on each call, so it does not compare equal across runs
	// while everything the catalog actually publishes does. That it is still set
	// on smb, and still nil elsewhere, is asserted separately.
	assert.Equal(t, withoutStatusErrs(clean), withoutStatusErrs(flaky))

	for i := range flaky {
		if flaky[i].Name == "smb" {
			assert.Error(t, flaky[i].StatusErr, "smb should still report why it could not be measured")
		} else {
			assert.NoError(t, flaky[i].StatusErr, flaky[i].Name)
		}
	}

	test.AssertNoWrites(t)
}

// withoutStatusErrs copies the catalog with the per-storage status errors
// cleared, so two runs can be compared on what they publish.
func withoutStatusErrs(storages []proxmox.Storage) []proxmox.Storage {
	out := make([]proxmox.Storage, len(storages))

	for i, storage := range storages {
		storage.StatusErr = nil
		out[i] = storage
	}

	return out
}
