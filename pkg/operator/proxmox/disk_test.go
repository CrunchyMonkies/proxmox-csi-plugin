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

func TestListDisksUnknownRegion(t *testing.T) {
	_, err := newPool(t).ListDisks(t.Context(), "nowhere", "pve-1", "local-lvm")

	require.Error(t, err)
	assert.ErrorIs(t, err, pxpool.ErrRegionNotFound)
}

func TestListDisksOnANode(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	testcluster.SetupMockResponders()

	disks, err := newPool(t).ListDisks(t.Context(), "cluster-1", "pve-1", "local-lvm")
	require.NoError(t, err)

	names := make([]string, 0, len(disks))
	for _, disk := range disks {
		names = append(names, disk.Name)
	}

	// Sorted by name, which is what makes a drift report diffable between runs.
	assert.Equal(t, []string{
		"vm-101-pvc-reassigned",
		"vm-9999-pvc-123",
		"vm-9999-pvc-error",
		"vm-9999-pvc-exist",
		"vm-9999-pvc-exist-same-size",
		"vm-9999-pvc-unpublished",
	}, names)

	// vm-101-pvc-reassigned is the fixture's already-renamed volume: attached to
	// VM 101 and named for it, while the PV that owns it still carries 9999 in
	// its handle. Adoption has to find it under this name, so the listing must
	// report the owner Proxmox actually has rather than the one the ledger
	// expects.
	assert.Equal(t, proxmox.Disk{
		Region: "cluster-1", Node: "pve-1", Storage: "local-lvm",
		Name: "vm-101-pvc-reassigned", VMID: 101, SizeBytes: gib / 2,
	}, disks[0])

	test.AssertNoWrites(t)
}

func TestListDisksAcrossEveryNodeServingAStorage(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	testcluster.SetupMockResponders()

	// rbd is shared and reported by pve-1 and pve-2, which both answer with the
	// same volid. A shared volume exists once; listing it once per node would
	// make the drift detector count one disk as several and would make an
	// adoption ambiguous about which entry it matched.
	disks, err := newPool(t).ListDisks(t.Context(), "cluster-1", "", "rbd")
	require.NoError(t, err)

	assert.Equal(t, []proxmox.Disk{
		{
			Region: "cluster-1", Node: "pve-1", Storage: "rbd",
			Name: "9999/vm-9999-volume-rbd.raw", VMID: 9999, SizeBytes: gib,
		},
	}, disks)

	test.AssertNoWrites(t)
}

// registerStorageConfig answers the cluster storage configuration with exactly
// these storages.
//
// The shared fixture does not serve /storage -- it has per-node status and
// content endpoints only -- and that endpoint is the one view of a storage that
// does not depend on a node being up. It is therefore what separates a storage
// nothing currently serves from a storage that no longer exists.
func registerStorageConfig(names ...string) {
	configured := make([]map[string]any, 0, len(names))

	for _, name := range names {
		configured = append(configured, map[string]any{
			"storage": name, "type": "lvmthin", "content": "images",
		})
	}

	httpmock.RegisterResponder("GET", `=~/storage$`,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{"data": configured}))
}

func TestListDisksOnAStorageNoNodeServes(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	testcluster.SetupMockResponders()

	// Configured, but absent from /cluster/resources: every node serving it is
	// down. Not an empty listing and not a definitive absence either -- a storage
	// nothing currently serves is unknown, and the difference decides whether
	// every volume recorded on it is reported as a disk that has disappeared.
	registerStorageConfig("local-lvm", "rbd", "zfs", "smb", "offline")

	_, err := newPool(t).ListDisks(t.Context(), "cluster-1", "", "offline")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "locating storage offline")
	assert.NotErrorIs(t, err, proxmox.ErrStorageNotFound,
		"a storage whose nodes are all down has not been deleted")

	test.AssertNoWrites(t)
}

func TestListDisksOnAStorageThatWasDeleted(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	testcluster.SetupMockResponders()

	// The storage is in neither /cluster/resources nor the configuration, which
	// is Proxmox answering that it does not exist. Classified so the drift
	// detector reports the volumes recorded on it as missing instead of
	// suppressing them forever -- see TestADeletedStorageReportsItsVolumesMissing.
	registerStorageConfig("local-lvm", "rbd", "zfs", "smb")

	_, err := newPool(t).ListDisks(t.Context(), "cluster-1", "", "nowhere")

	require.Error(t, err)
	assert.ErrorIs(t, err, proxmox.ErrStorageNotFound)
	assert.Contains(t, err.Error(), "locating storage nowhere")

	test.AssertNoWrites(t)
}

func TestListDisksWillNotCallAStorageDeletedWhenItCannotCheck(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	testcluster.SetupMockResponders()

	// No /storage responder, so the configuration cannot be read. An unanswered
	// question is not evidence, least of all of the reading that makes the
	// detector declare volumes gone, so this must stay an ordinary error.
	_, err := newPool(t).ListDisks(t.Context(), "cluster-1", "", "nowhere")

	require.Error(t, err)
	assert.NotErrorIs(t, err, proxmox.ErrStorageNotFound)

	test.AssertNoWrites(t)
}

func TestListDisksOnAStorageThatCannotBeRead(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	testcluster.SetupMockResponders()

	// The fixture answers a content listing for an unknown storage with a 500.
	// It has to surface: adoption treats "not found" as a refusal to record the
	// volume, and a read failure that arrived as an empty listing would look
	// exactly like a disk that is not there.
	_, err := newPool(t).ListDisks(t.Context(), "cluster-1", "pve-1", "nowhere")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "listing nowhere content on pve-1")

	test.AssertNoWrites(t)
}

func TestListDisksSurvivesAFlakyAPI(t *testing.T) {
	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	testcluster.SetupMockResponders()

	clean, err := newPool(t).ListDisks(t.Context(), "cluster-1", "pve-1", "local-lvm")
	require.NoError(t, err)

	// See TestListVMsSurvivesAFlakyAPI for why this is asserted at all. It
	// matters most here: a listing that came back short rather than failing is
	// how the drift detector would be told that volumes it holds in the ledger
	// have vanished from the hypervisor.
	testcluster.FailNextReads(2)

	flaky, err := newPool(t).ListDisks(t.Context(), "cluster-1", "pve-1", "local-lvm")
	require.NoError(t, err)

	assert.Equal(t, clean, flaky)

	test.AssertNoWrites(t)
}
