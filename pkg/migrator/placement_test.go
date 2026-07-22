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

package migrator_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/migrator"
	testcluster "github.com/sergelogvinov/proxmox-csi-plugin/test/cluster"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const gib = int64(1024 * 1024 * 1024)

func TestSelectTarget(t *testing.T) {
	t.Parallel()

	zones := []migrator.ZoneCapacity{
		{Zone: "pve-1", Avail: 100 * gib, Total: 200 * gib},
		{Zone: "pve-2", Avail: 50 * gib, Total: 200 * gib},
		{Zone: "pve-3", Avail: 10 * gib, Total: 200 * gib},
	}

	tests := []struct {
		name     string
		exclude  []string
		size     int64
		headroom float64
		expected string
		wantErr  bool
	}{
		{name: "most free zone wins", size: 5 * gib, headroom: 0.15, expected: "pve-1"},
		{name: "excluded zone is skipped", exclude: []string{"pve-1"}, size: 5 * gib, headroom: 0.15, expected: "pve-2"},
		{name: "zone without headroom is skipped", exclude: []string{"pve-1"}, size: 48 * gib, headroom: 0.15, expected: "", wantErr: true},
		{name: "no zone fits", size: 500 * gib, headroom: 0.15, wantErr: true},
		{name: "negative headroom falls back to default", size: 5 * gib, headroom: -1, expected: "pve-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			target, err := migrator.SelectTarget(zones, tt.exclude, tt.size, tt.headroom)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, migrator.ErrInvalidTarget)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, target)
		})
	}
}

func TestSelectCrossStorageTarget(t *testing.T) {
	t.Parallel()

	// Per-zone storage names: storA exists only in the source zone, storB
	// (the default StorageClass's storage) in zoneB, storC in zoneC with the
	// most free space, and storA2 hosts the source storage name in zoneD.
	candidates := func(withSameName bool) *migrator.StorageCandidates {
		c := &migrator.StorageCandidates{
			Capacities: map[string][]migrator.ZoneCapacity{
				"storA": {{Zone: "zoneA", Avail: 100 * gib, Total: 200 * gib}},
				"storB": {{Zone: "zoneB", Avail: 50 * gib, Total: 200 * gib}},
				"storC": {{Zone: "zoneC", Avail: 80 * gib, Total: 200 * gib}},
			},
			DefaultStorage: "storB",
		}
		if withSameName {
			c.Capacities["storA"] = append(c.Capacities["storA"], migrator.ZoneCapacity{Zone: "zoneD", Avail: 20 * gib, Total: 200 * gib})
		}

		return c
	}

	t.Run("same storage name in another zone is preferred", func(t *testing.T) {
		t.Parallel()

		zone, storage, err := migrator.SelectCrossStorageTarget(candidates(true), "zoneA", "storA", 5*gib, 0.15)
		require.NoError(t, err)
		assert.Equal(t, "zoneD", zone, "the zone hosting the source storage name wins even with less free space")
		assert.Equal(t, "storA", storage)
	})

	t.Run("default StorageClass storage when the same name is absent", func(t *testing.T) {
		t.Parallel()

		zone, storage, err := migrator.SelectCrossStorageTarget(candidates(false), "zoneA", "storA", 5*gib, 0.15)
		require.NoError(t, err)
		assert.Equal(t, "zoneB", zone, "the default class's storage wins over a roomier non-default one")
		assert.Equal(t, "storB", storage)
	})

	t.Run("headroom skips the default storage to a roomier blessed one", func(t *testing.T) {
		t.Parallel()

		// 45Gi * 1.15 = 51.75Gi: storB's 50Gi fails the headroom, storC fits.
		zone, storage, err := migrator.SelectCrossStorageTarget(candidates(false), "zoneA", "storA", 45*gib, 0.15)
		require.NoError(t, err)
		assert.Equal(t, "zoneC", zone)
		assert.Equal(t, "storC", storage)
	})

	t.Run("no candidate fits", func(t *testing.T) {
		t.Parallel()

		_, _, err := migrator.SelectCrossStorageTarget(candidates(true), "zoneA", "storA", 500*gib, 0.15)
		require.Error(t, err)
		assert.ErrorIs(t, err, migrator.ErrInvalidTarget)
		assert.Contains(t, err.Error(), "storB", "the error must name the considered storages")
	})

	t.Run("source zone is never a target", func(t *testing.T) {
		t.Parallel()

		c := &migrator.StorageCandidates{
			Capacities: map[string][]migrator.ZoneCapacity{
				"storA": {{Zone: "zoneA", Avail: 100 * gib, Total: 200 * gib}},
			},
		}

		_, _, err := migrator.SelectCrossStorageTarget(c, "zoneA", "storA", 5*gib, 0.15)
		require.Error(t, err)
		assert.ErrorIs(t, err, migrator.ErrInvalidTarget)
	})
}

func TestStorageForZone(t *testing.T) {
	t.Parallel()

	c := &migrator.StorageCandidates{
		Capacities: map[string][]migrator.ZoneCapacity{
			"storA": {{Zone: "zoneA", Avail: 100 * gib, Total: 200 * gib}},
			"storB": {{Zone: "zoneB", Avail: 50 * gib, Total: 200 * gib}},
			"storC": {{Zone: "zoneB", Avail: 80 * gib, Total: 200 * gib}, {Zone: "zoneC", Avail: 80 * gib, Total: 200 * gib}},
		},
		DefaultStorage: "storB",
	}

	// The explicit target zone hosts the source storage name: no override.
	storage, err := c.StorageForZone("zoneA", "storA")
	require.NoError(t, err)
	assert.Empty(t, storage)

	// The default class's storage wins on a zone hosting several candidates.
	storage, err = c.StorageForZone("zoneB", "storA")
	require.NoError(t, err)
	assert.Equal(t, "storB", storage)

	// Otherwise the blessed storage with the most free space on the zone.
	storage, err = c.StorageForZone("zoneC", "storA")
	require.NoError(t, err)
	assert.Equal(t, "storC", storage)

	// No candidate storage on the zone at all.
	_, err = c.StorageForZone("zoneX", "storA")
	require.Error(t, err)
	assert.ErrorIs(t, err, migrator.ErrInvalidTarget)
}

func TestPlan(t *testing.T) {
	t.Parallel()

	// pve-1 is 90% used, pve-2 is 30% used, pve-3 is 50% used.
	zones := []migrator.ZoneCapacity{
		{Zone: "pve-1", Avail: 10 * gib, Total: 100 * gib},
		{Zone: "pve-2", Avail: 70 * gib, Total: 100 * gib},
		{Zone: "pve-3", Avail: 50 * gib, Total: 100 * gib},
	}

	vols := []migrator.VolumeInfo{
		{Namespace: "default", PVCName: "a", PVName: "pv-a", Zone: "pve-1", SizeBytes: 10 * gib},
		{Namespace: "default", PVCName: "b", PVName: "pv-b", Zone: "pve-1", SizeBytes: 5 * gib},
		{Namespace: "default", PVCName: "c", PVName: "pv-c", Zone: "pve-1", SizeBytes: 20 * gib, InUse: true},
		{Namespace: "default", PVCName: "d", PVName: "pv-d", Zone: "pve-2", SizeBytes: 5 * gib},
	}

	moves := migrator.Plan(zones, vols, 0.80, 0.60, 0.15, 10)

	require.NotEmpty(t, moves)

	for _, m := range moves {
		assert.Equal(t, "pve-1", m.Source, "only the overloaded zone moves volumes")
		assert.Equal(t, "pve-2", m.Target, "the emptiest zone below the low threshold is the target")
		assert.NotEqual(t, "c", m.PVCName, "in-use volumes are never planned")
	}

	// Smallest volume moves first.
	assert.Equal(t, "b", moves[0].PVCName)
}

func TestPlanRespectsMaxMoves(t *testing.T) {
	t.Parallel()

	zones := []migrator.ZoneCapacity{
		{Zone: "pve-1", Avail: 5 * gib, Total: 100 * gib},
		{Zone: "pve-2", Avail: 90 * gib, Total: 100 * gib},
	}

	vols := []migrator.VolumeInfo{
		{PVCName: "a", Zone: "pve-1", SizeBytes: gib},
		{PVCName: "b", Zone: "pve-1", SizeBytes: gib},
		{PVCName: "c", Zone: "pve-1", SizeBytes: gib},
	}

	moves := migrator.Plan(zones, vols, 0.80, 0.60, 0.15, 1)
	assert.Len(t, moves, 1)

	moves = migrator.Plan(zones, vols, 0.80, 0.60, 0.15, 0)
	assert.Empty(t, moves)
}

func TestPlanBalancedClusterDoesNothing(t *testing.T) {
	t.Parallel()

	zones := []migrator.ZoneCapacity{
		{Zone: "pve-1", Avail: 60 * gib, Total: 100 * gib},
		{Zone: "pve-2", Avail: 70 * gib, Total: 100 * gib},
	}

	vols := []migrator.VolumeInfo{
		{PVCName: "a", Zone: "pve-1", SizeBytes: gib},
	}

	moves := migrator.Plan(zones, vols, 0.80, 0.60, 0.15, 10)
	assert.Empty(t, moves)
}

// TestGatherStorageCandidatesNewestDefaultWins is sequential (httpmock is
// process-global; the parallel placement tests above resume only after the
// sequential pass completes).
func TestGatherStorageCandidatesNewestDefaultWins(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// Two default StorageClasses: Kubernetes admission gives new PVCs the
	// NEWEST by creationTimestamp, so DefaultStorage must follow it. The newer
	// one is listed FIRST, which a last-in-list selection would get wrong.
	older := newTestStorageClass("default-old", "zfs", true)
	older.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Hour))

	newer := newTestStorageClass("default-new", "rbd", true)
	newer.CreationTimestamp = metav1.NewTime(time.Now().Add(-1 * time.Hour))

	kclient := fake.NewClientset(newer, older)

	cluster, err := newProxmoxPool(t).GetProxmoxCluster(testRegion)
	require.NoError(t, err)

	candidates, err := migrator.GatherStorageCandidates(context.Background(), cluster, kclient, "local-lvm")
	require.NoError(t, err)
	assert.Equal(t, "rbd", candidates.DefaultStorage, "the newest default StorageClass must win, matching admission defaulting")
}

// TestGatherStorageCandidatesDegradesOnStorageClassListError proves that a
// denied storageclasses list (an image-before-RBAC upgrade skew) degrades to
// the source storage only instead of failing, so same-storage-name evacuation
// keeps working. Sequential for the same httpmock reason as above.
func TestGatherStorageCandidatesDegradesOnStorageClassListError(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	kclient := fake.NewClientset()
	kclient.PrependReactor("list", "storageclasses", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("forbidden: cannot list storageclasses")
	})

	cluster, err := newProxmoxPool(t).GetProxmoxCluster(testRegion)
	require.NoError(t, err)

	candidates, err := migrator.GatherStorageCandidates(context.Background(), cluster, kclient, "local-lvm")
	require.NoError(t, err, "a denied storageclasses list must degrade gracefully, not fail")
	assert.Empty(t, candidates.DefaultStorage, "no default storage is available without the storageclasses read")
	assert.Contains(t, candidates.Capacities, "local-lvm", "the source storage stays a candidate so same-storage-name evacuation still works")
}
