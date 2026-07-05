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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/migrator"
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
