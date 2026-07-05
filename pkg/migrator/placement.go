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

package migrator

import (
	"context"
	"fmt"
	"slices"

	goproxmox "github.com/sergelogvinov/go-proxmox"
)

// DefaultHeadroom is the default fractional free-space margin required on a
// migration target.
const DefaultHeadroom = 0.15

// ZoneCapacity is the storage capacity of one Proxmox node (zone).
type ZoneCapacity struct {
	Zone  string
	Avail int64
	Total int64
}

// ZoneCapacities returns the capacity of the given storage on every Proxmox
// node that hosts it.
func ZoneCapacities(ctx context.Context, cluster *goproxmox.APIClient, storageID string) ([]ZoneCapacity, error) {
	zones, err := cluster.GetNodesForStorage(ctx, storageID)
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes for storage %s: %v", storageID, err)
	}

	capacities := make([]ZoneCapacity, 0, len(zones))

	for _, zone := range zones {
		storage, err := cluster.GetStorageStatus(ctx, zone, storageID)
		if err != nil {
			return nil, fmt.Errorf("failed to get storage status for %s on %s: %v", storageID, zone, err)
		}

		capacities = append(capacities, ZoneCapacity{
			Zone:  zone,
			Avail: int64(storage.Avail),
			Total: int64(storage.Total),
		})
	}

	return capacities, nil
}

// SelectTarget picks the zone with the most available space that still fits
// sizeBytes plus the fractional headroom, excluding the given zones.
func SelectTarget(zones []ZoneCapacity, exclude []string, sizeBytes int64, headroom float64) (string, error) {
	if headroom < 0 {
		headroom = DefaultHeadroom
	}

	required := int64(float64(sizeBytes) * (1 + headroom))

	best := ZoneCapacity{}

	for _, z := range zones {
		if slices.Contains(exclude, z.Zone) {
			continue
		}

		if z.Avail < required {
			continue
		}

		if z.Avail > best.Avail {
			best = z
		}
	}

	if best.Zone == "" {
		return "", fmt.Errorf("%w: no zone with %d bytes available (headroom %.2f)", ErrInvalidTarget, required, headroom)
	}

	return best.Zone, nil
}

// Move is one planned volume migration.
type Move struct {
	Namespace string
	PVCName   string
	PVName    string
	Source    string
	Target    string
	SizeBytes int64
}

// VolumeInfo describes a candidate volume for rebalancing.
type VolumeInfo struct {
	Namespace string
	PVCName   string
	PVName    string
	Zone      string
	SizeBytes int64
	InUse     bool
}

// Plan computes a greedy rebalance plan: volumes are moved from zones above
// highThreshold (used fraction) to the emptiest zone below lowThreshold,
// smallest volumes first, until the source drops below the threshold or
// maxMoves is reached. In-use volumes are skipped.
func Plan(zones []ZoneCapacity, vols []VolumeInfo, highThreshold, lowThreshold, headroom float64, maxMoves int) []Move {
	moves := []Move{}

	if maxMoves <= 0 || len(zones) < 2 {
		return moves
	}

	avail := map[string]int64{}
	total := map[string]int64{}

	for _, z := range zones {
		avail[z.Zone] = z.Avail
		total[z.Zone] = z.Total
	}

	used := func(zone string) float64 {
		if total[zone] == 0 {
			return 0
		}

		return float64(total[zone]-avail[zone]) / float64(total[zone])
	}

	// Candidate volumes by zone, smallest first.
	byZone := map[string][]VolumeInfo{}

	for _, v := range vols {
		if v.InUse {
			continue
		}

		byZone[v.Zone] = append(byZone[v.Zone], v)
	}

	for zone := range byZone {
		slices.SortFunc(byZone[zone], func(a, b VolumeInfo) int {
			return int(a.SizeBytes - b.SizeBytes)
		})
	}

	for len(moves) < maxMoves {
		// Fullest zone above the high threshold.
		source := ""

		for _, z := range zones {
			if used(z.Zone) >= highThreshold && len(byZone[z.Zone]) > 0 {
				if source == "" || used(z.Zone) > used(source) {
					source = z.Zone
				}
			}
		}

		if source == "" {
			break
		}

		vol := byZone[source][0]
		byZone[source] = byZone[source][1:]

		// Emptiest zone below the low threshold that fits the volume.
		candidates := []ZoneCapacity{}

		for _, z := range zones {
			if z.Zone == source || used(z.Zone) >= lowThreshold {
				continue
			}

			candidates = append(candidates, ZoneCapacity{Zone: z.Zone, Avail: avail[z.Zone], Total: total[z.Zone]})
		}

		target, err := SelectTarget(candidates, []string{source}, vol.SizeBytes, headroom)
		if err != nil {
			continue
		}

		avail[source] += vol.SizeBytes
		avail[target] -= vol.SizeBytes

		moves = append(moves, Move{
			Namespace: vol.Namespace,
			PVCName:   vol.PVCName,
			PVName:    vol.PVName,
			Source:    source,
			Target:    target,
			SizeBytes: vol.SizeBytes,
		})
	}

	return moves
}
