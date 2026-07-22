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
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	log "github.com/sirupsen/logrus"

	goproxmox "github.com/sergelogvinov/go-proxmox"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"

	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientkubernetes "k8s.io/client-go/kubernetes"
)

// AnnotationDefaultStorageClass marks the cluster-default StorageClass.
const AnnotationDefaultStorageClass = "storageclass.kubernetes.io/is-default-class"

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
		return nil, fmt.Errorf("failed to get nodes for storage %s: %w", storageID, err)
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

// StorageCandidates are the migration target storages an operator has blessed
// for CSI use — the storages named by the driver's StorageClasses — with the
// per-zone capacities of each. Scanning all Proxmox storages instead would
// offer storages that were never meant to hold CSI volumes, so candidates come
// strictly from operator intent. The source storage is always a candidate even
// without a StorageClass, preserving the upstream same-name targeting.
type StorageCandidates struct {
	// Capacities maps each candidate storage name to the zones hosting it.
	Capacities map[string][]ZoneCapacity
	// DefaultStorage is the storage named by the cluster-default StorageClass,
	// or empty when no default exists or it belongs to another provisioner.
	DefaultStorage string
}

// GatherStorageCandidates builds the cross-storage target candidates for a
// volume on sourceStorage: the storages named by the driver's StorageClasses
// (plus the source storage itself), each with per-zone capacities. Storages a
// StorageClass names but this Proxmox cluster does not host are skipped — on
// multi-region setups a StorageClass may belong to another cluster.
func GatherStorageCandidates(ctx context.Context, cluster *goproxmox.APIClient, kclient clientkubernetes.Interface, sourceStorage string) (*StorageCandidates, error) {
	candidates := &StorageCandidates{Capacities: map[string][]ZoneCapacity{}}
	blessed := []string{sourceStorage}

	// Best-effort StorageClass read: a list failure (e.g. an image-before-RBAC
	// upgrade skew that predates the storageclasses grant) must not break the
	// same-storage-name evacuation that never needed StorageClass access. Degrade
	// to the source storage only — same-name-in-another-zone selection still
	// works via ZoneCapacities below; only the cross-storage default/blessed
	// target selection is unavailable in that mode. Mirrors the graceful
	// degradation of storageClassForStorage and checkTargetClassQuota.
	scs, err := kclient.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Warnf("failed to list storageclasses (%v); degrading to source storage %s only — same-storage-name evacuation still works, cross-storage selection is unavailable", err, sourceStorage)
	} else {
		blessed, candidates.DefaultStorage = blessedStoragesFromClasses(scs.Items, blessed)
	}

	for _, storage := range blessed {
		zones, zerr := ZoneCapacities(ctx, cluster, storage)
		if zerr != nil {
			if errors.Is(zerr, goproxmox.ErrNotFound) {
				continue
			}

			return nil, zerr
		}

		candidates.Capacities[storage] = zones
	}

	return candidates, nil
}

// blessedStoragesFromClasses appends the storages named by the driver's
// StorageClasses to blessed and returns the storage of the cluster-default
// StorageClass (the newest is-default-class by creationTimestamp, matching
// Kubernetes admission defaulting), or "" when there is no default.
func blessedStoragesFromClasses(scs []storagev1.StorageClass, blessed []string) ([]string, string) {
	var defaultSC *storagev1.StorageClass

	defaultNames := []string{}

	for i := range scs {
		sc := &scs[i]
		if sc.Provisioner != csi.DriverName {
			continue
		}

		storage := sc.Parameters[csi.StorageIDKey]
		if storage == "" {
			continue
		}

		if !slices.Contains(blessed, storage) {
			blessed = append(blessed, storage)
		}

		if sc.Annotations[AnnotationDefaultStorageClass] == "true" {
			defaultNames = append(defaultNames, sc.Name)

			if defaultSC == nil || sc.CreationTimestamp.After(defaultSC.CreationTimestamp.Time) {
				defaultSC = sc
			}
		}
	}

	if defaultSC == nil {
		return blessed, ""
	}

	if len(defaultNames) > 1 {
		log.Warnf("multiple default StorageClasses (%s); using the newest, %s, matching Kubernetes admission defaulting", strings.Join(defaultNames, ", "), defaultSC.Name)
	}

	return blessed, defaultSC.Parameters[csi.StorageIDKey]
}

// SelectCrossStorageTarget picks a (zone, storage) migration target for a
// volume on sourceStorage in sourceZone. Preference order:
//
//  1. the same storage name in another zone (upstream-compatible behavior),
//  2. the storage of the cluster-default StorageClass,
//  3. any blessed storage — the qualifying zone with the most free space.
//
// Every step applies SelectTarget's headroom rule and excludes the source zone.
func SelectCrossStorageTarget(c *StorageCandidates, sourceZone, sourceStorage string, sizeBytes int64, headroom float64) (string, string, error) {
	if zone, err := SelectTarget(c.Capacities[sourceStorage], []string{sourceZone}, sizeBytes, headroom); err == nil {
		return zone, sourceStorage, nil
	}

	if c.DefaultStorage != "" && c.DefaultStorage != sourceStorage {
		if zone, err := SelectTarget(c.Capacities[c.DefaultStorage], []string{sourceZone}, sizeBytes, headroom); err == nil {
			return zone, c.DefaultStorage, nil
		}
	}

	if headroom < 0 {
		headroom = DefaultHeadroom
	}

	required := int64(float64(sizeBytes) * (1 + headroom))
	bestZone, bestStorage := "", ""

	var bestAvail int64

	for _, storage := range slices.Sorted(maps.Keys(c.Capacities)) {
		if storage == sourceStorage || storage == c.DefaultStorage {
			continue
		}

		for _, z := range c.Capacities[storage] {
			if z.Zone == sourceZone || z.Avail < required {
				continue
			}

			if z.Avail > bestAvail {
				bestZone, bestStorage, bestAvail = z.Zone, storage, z.Avail
			}
		}
	}

	if bestZone == "" {
		return "", "", fmt.Errorf("%w: no zone with %d bytes available (headroom %.2f); considered %s",
			ErrInvalidTarget, required, headroom, c.summary())
	}

	return bestZone, bestStorage, nil
}

// StorageForZone resolves the storage to use on an explicitly chosen target
// zone: the source storage when the zone hosts it (empty, no override needed),
// else the default StorageClass's storage, else the blessed storage with the
// most available space on that zone.
func (c *StorageCandidates) StorageForZone(zone, sourceStorage string) (string, error) {
	hostsZone := func(storage string) bool {
		return slices.ContainsFunc(c.Capacities[storage], func(z ZoneCapacity) bool { return z.Zone == zone })
	}

	if hostsZone(sourceStorage) {
		return "", nil
	}

	if c.DefaultStorage != "" && hostsZone(c.DefaultStorage) {
		return c.DefaultStorage, nil
	}

	best := ""

	var bestAvail int64

	for _, storage := range slices.Sorted(maps.Keys(c.Capacities)) {
		for _, z := range c.Capacities[storage] {
			if z.Zone == zone && z.Avail > bestAvail {
				best, bestAvail = storage, z.Avail
			}
		}
	}

	if best == "" {
		return "", fmt.Errorf("%w: no candidate storage on zone %s; considered %s", ErrInvalidTarget, zone, c.summary())
	}

	return best, nil
}

// summary renders the considered storages and their zones for error messages,
// bounded so events stay readable on large clusters.
func (c *StorageCandidates) summary() string {
	const maxItems = 5

	names := slices.Sorted(maps.Keys(c.Capacities))
	if len(names) == 0 {
		return "no candidate storages (no StorageClass of this driver names a storage on this cluster)"
	}

	parts := make([]string, 0, maxItems+1)

	for i, name := range names {
		if i == maxItems {
			parts = append(parts, fmt.Sprintf("and %d more", len(names)-maxItems))

			break
		}

		zones := make([]string, 0, len(c.Capacities[name]))
		for _, z := range c.Capacities[name] {
			zones = append(zones, z.Zone)
		}

		parts = append(parts, fmt.Sprintf("storage %s (zones %s)", name, strings.Join(zones, ",")))
	}

	return strings.Join(parts, "; ")
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
