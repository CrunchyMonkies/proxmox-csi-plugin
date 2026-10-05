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
	"errors"
	"fmt"
	"sort"
	"strings"

	goproxmox "github.com/sergelogvinov/go-proxmox"
)

// Disk is one disk image on a Proxmox storage.
type Disk struct {
	// Region is the Proxmox cluster, matching the cloud config's region.
	Region string
	// Node is the Proxmox node whose content listing reported this disk. For a
	// shared storage that is whichever node answered first, and means nothing
	// about where the disk lives.
	Node string
	// Storage is the Proxmox storage ID.
	Storage string
	// Name is the disk as Proxmox names it: everything after "<storage>:" in the
	// volid, which for the directory plugin includes the leading "<vmid>/".
	Name string
	// VMID is the owner Proxmox reports on the content entry. Zero for content
	// that belongs to no VM, and not to be confused with the VMID embedded in
	// Name -- the two disagree exactly while a reassignment is half-done, which
	// is a case the ledger has to survive rather than trust either side of.
	VMID int32
	// SizeBytes is the size Proxmox reports.
	SizeBytes int64
}

// DiskReader reads Proxmox storage content.
//
// Read-only by type, like StorageReader and VMReader. This is the interface the
// adoption path and the drift detector are handed, and it is the reason "adopting
// a volume issues zero Proxmox writes" is a claim a reviewer can check by looking
// at one type rather than by reading both reconcilers.
type DiskReader interface {
	// ListDisks returns the disk images on one storage.
	//
	// zone names a single Proxmox node. Empty means every node that currently
	// reports the storage available, which is how both a shared storage -- whose
	// volumes are pinned to no node -- and the drift detector's full sweep are
	// asked for. Order is deterministic.
	ListDisks(ctx context.Context, region, zone, storage string) ([]Disk, error)
}

var _ DiskReader = (*Pool)(nil)

// ListDisks returns the disk images on one storage.
//
// Everything the listing reports is returned, including content this driver could
// never have created. Filtering here would be the wrong place for it: the caller
// deciding whether a disk is an orphan needs to see what is actually on the
// storage, and an adapter that quietly dropped rows would make that judgement
// against an edited picture.
func (p *Pool) ListDisks(ctx context.Context, region, zone, storage string) ([]Disk, error) {
	cl, err := p.pool.GetProxmoxCluster(region)
	if err != nil {
		return nil, fmt.Errorf("region %s: %w", region, err)
	}

	nodes := []string{zone}

	if zone == "" {
		nodes, err = cl.GetNodesForStorage(ctx, storage)
		if err != nil {
			// Not translated to an empty result, unlike ListVMs' empty-cluster
			// case. "No node currently serves this storage" is not "this storage
			// is empty": reporting it as empty would make every volume recorded
			// on a storage that is merely offline look like a disk that has
			// vanished, which is the one conclusion this ledger must never reach
			// on its own.
			//
			// Except when the storage is not configured at all, which is the one
			// case where the disks genuinely are gone. That question is answered
			// separately because this one cannot answer it: both a deleted
			// storage and a storage whose every node is down are simply absent
			// from /cluster/resources.
			if errors.Is(err, goproxmox.ErrNotFound) && !storageIsConfigured(ctx, cl, storage) {
				return nil, fmt.Errorf("region %s: locating storage %s: %w", region, storage, ErrStorageNotFound)
			}

			return nil, fmt.Errorf("region %s: locating storage %s: %w", region, storage, err)
		}

		sort.Strings(nodes)
	}

	// A shared storage answers identically on every node, so the same volid
	// arrives once per node. Keyed by volid, first node wins.
	seen := make(map[string]struct{})
	disks := []Disk{}

	for _, node := range nodes {
		content, err := cl.GetStorageContent(ctx, node, storage)
		if err != nil {
			return nil, fmt.Errorf("region %s: listing %s content on %s: %w", region, storage, node, err)
		}

		for _, item := range content {
			if item == nil {
				continue
			}

			name, ok := diskName(item.Volid, storage)
			if !ok {
				continue
			}

			if _, dup := seen[item.Volid]; dup {
				continue
			}

			seen[item.Volid] = struct{}{}

			disks = append(disks, Disk{
				Region:    region,
				Node:      node,
				Storage:   storage,
				Name:      name,
				VMID:      clampVMID(item.VMID),
				SizeBytes: clampToInt64(item.Size),
			})
		}
	}

	sort.Slice(disks, func(i, j int) bool {
		if disks[i].Name != disks[j].Name {
			return disks[i].Name < disks[j].Name
		}

		return disks[i].Node < disks[j].Node
	})

	return disks, nil
}

// storageIsConfigured reports whether the cluster's storage configuration still
// names this storage.
//
// Asked only once a listing has already failed, and asked of the configuration
// rather than of /cluster/resources, because the configuration is the one view
// that does not depend on a node being up: a storage missing from it has been
// deleted, while a storage in it that no node reports is merely offline.
//
// A check that could not be made answers "configured". The caller uses this to
// decide whether an absence is definitive, and an unanswered question is not
// evidence of anything -- least of all of the more destructive reading.
func storageIsConfigured(ctx context.Context, cl *goproxmox.APIClient, storage string) bool {
	configured, err := cl.GetStorageListByFilter(ctx)
	if err != nil {
		return true
	}

	for _, entry := range configured {
		if entry != nil && entry.Storage == storage {
			return true
		}
	}

	return false
}

// diskName splits "<storage>:<disk>" and reports whether the entry belongs to
// the storage that was asked about.
//
// The storage check is not paranoia about Proxmox: GetStorageContent is a
// node-scoped call whose path already names the storage, so a mismatch would
// mean the response does not answer the question that was asked, and adopting a
// disk from a storage nobody asked about is worse than reporting none.
func diskName(volid, storage string) (string, bool) {
	prefix, name, ok := strings.Cut(volid, ":")
	if !ok || prefix != storage || name == "" {
		return "", false
	}

	return name, true
}

// clampVMID narrows Proxmox's unsigned VMID to the signed type the API uses.
//
// Out-of-range is reported as zero rather than truncated, for the same reason
// ListVMs skips such a VM: a truncated VMID aliases onto a real one, and zero
// reads as "Proxmox named no owner", which is what every caller already handles.
func clampVMID(vmid uint64) int32 {
	const maxVMID = 999999999

	if vmid > maxVMID {
		return 0
	}

	return int32(vmid) //nolint:gosec // bounded above.
}
