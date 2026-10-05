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

// Package proxmox adapts the CSI driver's Proxmox client pool to what the
// operator's reconcilers need.
//
// The adapter exists so a reconciler depends on an interface it can fake rather
// than on a live *proxmoxpool.ProxmoxPool, and so the surface each reconciler is
// handed can be narrowed to what it is allowed to do. StorageReader is the first
// example and the pattern to follow: it has no method that mutates anything, so
// "the storage publisher never writes to Proxmox" is a property of the type
// rather than a property of the code that happens to be true today.
package proxmox

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	goproxmox "github.com/sergelogvinov/go-proxmox"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
)

// storageAvailable is the cluster-resource status Proxmox reports for a storage
// it can currently reach on a node.
const storageAvailable = "available"

// ErrStorageNotFound reports that Proxmox was reached, was asked about a storage,
// and answered that it is not there.
//
// This is evidence. A transport failure is the absence of evidence, and the two
// must not be handled alike: a caller that suppresses reporting on a storage it
// could not read has to keep reporting on a storage that is definitively gone,
// or deleting a storage silently hides every volume recorded on it.
//
// The scope is the scope of the call that produced it. From ListDisks it means
// the cluster's storage configuration does not name the storage at all; from a
// per-node status read it means that node does not serve it. Both are answers
// rather than silences, which is what the distinction turns on.
var ErrStorageNotFound = errors.New("storage not found")

// Storage is one Proxmox storage as seen from one place: a single node for local
// storage, or the cluster as a whole for shared storage.
type Storage struct {
	// Region is the Proxmox cluster, matching the cloud config's region.
	Region string
	// Zone is the Proxmox node. Empty when Shared is true, because a shared
	// storage is reachable everywhere and volumes on it are not pinned.
	Zone string
	// Name is the Proxmox storage ID, ie "local-lvm".
	Name string
	// Shared reports that every node sees this storage.
	Shared bool
	// PluginType is the Proxmox storage backend, e.g. "dir", "lvm", "lvmthin",
	// "zfspool", "nfs", "rbd". Needed by the provisioner to determine the disk
	// filename format.
	PluginType string
	// Types are the Proxmox content types the storage accepts, ie "images".
	Types []string

	// Active reports that Proxmox considers the storage usable here. False
	// either because the cluster listing does not report it available, or
	// because its status says it is disabled or inactive.
	Active bool

	TotalBytes     int64
	AvailableBytes int64
	UsedBytes      int64

	// StatusErr is set when the storage was listed but its capacity could not
	// be read. The entry is still returned, with Active false.
	//
	// Dropping it instead would be worse than useless: a tenant that stops
	// seeing a storage does not stop provisioning, it provisions somewhere
	// else. Reporting the storage as present and unusable is the answer that
	// leads to the right behavior on both sides.
	StatusErr error
}

// StorageReader reads the Proxmox storage catalog.
//
// Deliberately read-only: there is no method here that can change anything on
// the hypervisor, which is what lets the storage publisher be reviewed without
// re-deriving whether it might write.
type StorageReader interface {
	// Regions lists the configured Proxmox clusters.
	Regions() []string
	// ListStorages returns every storage in a region, one entry per place it is
	// reachable from. Order is deterministic.
	ListStorages(ctx context.Context, region string) ([]Storage, error)
}

// Pool implements StorageReader over the driver's client pool.
type Pool struct {
	pool *proxmoxpool.ProxmoxPool
}

// NewPool wraps a Proxmox client pool.
func NewPool(pool *proxmoxpool.ProxmoxPool) *Pool {
	return &Pool{pool: pool}
}

var _ StorageReader = (*Pool)(nil)

// Regions lists the configured Proxmox clusters.
func (p *Pool) Regions() []string {
	regions := p.pool.GetRegions()
	sort.Strings(regions)

	return regions
}

// ListStorages returns every storage in a region.
//
// One listing call plus one status call per resulting entry. The listing is what
// makes a shared storage collapse to a single entry: Proxmox reports it once per
// node, but a volume on it is not pinned to a node, so publishing it per node
// would invite a tenant to believe otherwise.
func (p *Pool) ListStorages(ctx context.Context, region string) ([]Storage, error) {
	cl, err := p.pool.GetProxmoxCluster(region)
	if err != nil {
		return nil, fmt.Errorf("region %s: %w", region, err)
	}

	resources, err := cl.GetClusterStoragesByFilter(ctx)
	if err != nil {
		return nil, fmt.Errorf("region %s: listing storages: %w", region, err)
	}

	// Collapse the per-node listing into one entry per published storage, and
	// remember which nodes report each one available so a shared storage's
	// capacity can be read from a node that can actually answer.
	entries := map[string]*Storage{}
	nodes := map[string][]string{}
	order := []string{}

	for _, r := range resources {
		if r.Storage == "" {
			continue
		}

		shared := r.Shared > 0

		key := r.Storage
		if !shared {
			key = r.Storage + "/" + r.Node
		}

		if _, ok := entries[key]; !ok {
			zone := r.Node
			if shared {
				zone = ""
			}

			entries[key] = &Storage{
				Region:     region,
				Zone:       zone,
				Name:       r.Storage,
				Shared:     shared,
				PluginType: r.PluginType,
				Types:      contentTypes(r.Content),
			}
			order = append(order, key)
		}

		if r.Status == storageAvailable {
			nodes[key] = append(nodes[key], r.Node)
		}
	}

	storages := make([]Storage, 0, len(order))

	for _, key := range order {
		entry := entries[key]

		available := nodes[key]
		sort.Strings(available)

		if len(available) > 0 {
			p.readStatus(ctx, cl, entry, available[0])
		}

		storages = append(storages, *entry)
	}

	sort.Slice(storages, func(i, j int) bool {
		if storages[i].Name != storages[j].Name {
			return storages[i].Name < storages[j].Name
		}

		return storages[i].Zone < storages[j].Zone
	})

	return storages, nil
}

// readStatus fills in the capacity figures for one entry, reading them from the
// given node.
//
// A failure here is recorded on the entry rather than returned, because one
// storage that cannot be measured must not cost the caller the rest of the
// catalog -- the publisher's pruning step treats an error from ListStorages as
// "I do not know what exists", and losing that distinction over a single bad
// storage would be the wrong trade.
func (p *Pool) readStatus(ctx context.Context, cl *goproxmox.APIClient, entry *Storage, node string) {
	status, err := cl.GetStorageStatus(ctx, node, entry.Name)
	if err != nil {
		// Proxmox answered, and the answer was that this node does not serve this
		// storage -- as opposed to the node not answering. The same string the
		// driver keys on in pkg/csi, kept identical on purpose: if PVE ever
		// changes the wording, both should stop classifying together rather than
		// one of them silently drifting.
		if strings.Contains(err.Error(), "No such storage") {
			err = fmt.Errorf("%w: %w", ErrStorageNotFound, err)
		}

		entry.StatusErr = fmt.Errorf("reading %s status on %s: %w", entry.Name, node, err)

		return
	}

	entry.Active = status.Enabled != 0 && status.Active != 0
	entry.TotalBytes = clampToInt64(status.Total)
	entry.AvailableBytes = clampToInt64(status.Avail)
	entry.UsedBytes = clampToInt64(status.Used)

	// The listing's content types come from the cluster resource; the status
	// call answers for this node specifically and is the better source when the
	// two disagree.
	if types := contentTypes(status.Content); len(types) > 0 {
		entry.Types = types
	}
}

// contentTypes splits a Proxmox comma-separated content list.
func contentTypes(content string) []string {
	if content == "" {
		return nil
	}

	parts := strings.Split(content, ",")
	types := make([]string, 0, len(parts))

	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			types = append(types, part)
		}
	}

	sort.Strings(types)

	return types
}

// clampToInt64 converts a Proxmox byte count to the signed type the API uses.
//
// Proxmox reports these as unsigned and Kubernetes stores them signed. A storage
// larger than 8 exbibytes does not exist, so the clamp is unreachable in
// practice; it is here so a garbled response becomes an implausible number
// rather than a negative one, which would read as "less than empty" to every
// comparison downstream.
func clampToInt64(v uint64) int64 {
	const maxInt64 = uint64(1)<<63 - 1

	if v > maxInt64 {
		return int64(maxInt64)
	}

	return int64(v)
}
