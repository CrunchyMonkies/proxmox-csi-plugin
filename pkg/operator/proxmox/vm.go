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
	"math"
	"sort"

	goproxmox "github.com/sergelogvinov/go-proxmox"
)

// VM is one Proxmox virtual machine, reduced to what ownership resolution needs.
//
// Deliberately not the hypervisor's own resource struct: everything about a VM
// that could tempt a reconciler into acting on it -- its power state, its disks,
// its memory -- is left out, so the only thing this type can be used for is
// deciding which tenant a VMID belongs to.
type VM struct {
	// Region is the Proxmox cluster, matching the cloud config's region.
	Region string
	// VMID is the Proxmox VM identifier.
	VMID int32
	// Node is the Proxmox node the VM currently runs on.
	Node string
	// Name is the VM's display name, for logs and for `kubectl describe`.
	Name string
	// Pool is the PVE pool the VM belongs to, empty if it is in none. This is
	// the field ownership layer 2 is built on.
	Pool string
}

// VMReader reads Proxmox VM inventory.
//
// Read-only by type, for the same reason as StorageReader: "the tenant
// reconciler never writes to Proxmox" should be checkable from the interface it
// is handed rather than by reading every line of it.
type VMReader interface {
	// ListVMs returns every non-template QEMU VM in a region. Order is
	// deterministic, by VMID.
	ListVMs(ctx context.Context, region string) ([]VM, error)
}

var _ VMReader = (*Pool)(nil)

// ListVMs returns every non-template QEMU VM in a region.
//
// One call, reading /cluster/resources?type=vm, which is also where pool
// membership comes from -- asking per VM would be a fan-out over the whole
// cluster to learn something the listing already answered.
func (p *Pool) ListVMs(ctx context.Context, region string) ([]VM, error) {
	cl, err := p.pool.GetProxmoxCluster(region)
	if err != nil {
		return nil, fmt.Errorf("region %s: %w", region, err)
	}

	resources, err := cl.GetVMsByFilter(ctx)
	if err != nil {
		// The client reports an empty result as an error. A Proxmox cluster with
		// no VMs is not a failure, and translating it here is what keeps the
		// caller's "could not read" branch meaning only that: a tenant whose
		// pool resolves to nothing must end up with no VMIDs, not with its
		// previous list preserved as though the hypervisor were unreachable.
		if errors.Is(err, goproxmox.ErrVirtualMachineNotFound) {
			return nil, nil
		}

		return nil, fmt.Errorf("region %s: listing VMs: %w", region, err)
	}

	vms := make([]VM, 0, len(resources))

	for _, r := range resources {
		if r.VMID == 0 || r.VMID > math.MaxInt32 {
			// Unreachable against a real PVE, whose VMIDs stop at 999999999.
			// Skipped rather than clamped because a truncated VMID would alias
			// onto a real one, and an allowlist that matches the wrong VM is
			// worse than one missing an entry.
			continue
		}

		vms = append(vms, VM{
			Region: region,
			VMID:   int32(r.VMID),
			Node:   r.Node,
			Name:   r.Name,
			Pool:   r.Pool,
		})
	}

	sort.Slice(vms, func(i, j int) bool { return vms[i].VMID < vms[j].VMID })

	return vms, nil
}
