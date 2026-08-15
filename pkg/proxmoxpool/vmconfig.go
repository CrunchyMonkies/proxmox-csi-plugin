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

package proxmoxpool

import (
	"context"
	"fmt"
	"net/url"

	proxmox "github.com/luthermonson/go-proxmox"

	goproxmox "github.com/sergelogvinov/go-proxmox"
)

// vmStatusUnknown is the cluster-resource status Proxmox reports for a VM on a
// node it cannot reach.
const vmStatusUnknown = "unknown"

// GetVMConfigByResource fetches a VM's configuration in a single API GET.
//
// goproxmox.GetVMConfig costs three: VirtualMachine.Ping fetches
// /status/current and then /config, and GetVMConfig fetches /config a second
// time. That is unremarkable for a one-off, and ruinous inside the scan loops
// that walk every qemu VM looking for the one holding a volume — a single
// controller call against a ten-VM node becomes thirty-odd requests, which is
// the load that made the Proxmox API start answering with empty bodies.
//
// It takes the cluster resource the caller is already iterating rather than a
// VM id, which also removes the GetVMByID lookup: the node name and the
// unreachable-VM check both come off the resource. Both are what GetVMConfig
// was doing with them.
//
// The returned VirtualMachine carries Node, VMID and VirtualMachineConfig and
// nothing else. Callers that need the status fields Ping populates must keep
// using GetVMConfig.
func GetVMConfigByResource(ctx context.Context, cl *goproxmox.APIClient, rs *proxmox.ClusterResource) (*proxmox.VirtualMachine, error) {
	if rs.Status == vmStatusUnknown {
		return nil, goproxmox.ErrVirtualMachineUnreachable
	}

	vm := &proxmox.VirtualMachine{
		Node:                 rs.Node,
		VMID:                 proxmox.StringOrUint64(rs.VMID),
		VirtualMachineConfig: &proxmox.VirtualMachineConfig{},
	}

	path := fmt.Sprintf("/nodes/%s/qemu/%d/config", url.PathEscape(rs.Node), rs.VMID)
	if err := cl.Get(ctx, path, vm.VirtualMachineConfig); err != nil {
		return nil, err
	}

	return vm, nil
}
