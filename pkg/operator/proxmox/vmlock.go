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
	"fmt"
	"sync"
)

// VMLock is a keyed mutex that serializes operations targeting the same
// Proxmox VM within the leader. Keyed by region/vmid so two operations on
// distinct VMs proceed in parallel, while two on the same VM serialize.
//
// This is the same pattern as pkg/csi.VMLocks, lifted to the operator level
// and keyed by region as well — the operator manages VMs across regions,
// and a VMID is unique only within one Proxmox cluster.
type VMLock struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// NewVMLock creates a new per-VM lock.
func NewVMLock() *VMLock {
	return &VMLock{
		locks: make(map[string]*sync.Mutex),
	}
}

// Lock acquires the lock for the given region and VM ID.
func (l *VMLock) Lock(region string, vmid int) {
	l.get(key(region, vmid)).Lock()
}

// Unlock releases the lock for the given region and VM ID.
func (l *VMLock) Unlock(region string, vmid int) {
	l.get(key(region, vmid)).Unlock()
}

func (l *VMLock) get(key string) *sync.Mutex {
	l.mu.Lock()
	defer l.mu.Unlock()

	m, ok := l.locks[key]
	if !ok {
		m = &sync.Mutex{}
		l.locks[key] = m
	}

	return m
}

func key(region string, vmid int) string {
	return fmt.Sprintf("%s/%d", region, vmid)
}
