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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"
)

func TestVMLockSerializesSameVM(t *testing.T) {
	t.Parallel()

	lock := proxmox.NewVMLock()

	counter := 0

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			lock.Lock("region-1", 100)
			defer lock.Unlock("region-1", 100)

			// Under the lock, this is safe.
			counter++
		}()
	}

	wg.Wait()
	assert.Equal(t, 10, counter)
}

func TestVMLockDifferentVMsAreIndependent(t *testing.T) {
	t.Parallel()

	lock := proxmox.NewVMLock()

	// Two goroutines locking different VMs should not block each other.
	done := make(chan struct{}, 2)

	lock.Lock("region-1", 100)

	go func() {
		// This should not block because it's a different VM.
		lock.Lock("region-1", 200)
		lock.Unlock("region-1", 200)

		done <- struct{}{}
	}()

	// Wait for the second goroutine to complete.
	<-done

	lock.Unlock("region-1", 100)
}

func TestVMLockDifferentRegionsSameVMID(t *testing.T) {
	t.Parallel()

	lock := proxmox.NewVMLock()

	// Same VMID in different regions should not block each other.
	done := make(chan struct{}, 2)

	lock.Lock("region-1", 100)

	go func() {
		lock.Lock("region-2", 100)
		lock.Unlock("region-2", 100)

		done <- struct{}{}
	}()

	<-done

	lock.Unlock("region-1", 100)
}
