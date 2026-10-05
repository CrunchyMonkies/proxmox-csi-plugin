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

package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The assertion every adoption and authorization test depends on is only worth
// as much as its classifier, so the classifier is tested directly.
func TestInspectCalls(t *testing.T) {
	for _, tc := range []struct {
		name       string
		info       map[string]int
		wantWrites []string
		wantTotal  int
	}{
		{
			name:      "nothing called",
			info:      map[string]int{},
			wantTotal: 0,
		},
		{
			name: "registered but never called",
			// The fixture registers writers for tests that do issue them. An
			// uncalled responder is not a write.
			info: map[string]int{
				"DELETE =~/nodes/pve-1/storage/local-lvm/content/vm-9999-pvc-123": 0,
				"POST =~/nodes/(\\S+)/proxmod/csi-storage/rename":                 0,
			},
			wantTotal: 0,
		},
		{
			name: "reads only",
			info: map[string]int{
				"GET  https://127.0.0.1:8006/api2/json/cluster/resources": 1,
				"HEAD https://127.0.0.1:8006/api2/json/version":           2,
				"DELETE =~/nodes/pve-1/qemu/100/config":                   0,
			},
			wantTotal: 3,
		},
		{
			name: "a write among the reads",
			info: map[string]int{
				"GET https://127.0.0.1:8006/api2/json/cluster/resources":           4,
				"PUT https://127.0.0.1:8006/api2/json/nodes/pve-1/qemu/100/resize": 1,
				"POST =~/nodes/(\\S+)/proxmod/csi-storage/rename":                  2,
			},
			wantWrites: []string{
				"POST =~/nodes/(\\S+)/proxmod/csi-storage/rename",
				"PUT https://127.0.0.1:8006/api2/json/nodes/pve-1/qemu/100/resize",
			},
			wantTotal: 7,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writes, total := inspectCalls(tc.info)

			assert.Equal(t, tc.wantWrites, writes)
			assert.Equal(t, tc.wantTotal, total)
		})
	}
}
