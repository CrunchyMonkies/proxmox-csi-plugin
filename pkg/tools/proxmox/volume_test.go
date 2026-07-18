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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEscapeVolumePath(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		// Normal PVE volume names are unchanged (no-op for the safe charset).
		{"block volname", "vm-9999-disk-0", "vm-9999-disk-0"},
		{"pvc uuid", "vm-9999-pvc-1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d", "vm-9999-pvc-1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d"},
		// Directory-storage names keep their '/' separators, segments escaped individually.
		{"dir storage path preserved", "9999/vm-9999-disk-0.raw", "9999/vm-9999-disk-0.raw"},
		// Injection attempts are neutralized so they cannot alter the API URL.
		{"query injection", "vm-0?delete=1", "vm-0%3Fdelete=1"},
		{"fragment truncation", "vm-0#x", "vm-0%23x"},
		{"space", "vm 0", "vm%200"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, escapeVolumePath(tt.in))
		})
	}
}
