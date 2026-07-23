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

package migrator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/migrator"
)

func TestAnnotationKeysAreNamespaced(t *testing.T) {
	t.Parallel()

	// The canonical keys live under the project's own namespace; the legacy
	// variants keep the upstream driver-name prefix. Literal strings on
	// purpose: these are wire-format values that must never drift.
	assert.Equal(t, "proxmox.crunchymonkies.com/migrate-node", migrator.AnnotationMigrateNode)
	assert.Equal(t, "proxmox.crunchymonkies.com/evacuate", migrator.AnnotationEvacuate)

	assert.Equal(t, "csi.proxmox.sinextra.dev/migrate-node", migrator.LegacyAnnotation(migrator.AnnotationMigrateNode))
	assert.Equal(t, "csi.proxmox.sinextra.dev/migrate-state", migrator.LegacyAnnotation(migrator.AnnotationMigrateState))
	assert.Equal(t, "csi.proxmox.sinextra.dev/evacuate-force", migrator.LegacyAnnotation(migrator.AnnotationEvacuateForce))
}

func TestGetAnnotationDualRead(t *testing.T) {
	t.Parallel()

	tests := []struct {
		msg         string
		annotations map[string]string
		expected    string
	}{
		{
			msg:         "canonical key",
			annotations: map[string]string{"proxmox.crunchymonkies.com/migrate-node": "pve-2"},
			expected:    "pve-2",
		},
		{
			msg:         "legacy key still read",
			annotations: map[string]string{"csi.proxmox.sinextra.dev/migrate-node": "pve-2"},
			expected:    "pve-2",
		},
		{
			msg: "canonical key wins over legacy",
			annotations: map[string]string{
				"proxmox.crunchymonkies.com/migrate-node": "pve-2",
				"csi.proxmox.sinextra.dev/migrate-node":   "pve-9",
			},
			expected: "pve-2",
		},
		{
			msg:         "absent",
			annotations: map[string]string{},
			expected:    "",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.msg, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, testCase.expected, migrator.GetAnnotation(testCase.annotations, migrator.AnnotationMigrateNode))
		})
	}
}
