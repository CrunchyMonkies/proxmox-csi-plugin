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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestNormalizeLegacy(t *testing.T) {
	t.Parallel()

	const (
		canonicalNode  = "proxmox.crunchymonkies.com/migrate-node"
		legacyNode     = "csi.proxmox.sinextra.dev/migrate-node"
		canonicalPhase = "proxmox.crunchymonkies.com/migrate-phase"
		legacyPhase    = "csi.proxmox.sinextra.dev/migrate-phase"
		canonicalReact = "proxmox.crunchymonkies.com/reactive-evacuation"
		legacyReact    = "csi.proxmox.sinextra.dev/reactive-evacuation"
	)

	tests := []struct {
		msg      string
		current  map[string]string
		patch    map[string]*string
		expected map[string]*string
	}{
		{
			msg:      "legacy only is copied onto the canonical key and deleted",
			current:  map[string]string{legacyNode: "pve-2"},
			patch:    map[string]*string{},
			expected: map[string]*string{canonicalNode: ptr("pve-2"), legacyNode: nil},
		},
		{
			msg:      "both present with different values keeps the canonical value",
			current:  map[string]string{canonicalNode: "pve-2", legacyNode: "pve-9"},
			patch:    map[string]*string{},
			expected: map[string]*string{canonicalNode: ptr("pve-2"), legacyNode: nil},
		},
		{
			msg:      "a patch addressing the canonical key wins",
			current:  map[string]string{canonicalNode: "pve-2", legacyNode: "pve-9"},
			patch:    map[string]*string{canonicalNode: ptr("pve-7")},
			expected: map[string]*string{canonicalNode: ptr("pve-7"), legacyNode: nil},
		},
		{
			msg:      "a patch deleting the canonical key drops both",
			current:  map[string]string{canonicalNode: "pve-2", legacyNode: "pve-9"},
			patch:    map[string]*string{canonicalNode: nil},
			expected: map[string]*string{canonicalNode: nil, legacyNode: nil},
		},
		{
			msg:      "canonical only leaves the patch untouched",
			current:  map[string]string{canonicalNode: "pve-2"},
			patch:    map[string]*string{canonicalPhase: ptr("Pending")},
			expected: map[string]*string{canonicalPhase: ptr("Pending")},
		},
		{
			msg:      "an empty legacy value is still deleted without inventing a canonical key",
			current:  map[string]string{legacyNode: ""},
			patch:    map[string]*string{},
			expected: map[string]*string{legacyNode: nil},
		},
		{
			msg:     "keys outside the patch are normalized too",
			current: map[string]string{legacyNode: "pve-2", legacyReact: "true"},
			patch:   map[string]*string{canonicalPhase: ptr("Draining")},
			expected: map[string]*string{
				canonicalPhase: ptr("Draining"),
				canonicalNode:  ptr("pve-2"),
				legacyNode:     nil,
				canonicalReact: ptr("true"),
				legacyReact:    nil,
			},
		},
		{
			msg: "foreign annotations are never touched",
			current: map[string]string{
				"kubectl.kubernetes.io/last-applied-configuration": "{}",
				"csi.proxmox.sinextra.dev/instance-id":             "9101",
				"topology.proxmox.sinextra.dev/region":             "cluster-1",
				legacyPhase:                                        "Pending",
			},
			patch:    map[string]*string{},
			expected: map[string]*string{canonicalPhase: ptr("Pending"), legacyPhase: nil},
		},
		{
			msg:      "nothing stamped leaves the patch alone",
			current:  map[string]string{},
			patch:    map[string]*string{canonicalPhase: ptr("Failed")},
			expected: map[string]*string{canonicalPhase: ptr("Failed")},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.msg, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, testCase.expected, migrator.NormalizeLegacy(testCase.current, testCase.patch))
		})
	}
}

func TestAnnotationsPatch(t *testing.T) {
	t.Parallel()

	raw, err := migrator.AnnotationsPatch(map[string]*string{
		"proxmox.crunchymonkies.com/migrate-phase": ptr("Completed"),
		"csi.proxmox.sinextra.dev/migrate-phase":   nil,
	})
	require.NoError(t, err)

	// A nil value must render as a JSON null so the merge patch deletes the key
	// rather than setting it to the empty string.
	assert.JSONEq(t, `{"metadata":{"annotations":{
		"proxmox.crunchymonkies.com/migrate-phase":"Completed",
		"csi.proxmox.sinextra.dev/migrate-phase":null
	}}}`, string(raw))

	// The rendered patch must survive a round-trip as a generic object: the
	// apiserver applies it as one, and a deleted key must stay present-and-null.
	var decoded map[string]map[string]map[string]any

	require.NoError(t, json.Unmarshal(raw, &decoded))
	annotations := decoded["metadata"]["annotations"]

	require.Contains(t, annotations, "csi.proxmox.sinextra.dev/migrate-phase")
	assert.Nil(t, annotations["csi.proxmox.sinextra.dev/migrate-phase"])
}

func ptr(s string) *string { return &s }
