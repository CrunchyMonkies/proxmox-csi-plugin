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

package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apivalidation "k8s.io/apimachinery/pkg/util/validation"
)

func TestObjectName(t *testing.T) {
	for _, tc := range []struct {
		name    string
		region  string
		zone    string
		storage string
		want    string
	}{
		{
			// The readable case, and the one the milestone is accepted on: this
			// is what a human compares to a row of `pvesm status`.
			name: "local storage", region: "bne", zone: "pve-1", storage: "local-lvm",
			want: "bne.pve-1.local-lvm",
		},
		{
			name: "shared storage has no zone", region: "bne", storage: "rbd",
			want: "bne.rbd",
		},
		{
			// Proxmox region names in this estate are upper case. Lowercasing is
			// not a sanitization, so it must not drag in a hash.
			name: "upper case is only lowercased", region: "BNE", zone: "PVE-1", storage: "local-lvm",
			want: "bne.pve-1.local-lvm",
		},
		{
			// Proxmox allows underscores in storage IDs; Kubernetes names do not.
			name: "underscores are replaced and disambiguated", region: "bne", zone: "pve-1", storage: "fast_nvme",
			want: "bne.pve-1.fast-nvme-" + digest("bne\x00pve-1\x00fast_nvme"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, ObjectName(tc.region, tc.zone, tc.storage))
		})
	}
}

func TestObjectNameDistinguishesCollidingIdentifiers(t *testing.T) {
	// Both sanitize to "bne.pve-1.fast-nvme". The digest is over the raw
	// coordinate, so they still land on different objects -- if they did not,
	// one storage's capacity would silently overwrite the other's.
	underscore := ObjectName("bne", "pve-1", "fast_nvme")
	slash := ObjectName("bne", "pve-1", "fast/nvme")

	assert.NotEqual(t, underscore, slash)
	assert.True(t, strings.HasPrefix(underscore, "bne.pve-1.fast-nvme-"))
	assert.True(t, strings.HasPrefix(slash, "bne.pve-1.fast-nvme-"))
}

func TestObjectNameIsAlwaysAValidName(t *testing.T) {
	// The names come from a hypervisor, not from this repo, so the guarantee has
	// to hold for input nobody anticipated: an unwritable object is a region
	// that never publishes, and it would fail at the API server rather than here.
	for _, tc := range []struct {
		name                    string
		region, zone, storageID string
	}{
		{name: "plain", region: "bne", zone: "pve-1", storageID: "local-lvm"},
		{name: "shared", region: "bne", storageID: "rbd"},
		{name: "underscores", region: "bne", zone: "pve_1", storageID: "fast_nvme"},
		{name: "leading punctuation", region: "bne", zone: "-pve-1-", storageID: "_lvm_"},
		{name: "unicode", region: "bne", zone: "pve-1", storageID: "spéciål"},
		{name: "spaces", region: "bne", zone: "pve 1", storageID: "my storage"},
		{name: "over the length limit", region: strings.Repeat("r", 200), zone: strings.Repeat("z", 200), storageID: strings.Repeat("s", 200)},
		{name: "all punctuation", region: "!", zone: "@", storageID: "#"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := ObjectName(tc.region, tc.zone, tc.storageID)

			require.NotEmpty(t, name)
			assert.LessOrEqual(t, len(name), maxNameLength)
			assert.Empty(t, apivalidation.IsDNS1123Subdomain(name), "not a valid object name: %q", name)
		})
	}
}

func TestLabelValue(t *testing.T) {
	for _, tc := range []struct {
		name, in, want string
	}{
		{name: "plain", in: "local-lvm", want: "local-lvm"},
		{name: "case is preserved", in: "BNE", want: "BNE"},
		{name: "underscores are legal in a label", in: "fast_nvme", want: "fast_nvme"},
		{name: "slashes are not", in: "fast/nvme", want: "fast-nvme"},
		{name: "trimmed to alphanumeric ends", in: "-lvm.", want: "lvm"},
		{name: "truncated", in: strings.Repeat("s", 100), want: strings.Repeat("s", maxLabelLength)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value := tc.want

			assert.Equal(t, value, labelValue(tc.in))
			assert.Empty(t, apivalidation.IsValidLabelValue(value))
		})
	}
}
