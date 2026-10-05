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
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// maxNameLength is the DNS subdomain limit Kubernetes enforces on object names.
const maxNameLength = 253

// suffixLength is how much of the key digest is appended when a name has to be
// disambiguated. Eight hex characters is 32 bits, which across the tens of
// storages a Proxmox cluster has is not a collision risk worth more name.
const suffixLength = 8

// ObjectName is the ProxmoxStorage name for one storage in one place.
//
// ProxmoxStorage is cluster-scoped, so the name has to carry the whole coordinate.
// It is built to be readable -- "bne.pve-1.local-lvm", or "bne.rbd" for a shared
// storage -- because the acceptance test for this controller is a human reading
// `kubectl get pxstore` beside `pvesm status`, and a name of hashes fails that
// even though it would be easier to generate.
//
// Readability cannot come at the cost of correctness, though. Proxmox storage IDs
// accept characters Kubernetes names do not, underscores among them, so anything
// outside the allowed set is replaced and a digest of the exact coordinate is
// appended. The digest is over the raw values, so two storages that sanitize to
// the same string still get different names.
func ObjectName(region, zone, storage string) string {
	parts := []string{region}
	if zone != "" {
		parts = append(parts, zone)
	}

	parts = append(parts, storage)

	// The key is the exact coordinate with a separator that cannot appear in
	// any component, so it is unambiguous even where the name is not.
	key := strings.Join([]string{region, zone, storage}, "\x00")

	// Sanitize each dot-separated label on its own. A DNS subdomain requires
	// *every* label to start and end alphanumeric, not just the whole string, so
	// sanitizing the joined form would happily produce "bne.-pve-1-.lvm" and be
	// rejected by the API server rather than here.
	labels := make([]string, 0, len(parts))
	exact := true

	for _, part := range parts {
		lowered := strings.ToLower(part)

		label := sanitize(lowered)
		if label != lowered {
			exact = false
		}

		if label != "" {
			labels = append(labels, label)
		}
	}

	// Nothing survived, so the digest is the whole name. Hex always starts
	// alphanumeric, which is the only property the name still has to have.
	if len(labels) == 0 {
		return digest(key)
	}

	name := strings.Join(labels, ".")
	if exact && len(name) <= maxNameLength {
		return name
	}

	suffix := "-" + digest(key)

	if len(name)+len(suffix) > maxNameLength {
		// Truncation can only damage the tail, and the leading label is
		// non-empty and already starts alphanumeric, so trimming the tail is
		// enough to keep the result valid.
		name = strings.TrimRight(name[:maxNameLength-len(suffix)], "-.")
	}

	return name + suffix
}

// sanitize maps a lowercased name label onto the characters a Kubernetes name
// allows, and guarantees the result starts and ends with an alphanumeric.
func sanitize(s string) string {
	out := make([]byte, 0, len(s))

	for i := range len(s) {
		c := s[i]

		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '.':
			out = append(out, c)
		default:
			out = append(out, '-')
		}
	}

	return strings.Trim(string(out), "-.")
}

// maxLabelLength is the limit Kubernetes enforces on a label value.
const maxLabelLength = 63

// labelValue renders a Proxmox identifier as a label value.
//
// The label is a selector for narrowing a List, never a decision input: the
// publisher re-checks spec.region on every object it is about to prune, so a
// value that two identifiers happen to share -- or one an operator hand-edits --
// costs a wasted comparison and nothing more.
func labelValue(s string) string {
	out := make([]byte, 0, len(s))

	for i := range len(s) {
		c := s[i]

		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '.', c == '_':
			out = append(out, c)
		default:
			out = append(out, '-')
		}
	}

	value := strings.Trim(string(out), "-._")
	if len(value) > maxLabelLength {
		value = strings.Trim(value[:maxLabelLength], "-._")
	}

	return value
}

// digest is the short hash appended to a name that had to be sanitized.
func digest(key string) string {
	sum := sha256.Sum256([]byte(key))

	return hex.EncodeToString(sum[:])[:suffixLength]
}
