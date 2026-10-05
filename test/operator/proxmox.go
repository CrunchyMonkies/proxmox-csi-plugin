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

// Package test holds helpers shared by the operator's tests.
//
// The Proxmox side of every test in this repo runs against the driver's own
// httpmock fixture, imported from the plugin module. Sharing it is deliberate:
// "the operator issues no writes" and "the driver issues no writes" then mean
// the same sentence checked against the same mock hypervisor, rather than two
// fixtures that could drift into disagreeing about what a write is.
package test

import (
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/jarcoal/httpmock"
)

// readMethods are the HTTP methods that cannot change anything on a Proxmox
// cluster.
//
// Proxmox's API is not REST-pure -- it has GET endpoints that are expensive and
// POST endpoints that only read -- but it has no GET endpoint that mutates, so
// the method is a sound one-way test: a GET is proof of safety, and anything
// else has to be justified by a reviewer rather than by this helper.
var readMethods = map[string]bool{ //nolint:gochecknoglobals
	http.MethodGet:  true,
	http.MethodHead: true,
}

// AssertNoWrites fails the test if anything but a read was issued to Proxmox.
//
// This is the security assertion for the adoption and authorization suites: the
// claim those milestones make to an operator is that a dry run, a rejected
// request or a catalog sync cannot have moved a byte on the hypervisor, and
// this is what makes that claim checkable rather than reviewed.
//
// It also fails when no call was made at all. A helper that passes against a
// test which never reached Proxmox is worse than no helper, because it reads in
// the diff exactly like one that proved something -- and the way this assertion
// gets broken is a copy-paste into a test whose setup silently stopped working.
func AssertNoWrites(t *testing.T) {
	t.Helper()

	writes, total := inspectCalls(httpmock.GetCallCountInfo())

	if total == 0 {
		t.Error("AssertNoWrites: no Proxmox calls were made at all, so this asserts nothing")

		return
	}

	if len(writes) > 0 {
		t.Errorf("expected only reads to Proxmox, got %d write(s):\n  %s",
			len(writes), strings.Join(writes, "\n  "))
	}
}

// inspectCalls splits httpmock's call tally into the non-read calls and the
// total. Separated from the assertion so the classification is itself testable.
//
// Keys are "<METHOD> <url-or-pattern>", and httpmock reports every *registered*
// responder including the ones never called -- so the zero counts must be
// dropped first. Skipping that would make this trip on the fixture's own
// registered DELETE and POST responders in a test that issued neither.
func inspectCalls(info map[string]int) (writes []string, total int) {
	for key, count := range info {
		if count == 0 {
			continue
		}

		method, _, found := strings.Cut(key, " ")
		if !found {
			continue
		}

		total += count

		if !readMethods[method] {
			writes = append(writes, key)
		}
	}

	sort.Strings(writes)

	return writes, total
}
