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

// Package crd_test validates the generated CustomResourceDefinitions.
//
// kubeconform is the obvious tool for this and does not work: its schema set has
// no top-level CustomResourceDefinition schema, so it reports "could not find
// schema" for every file. Running the apiserver's own validation code instead is
// both offline and strictly stronger -- it enforces the structural-schema rules
// and, critically, compiles every +kubebuilder:validation:XValidation CEL
// expression. Several of the immutability rules in api/csi/v1alpha1 are CEL, and
// a typo in one would otherwise surface as a rejected `kubectl apply` on the
// management cluster.
package crd_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsvalidation "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

const crdDir = "../../../charts/proxmox-csi-plugin/files/crds"

// expectedCRDs is spelled out rather than derived from the directory listing:
// the check that matters is that every kind still has a manifest, and a test
// that only validates whatever files happen to exist passes just as happily
// when someone deletes one.
var expectedCRDs = []string{ //nolint:gochecknoglobals
	"proxmoxstorages.csi.crunchymonkies.com",
	"proxmoxvolumeattachments.csi.crunchymonkies.com",
	"proxmoxvolumes.csi.crunchymonkies.com",
	"proxmoxvolumesnapshots.csi.crunchymonkies.com",
	"tenantclusters.csi.crunchymonkies.com",
}

func TestGeneratedCRDsAreValid(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("register apiextensions/v1: %v", err)
	}

	if err := apiextensions.AddToScheme(scheme); err != nil {
		t.Fatalf("register apiextensions internal: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(crdDir, "*.yaml"))
	if err != nil {
		t.Fatalf("glob %s: %v", crdDir, err)
	}

	seen := make(map[string]bool, len(files))

	for _, file := range files {
		raw, err := os.ReadFile(file) //nolint:gosec // fixed, generated path
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}

		external := &apiextensionsv1.CustomResourceDefinition{}
		if err := utilyaml.UnmarshalStrict(raw, external); err != nil {
			t.Fatalf("decode %s: %v", file, err)
		}

		// Defaulting first, because validation assumes a defaulted object --
		// skipping it produces failures for fields controller-gen legitimately
		// leaves empty.
		scheme.Default(external)

		internal := &apiextensions.CustomResourceDefinition{}
		if err := scheme.Convert(external, internal, nil); err != nil {
			t.Fatalf("convert %s: %v", file, err)
		}

		if errs := apiextensionsvalidation.ValidateCustomResourceDefinition(
			context.Background(), internal,
		); len(errs) > 0 {
			for _, e := range errs {
				t.Errorf("%s: %s", filepath.Base(file), e.Error())
			}
		}

		seen[internal.Name] = true
	}

	for _, name := range expectedCRDs {
		if !seen[name] {
			t.Errorf("no manifest generated for %s -- run 'make manifests'", name)
		}
	}
}
