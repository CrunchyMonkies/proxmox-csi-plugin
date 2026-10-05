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

package v1alpha1_test

import (
	"strings"
	"testing"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"

	"k8s.io/apimachinery/pkg/runtime"
)

// Deliberately no testify here, and none anywhere else in this module.
//
// This is a leaf module that proxmox-csi-plugin imports. A test-only dependency
// still lands in go.mod and therefore in every consumer's module graph, which
// would quietly undo the "depends on nothing but apimachinery" property the
// whole two-repo split rests on. Plain stdlib testing is a small price.

// The scheme builder is hand-rolled rather than controller-runtime's, so it gets
// an actual test: a typo in addKnownTypes would otherwise surface as an operator
// that cannot decode its own resources at runtime.
func TestAddToScheme(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	for _, kind := range []string{
		"TenantCluster", "TenantClusterList",
		"ProxmoxVolume", "ProxmoxVolumeList",
		"ProxmoxVolumeAttachment", "ProxmoxVolumeAttachmentList",
		"ProxmoxVolumeSnapshot", "ProxmoxVolumeSnapshotList",
		"ProxmoxStorage", "ProxmoxStorageList",
	} {
		gvk := v1alpha1.GroupVersion.WithKind(kind)

		obj, err := scheme.New(gvk)
		if err != nil {
			t.Errorf("scheme.New(%s): %v", gvk, err)

			continue
		}

		gvks, _, err := scheme.ObjectKinds(obj)
		if err != nil {
			t.Errorf("ObjectKinds(%s): %v", gvk, err)

			continue
		}

		if gvks[0] != gvk {
			t.Errorf("ObjectKinds(%s) = %s, want round trip", gvk, gvks[0])
		}
	}
}

// DeepCopyObject must return an independent object. controller-gen gets this
// right, but a hand-edited zz_generated file would not, and the failure mode --
// two reconcilers sharing a map through the informer cache -- is the kind that
// only shows up under load.
func TestDeepCopyIsIndependent(t *testing.T) {
	t.Parallel()

	original := &v1alpha1.ProxmoxVolume{
		Spec: v1alpha1.ProxmoxVolumeSpec{
			Storage:    "local-lvm",
			Parameters: map[string]string{"cache": "none"},
		},
	}

	clone, ok := original.DeepCopyObject().(*v1alpha1.ProxmoxVolume)
	if !ok {
		t.Fatal("DeepCopyObject did not return a *ProxmoxVolume")
	}

	clone.Spec.Parameters["cache"] = "writeback"

	if original.Spec.Parameters["cache"] != "none" {
		t.Error("mutating the clone's parameters changed the original")
	}
}

// The four finalizers and three labels must all be namespaced under the API
// group. An unqualified finalizer is rejected by the apiserver, which would only
// be discovered the first time the operator tried to take ownership of a volume.
func TestQualifiedNames(t *testing.T) {
	t.Parallel()

	for _, name := range []string{
		v1alpha1.VolumeFinalizer,
		v1alpha1.AttachmentFinalizer,
		v1alpha1.SnapshotFinalizer,
		v1alpha1.TenantFinalizer,
		v1alpha1.LabelTenant,
		v1alpha1.LabelStorage,
		v1alpha1.LabelRegion,
	} {
		prefix, suffix, found := strings.Cut(name, "/")
		if !found || prefix != v1alpha1.GroupName || suffix == "" {
			t.Errorf("%q is not qualified as %s/<name>", name, v1alpha1.GroupName)
		}
	}
}
