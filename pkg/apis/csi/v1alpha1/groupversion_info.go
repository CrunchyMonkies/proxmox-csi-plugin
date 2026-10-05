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

// Package v1alpha1 contains the API types for the csi.crunchymonkies.com group.
//
// Tenant Kubernetes clusters ask for Proxmox volumes by creating these objects
// in the management cluster; the operator there reconciles them against Proxmox.
// The CRD group is new, so it is named after this fork. The CSI *driver* name is
// not: csi.proxmox.sinextra.dev is baked into every existing PersistentVolume's
// spec.csi.driver, into every VolumeAttachment and into every StorageClass
// provisioner field, none of which are mutable. Renaming the driver would mean
// recreating every volume, which is the exact opposite of what this migration is
// for.
//
// +kubebuilder:object:generate=true
// +groupName=csi.crunchymonkies.com
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName is the API group these types belong to.
const GroupName = "csi.crunchymonkies.com"

// GroupVersion is the group and version of this API.
var GroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"} //nolint:gochecknoglobals

// SchemeBuilder registers these types with a runtime.Scheme.
//
// This is deliberately runtime.NewSchemeBuilder and not controller-runtime's
// scheme.Builder: this module must depend on nothing beyond apimachinery so that
// proxmox-csi-plugin can import it without pulling in the operator's world. See
// the note in go.mod.
var SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes) //nolint:gochecknoglobals

// AddToScheme adds these types to a runtime.Scheme.
var AddToScheme = SchemeBuilder.AddToScheme //nolint:gochecknoglobals

// Resource qualifies an unqualified resource name with this group, for use in
// schema.GroupResource errors.
func Resource(resource string) schema.GroupResource {
	return GroupVersion.WithResource(resource).GroupResource()
}

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion,
		&TenantCluster{}, &TenantClusterList{},
		&ProxmoxVolume{}, &ProxmoxVolumeList{},
		&ProxmoxVolumeAttachment{}, &ProxmoxVolumeAttachmentList{},
		&ProxmoxVolumeSnapshot{}, &ProxmoxVolumeSnapshotList{},
		&ProxmoxStorage{}, &ProxmoxStorageList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)

	return nil
}
