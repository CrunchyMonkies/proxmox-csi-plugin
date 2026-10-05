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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ProxmoxVolumeSnapshotSpec asks for a point-in-time copy of a volume.
//
// This is the kind that benefits most from the whole pattern. A snapshot is a
// full disk copy with a 3600-second task timeout, so today CreateSnapshot is a
// gRPC call that can block for an hour. Here the tenant returns as soon as
// status.snapshotID is set and reports progress through readyToUse, which is
// exactly what the CSI contract asks of a driver that does not implement
// ListSnapshots.
type ProxmoxVolumeSnapshotSpec struct {
	// SourceVolumeName is the ProxmoxVolume being snapshotted, in this
	// namespace. A name reference, never a raw handle -- same reason as
	// VolumeSource.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="sourceVolumeName is immutable"
	SourceVolumeName string `json:"sourceVolumeName"`

	// Storage is where the snapshot copy is written. Defaults to the source
	// volume's storage; must be in the tenant's allowedStorages either way.
	//
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storage is immutable"
	Storage string `json:"storage,omitempty"`

	// Zone is the target Proxmox node for cross-zone snapshot placement.
	// Defaults to the source volume's zone when empty.
	//
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="zone is immutable"
	Zone string `json:"zone,omitempty"`

	// ClaimRef records the tenant-side VolumeSnapshot this was created for.
	//
	// +optional
	ClaimRef TenantClaimRef `json:"claimRef,omitempty"`
}

// ProxmoxVolumeSnapshotStatus is written only by the operator.
type ProxmoxVolumeSnapshotStatus struct {
	// SnapshotID is the CSI snapshot handle, same
	// "<region>/<zone>/<storage>/<disk>" shape as a volume handle.
	//
	// +optional
	SnapshotID string `json:"snapshotID,omitempty"`

	// ReadyToUse reports that the copy finished. The tenant returns the handle
	// before this flips; external-snapshotter polls for the rest.
	//
	// +optional
	ReadyToUse bool `json:"readyToUse"`

	// RestoreSize is the size a volume restored from this snapshot must be.
	//
	// +optional
	RestoreSizeBytes int64 `json:"restoreSizeBytes,omitempty"`

	// CreationTime is when Proxmox finished writing the copy.
	//
	// +optional
	CreationTime *metav1.Time `json:"creationTime,omitempty"`

	// TaskID is the Proxmox UPID of the in-flight copy, for correlating a slow
	// snapshot with the hypervisor's own task log.
	//
	// +optional
	TaskID string `json:"taskID,omitempty"`

	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// ProxmoxVolumeSnapshot is a point-in-time copy of a ProxmoxVolume.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=pxsnap;pxsnaps,categories=proxmox
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.spec.sourceVolumeName`
// +kubebuilder:printcolumn:name="Ready",type=boolean,JSONPath=`.status.readyToUse`
// +kubebuilder:printcolumn:name="SnapshotID",type=string,JSONPath=`.status.snapshotID`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ProxmoxVolumeSnapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ProxmoxVolumeSnapshotSpec   `json:"spec,omitempty"`
	Status ProxmoxVolumeSnapshotStatus `json:"status,omitempty"`
}

// ProxmoxVolumeSnapshotList is a list of ProxmoxVolumeSnapshots.
//
// +kubebuilder:object:root=true
type ProxmoxVolumeSnapshotList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ProxmoxVolumeSnapshot `json:"items"`
}
