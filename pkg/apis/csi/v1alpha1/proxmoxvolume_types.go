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

// ProxmoxVolumeSpec is a tenant's request for one Proxmox disk.
//
// The object is named after the PersistentVolume name that CreateVolume was
// called with (pvc-<uuid>), which is what makes provisioning idempotent under
// CSI's at-least-once retry semantics: a retry finds the work already in flight
// rather than allocating a second disk.
type ProxmoxVolumeSpec struct {
	// Region is the Proxmox region. Must match the tenant's registered region.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="region is immutable"
	Region string `json:"region"`

	// Zone is the Proxmox node. Empty for shared storage, where the disk is not
	// pinned to a node.
	//
	// Unlike the other identity fields this is mutable: the migration controller
	// patches it to move a volume between nodes.
	//
	// +optional
	Zone string `json:"zone,omitempty"`

	// Storage is the Proxmox storage name. Must appear in the tenant's
	// allowedStorages.
	//
	// Mutable for the same reason as Zone. Note that changing it changes the
	// volumeID, since the handle encodes <region>/<zone>/<storage>/<disk> -- the
	// operator republishes status.volumeID and sets Degraded=VolumeIDChanged, and
	// the tenant-side migration controller rewrites the PV. The operator cannot
	// reach the tenant's PV objects, by design.
	//
	// +kubebuilder:validation:MinLength=1
	Storage string `json:"storage"`

	// CapacityBytes is the requested size. Patched upward by
	// ControllerExpandVolume; shrinking is rejected.
	//
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:XValidation:rule="self >= oldSelf",message="capacityBytes cannot shrink"
	CapacityBytes int64 `json:"capacityBytes"`

	// Parameters are the StorageClass parameters, filtered by the tenant's
	// ParameterPolicy. Patched by ControllerModifyVolume.
	//
	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`

	// Source clones an existing volume or restores a snapshot.
	//
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="source is immutable"
	Source *VolumeSource `json:"source,omitempty"`

	// MutableParameters are the VolumeAttributesClass parameters, written by
	// ControllerModifyVolume. These are kept separate from creation Parameters
	// because they can change after provisioning and are a different set of
	// fields: see pkg/csi/parameters.go ExtractModifyVolumeParameters for the
	// exact keys (backup, discard, diskIOPS, diskMBps and their per-direction
	// variants).
	//
	// +optional
	MutableParameters map[string]string `json:"mutableParameters,omitempty"`

	// ClaimRef records which PVC in the tenant cluster this was provisioned for.
	// Required when the tenant has namespaceQuotas configured -- the operator
	// sets Admitted=False/MissingClaimRef rather than silently accounting a
	// volume to nobody, because the usual cause is a provisioner started without
	// --extra-create-metadata and that would be a quiet quota bypass.
	//
	// +optional
	ClaimRef TenantClaimRef `json:"claimRef,omitempty"`

	// Adopt takes ownership of a disk that already exists. The operator confirms
	// the disk is there, records its real name, node and size, and publishes the
	// existing handle verbatim -- it issues no create, no move, no rename and no
	// attach. Adoption is pure bookkeeping, which is what makes the migration
	// reversible with a single helm rollback.
	//
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="adopt is immutable"
	Adopt bool `json:"adopt,omitempty"`

	// AdoptVolumeID is the pre-existing handle, exactly as it appears in the
	// PersistentVolume's spec.csi.volumeHandle. Byte-identity here is the whole
	// point: the node plugin must never notice that anything changed.
	//
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="adoptVolumeID is immutable"
	AdoptVolumeID string `json:"adoptVolumeID,omitempty"`
}

// VolumeSource references content to populate a new volume from.
//
// Both fields are NAME references resolved within the requesting volume's own
// namespace, never raw Proxmox handles. That is deliberate and closes a real
// hole: today's CSI VolumeContentSource carries a caller-chosen volumeID, so a
// tenant can clone a disk belonging to another cluster.
//
// +kubebuilder:validation:MaxProperties=1
// +kubebuilder:validation:MinProperties=1
type VolumeSource struct {
	// VolumeName is a ProxmoxVolume in the same namespace.
	//
	// +optional
	VolumeName string `json:"volumeName,omitempty"`

	// SnapshotName is a ProxmoxVolumeSnapshot in the same namespace.
	//
	// +optional
	SnapshotName string `json:"snapshotName,omitempty"`
}

// ProxmoxVolumeStatus is written only by the operator.
type ProxmoxVolumeStatus struct {
	// VolumeID is the CSI volume handle, "<region>/<zone>/<storage>/<disk>",
	// returned to the tenant verbatim. Unique across all namespaces: the
	// operator maintains a cluster-wide index and refuses a second claim on the
	// same handle with Degraded=DuplicateClaim. Adopting every cluster before
	// enforcing any is how you find out whether two clusters already share a
	// disk today.
	//
	// +optional
	VolumeID string `json:"volumeID,omitempty"`

	// DiskName is the Proxmox disk name, "vm-<vmid>-pvc-<uuid>" possibly with a
	// format suffix. Re-read on every reconcile because the rename path can
	// change it underneath us.
	//
	// +optional
	DiskName string `json:"diskName,omitempty"`

	// OwnerVMID is the VMID currently embedded in the disk name: the tenant's
	// placeholder when detached, the attached VM's ID when attached, or a legacy
	// placeholder for an adopted disk.
	//
	// +optional
	OwnerVMID int32 `json:"ownerVmid,omitempty"`

	// CapacityBytes is the size Proxmox actually reports, which may exceed the
	// request after rounding.
	//
	// +optional
	CapacityBytes int64 `json:"capacityBytes,omitempty"`

	// AccessibleTopology is the list of zones where the volume is reachable,
	// matching the topology returned by CreateVolume. A local storage has one
	// zone; a shared storage may list several.
	//
	// +optional
	AccessibleTopology []string `json:"accessibleTopology,omitempty"`

	// Adopted records that this volume was taken over rather than created here.
	// The delete path consults it: an adopted disk is only destroyed when the
	// tenant is in Decommission mode.
	//
	// +optional
	Adopted bool `json:"adopted,omitempty"`

	// +optional
	Phase VolumePhase `json:"phase,omitempty"`

	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// LastSyncTime is when the operator last confirmed this against Proxmox.
	//
	// +optional
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// ProxmoxVolume is one Proxmox disk, owned by exactly one tenant cluster.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=pxvol;pxvols,categories=proxmox
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Storage",type=string,JSONPath=`.spec.storage`
// +kubebuilder:printcolumn:name="Capacity",type=integer,format=byte,JSONPath=`.status.capacityBytes`
// +kubebuilder:printcolumn:name="VolumeID",type=string,JSONPath=`.status.volumeID`,priority=1
// +kubebuilder:printcolumn:name="Adopted",type=boolean,JSONPath=`.status.adopted`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ProxmoxVolume struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ProxmoxVolumeSpec   `json:"spec,omitempty"`
	Status ProxmoxVolumeStatus `json:"status,omitempty"`
}

// ProxmoxVolumeList is a list of ProxmoxVolumes.
//
// +kubebuilder:object:root=true
type ProxmoxVolumeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ProxmoxVolume `json:"items"`
}
