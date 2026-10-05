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

// ProxmoxVolumeAttachmentSpec asks for one disk to be attached to one VM.
//
// Attachment is a separate kind from the volume on purpose. It is driven by a
// different sidecar (csi-attacher, not csi-provisioner) with an independent
// lifecycle, so keeping it separate stops attach churn conflicting with
// concurrent expand and modify writes on the volume object, mirrors upstream's
// own VolumeAttachment split, and lets admission policy gate *attach* -- the
// operation that actually crosses a tenant boundary -- on its own.
//
// Every field is immutable. An attachment is created and deleted, never edited.
type ProxmoxVolumeAttachmentSpec struct {
	// VolumeName is a ProxmoxVolume in the same namespace.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="volumeName is immutable"
	VolumeName string `json:"volumeName"`

	// VolumeID is the handle the tenant believes it is publishing.
	//
	// Carried redundantly on purpose. The operator refuses the attachment unless
	// it equals the referenced volume's status.volumeID AND that volume is in
	// this namespace. Two independent checks, so pointing a resource you own at
	// a handle you do not gets you nothing.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="volumeID is immutable"
	VolumeID string `json:"volumeID"`

	// NodeID is the raw CSI node id, recorded for audit.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="nodeID is immutable"
	NodeID string `json:"nodeID"`

	// VMID is the target Proxmox VM.
	//
	// This is the field that carries the whole attach authorization decision. The
	// tenant resolves nodeID to VMID from its own Node objects, so a compromised
	// tenant controller can name any VMID it likes. The operator re-checks it
	// against the tenant's live resolvedVmids before every attach; that check is
	// not optional.
	//
	// +kubebuilder:validation:Minimum=100
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="vmid is immutable"
	VMID int32 `json:"vmid"`

	// Readonly attaches the disk read-only.
	//
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="readonly is immutable"
	Readonly bool `json:"readonly,omitempty"`
}

// ProxmoxVolumeAttachmentStatus is written only by the operator.
//
// DevicePath, LUN and ResizeRequired are exactly the PublishContext map the node
// plugin consumes; the tenant-side backend passes them straight through.
type ProxmoxVolumeAttachmentStatus struct {
	// Attached reports that the disk is in the VM's config and the operator has
	// confirmed it.
	//
	// +optional
	Attached bool `json:"attached"`

	// DevicePath is the stable device link the node plugin looks for, e.g.
	// "/dev/disk/by-id/wwn-0x...".
	//
	// +optional
	DevicePath string `json:"devicePath,omitempty"`

	// LUN is the SCSI index the disk was attached at.
	//
	// +optional
	LUN string `json:"lun,omitempty"`

	// ResizeRequired tells the node plugin to grow the filesystem after mount.
	//
	// +optional
	ResizeRequired bool `json:"resizeRequired,omitempty"`

	// AttachedAt is when the attach completed.
	//
	// +optional
	AttachedAt *metav1.Time `json:"attachedAt,omitempty"`

	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// ProxmoxVolumeAttachment binds one ProxmoxVolume to one Proxmox VM.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=pxattach;pxattaches,categories=proxmox
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Volume",type=string,JSONPath=`.spec.volumeName`
// +kubebuilder:printcolumn:name="VMID",type=integer,JSONPath=`.spec.vmid`
// +kubebuilder:printcolumn:name="Attached",type=boolean,JSONPath=`.status.attached`
// +kubebuilder:printcolumn:name="Device",type=string,JSONPath=`.status.devicePath`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ProxmoxVolumeAttachment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ProxmoxVolumeAttachmentSpec   `json:"spec,omitempty"`
	Status ProxmoxVolumeAttachmentStatus `json:"status,omitempty"`
}

// ProxmoxVolumeAttachmentList is a list of ProxmoxVolumeAttachments.
//
// +kubebuilder:object:root=true
type ProxmoxVolumeAttachmentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ProxmoxVolumeAttachment `json:"items"`
}
