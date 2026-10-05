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

// Finalizers. The operator holds these while a real Proxmox object exists behind
// a custom resource, so that deleting the resource cannot silently orphan a disk.
const (
	// VolumeFinalizer guards a ProxmoxVolume. Removing it is the operator's way
	// of saying the backing disk is gone, or was never ours to delete (adopted
	// volumes released while the tenant is not in Decommission mode).
	VolumeFinalizer = GroupName + "/volume-protection"

	// AttachmentFinalizer guards a ProxmoxVolumeAttachment until the disk is
	// actually detached from the VM.
	AttachmentFinalizer = GroupName + "/attachment-protection"

	// SnapshotFinalizer guards a ProxmoxVolumeSnapshot until the snapshot disk
	// is removed.
	SnapshotFinalizer = GroupName + "/snapshot-protection"

	// TenantFinalizer guards a TenantCluster. It refuses to release while any
	// ProxmoxVolume still exists in the tenant's namespace, so deleting a
	// registration cannot strand a ledger.
	TenantFinalizer = GroupName + "/tenant-protection"
)

// Labels the operator sets on the objects it manages. Tenants may read them but
// must not be trusted to set them: every one is recomputed on reconcile.
const (
	// LabelTenant is the TenantCluster name that owns an object.
	LabelTenant = GroupName + "/tenant"

	// LabelStorage is the Proxmox storage a volume lives on, for selecting
	// volumes when a storage is drained.
	LabelStorage = GroupName + "/storage"

	// LabelRegion is the Proxmox region.
	LabelRegion = GroupName + "/region"
)

// Condition types shared across the resource kinds. Conditions follow the
// metav1.Condition convention: type is a state, not an event, and True is always
// the good news.
const (
	// ConditionAdmitted reports whether the operator accepted the spec. False
	// means the request was refused before Proxmox was touched at all, which is
	// the state every authorization test asserts on.
	ConditionAdmitted = "Admitted"

	// ConditionReady reports whether the backing Proxmox object exists and
	// matches the spec.
	ConditionReady = "Ready"

	// ConditionDegraded reports a disagreement between the ledger and Proxmox.
	// The operator sets this and stops; it never resolves drift by mutating or
	// deleting, because the ledger being wrong is exactly the case where a
	// destructive repair would do the damage.
	ConditionDegraded = "Degraded"
)

// Reasons for the Admitted condition. These are the authorization contract: an
// operator that refuses work must say which layer refused it.
const (
	ReasonAccepted          = "Accepted"
	ReasonStorageNotAllowed = "StorageNotAllowed"
	ReasonVMIDNotOwned      = "VMIDNotOwned"
	ReasonVMIDsStale        = "VMIDsStale"
	ReasonQuotaExceeded     = "QuotaExceeded"
	ReasonNamespaceQuota    = "NamespaceQuotaExceeded"
	ReasonParameterRejected = "ParameterRejected"
	ReasonDuplicateClaim    = "DuplicateClaim"
	ReasonTenantUnknown     = "TenantUnknown"
	ReasonTenantSuspended   = "TenantSuspended"
	ReasonTenantObserveOnly = "TenantObserveOnly"
	ReasonInvalidSpec       = "InvalidSpec"
	ReasonSourceNotFound    = "SourceNotFound"
	ReasonAdoptDiskNotFound = "AdoptDiskNotFound"
	ReasonVolumeIDMismatch  = "VolumeIDMismatch"
	ReasonProxmoxError      = "ProxmoxError"
	ReasonVolumeIDChanged   = "VolumeIDChanged"
	ReasonMigrationInFlight = "MigrationInFlight"
	ReasonMissingClaimRef   = "MissingClaimRef"
	ReasonReconciling       = "Reconciling"
)

// TenantMode gates what the operator is willing to do on a tenant's behalf.
//
// +kubebuilder:validation:Enum=Observe;Enforce;Suspended;Decommission
type TenantMode string

const (
	// TenantModeObserve adopts and reports but never mutates Proxmox. This is
	// the cut-over state and the default: registering a tenant must not be able
	// to change anything on the hypervisor.
	TenantModeObserve TenantMode = "Observe"

	// TenantModeEnforce is normal operation.
	TenantModeEnforce TenantMode = "Enforce"

	// TenantModeSuspended rejects new work and leaves existing volumes alone.
	TenantModeSuspended TenantMode = "Suspended"

	// TenantModeDecommission additionally permits finalizers to delete real
	// disks on teardown. Nothing else in the system will destroy a disk.
	TenantModeDecommission TenantMode = "Decommission"
)

// VolumePhase is a coarse summary of ProxmoxVolumeStatus, for printer columns.
// Conditions remain the machine-readable truth.
//
// +kubebuilder:validation:Enum=Pending;Ready;Migrating;Rejected;Failed;Deleting
type VolumePhase string

// The volume phases. Migrating and Rejected are not CSI concepts; they exist
// because a tenant reading `kubectl get pxvol` needs to tell "the operator
// refused this" apart from "Proxmox is still working on it", and Failed says
// neither.
const (
	VolumePhasePending   VolumePhase = "Pending"
	VolumePhaseReady     VolumePhase = "Ready"
	VolumePhaseMigrating VolumePhase = "Migrating"
	VolumePhaseRejected  VolumePhase = "Rejected"
	VolumePhaseFailed    VolumePhase = "Failed"
	VolumePhaseDeleting  VolumePhase = "Deleting"
)

// TenantClaimRef identifies the PersistentVolumeClaim inside the TENANT cluster
// that a volume was provisioned for.
//
// This is provenance, populated from the csi.storage.k8s.io/pvc/{name,namespace}
// parameters that external-provisioner only sends when started with
// --extra-create-metadata.
//
// It is tenant-asserted. Namespace quotas keyed on it are fairness between a
// tenant's own namespaces, not a security boundary -- a compromised tenant
// controller can write any string here. The boundary is the tenant quota, which
// no value of this field can exceed.
type TenantClaimRef struct {
	// Name of the PVC in the tenant cluster.
	// +optional
	Name string `json:"name,omitempty"`

	// Namespace of the PVC in the tenant cluster.
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// PVName is the PersistentVolume name, which is also this resource's own
	// name. Carried for readability of `kubectl get -o wide` on the mgmt side.
	// +optional
	PVName string `json:"pvName,omitempty"`
}
