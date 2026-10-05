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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TenantClusterSpec registers a tenant Kubernetes cluster and carries its policy.
//
// This object is applied by an operator of the management cluster, never by a
// tenant and never by the Helm chart -- templating it would let a `helm upgrade`
// silently flip a tenant from Observe to Enforce.
type TenantClusterSpec struct {
	// Namespace in the MANAGEMENT cluster holding this tenant's volume objects.
	// Must be unique across TenantClusters.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="namespace is immutable"
	Namespace string `json:"namespace"`

	// Region is the Proxmox region, matching the tenant's topology labels.
	//
	// +kubebuilder:validation:MinLength=1
	Region string `json:"region"`

	// Subject is the authenticated username the management apiserver must see
	// for requests from this tenant, e.g. "pvx:bne1-cluster1".
	//
	// This must match the username literal in the apiserver's jwt[] stanza and
	// the identity in the operator chart's tenants[] entry. All three are checked
	// against each other in CI; a copy-pasted stanza whose username was never
	// changed is the one mistake issuer-uniqueness does not catch.
	//
	// +kubebuilder:validation:MinLength=1
	Subject string `json:"subject"`

	// Issuer is the OIDC issuer URL of the tenant's service account tokens, e.g.
	// "https://oidc.bne1-cluster1.ouchi.com.au". Recorded for audit and for the
	// onboarding runbook; the apiserver, not the operator, verifies it.
	//
	// +optional
	Issuer string `json:"issuer,omitempty"`

	// PlaceholderVMID is the VMID embedded in the names of this tenant's
	// unattached disks (vm-<placeholder>-pvc-<uuid>), mirroring the driver's
	// features.controllerVmID. Must be unique across TenantClusters -- every
	// cluster defaulting to 9999, as they do today, is what makes unattached
	// disks indistinguishable by owner.
	//
	// +kubebuilder:validation:Minimum=100
	// +kubebuilder:validation:Maximum=999999999
	PlaceholderVMID int32 `json:"placeholderVmid"`

	// Pool is a PVE pool whose VM membership defines this tenant's VMs. Resolved
	// on a timer into status.resolvedVmids, so a VMID that moves pools loses
	// access without anyone editing this object.
	//
	// +optional
	Pool string `json:"pool,omitempty"`

	// VMIDs is an explicit allowlist, unioned with the Pool expansion.
	//
	// +optional
	// +listType=set
	VMIDs []int32 `json:"vmids,omitempty"`

	// Mode gates what the operator will do for this tenant. Defaults to Observe:
	// registering a tenant must never be able to mutate Proxmox on its own.
	//
	// +kubebuilder:default=Observe
	Mode TenantMode `json:"mode,omitempty"`

	// LegacyControllerVMIDs are placeholder VMIDs this tenant's pre-existing
	// disks may already carry, e.g. [9999] during adoption. The ledger is
	// authoritative and the embedded VMID is advisory; renaming ~86 live disks to
	// tidy this up would be 86 chances to lose a volume, on a rename path that
	// already has a data-loss incident in its history.
	//
	// +optional
	// +listType=set
	LegacyControllerVMIDs []int32 `json:"legacyControllerVmids,omitempty"`

	// AllowedStorages is the set of Proxmox storage names this tenant may
	// provision on. Empty means none -- a tenant with no entry here can create
	// nothing, which is the correct default for a field an operator must fill in.
	//
	// +optional
	// +listType=set
	AllowedStorages []string `json:"allowedStorages,omitempty"`

	// ParameterPolicy constrains the StorageClass parameters this tenant may
	// request.
	//
	// +optional
	ParameterPolicy *ParameterPolicy `json:"parameterPolicy,omitempty"`

	// Quota caps this tenant in aggregate. This is the real boundary.
	//
	// +optional
	Quota *TenantQuota `json:"quota,omitempty"`

	// NamespaceQuotas cap individual namespaces inside the tenant cluster. See
	// TenantClaimRef: this is fairness, not security.
	//
	// +optional
	// +listType=map
	// +listMapKey=namespace
	NamespaceQuotas []NamespaceQuota `json:"namespaceQuotas,omitempty"`
}

// ParameterPolicy constrains the StorageClass parameters a tenant may request.
//
// Keys are the ones the driver understands: storage, storageFormat, cache, aio,
// discard, ssd, iothread, backup, diskIOPS, diskMBps, blockSize, inodeSize,
// rootDirPermissions, replicate.
type ParameterPolicy struct {
	// Allowed lists the parameter keys a tenant may set. Nil means all known
	// keys; empty means none.
	//
	// +optional
	// +listType=set
	Allowed []string `json:"allowed,omitempty"`

	// Forced overrides parameters regardless of what the tenant asked for, e.g.
	// backup: "false".
	//
	// +optional
	Forced map[string]string `json:"forced,omitempty"`

	// MaxInt caps numeric parameters, e.g. diskMBps: 150, diskIOPS: 3000. A
	// parameter named here that does not parse as an integer is rejected.
	//
	// +optional
	MaxInt map[string]int64 `json:"maxInt,omitempty"`

	// MaxVolumeBytes rejects a single oversized volume synchronously, which an
	// aggregate quota inherently cannot -- it is the check that catches the
	// one-40TiB-PVC mistake before any disk is allocated.
	//
	// +optional
	MaxVolumeBytes *resource.Quantity `json:"maxVolumeBytes,omitempty"`
}

// TenantQuota caps a tenant in aggregate.
type TenantQuota struct {
	// MaxVolumes caps the number of ProxmoxVolumes. Nil means unlimited.
	//
	// +optional
	MaxVolumes *int32 `json:"maxVolumes,omitempty"`

	// MaxCapacity caps total provisioned capacity per Proxmox storage name. A
	// storage absent from this map is unlimited; use AllowedStorages to deny.
	//
	// +optional
	MaxCapacity map[string]resource.Quantity `json:"maxCapacity,omitempty"`
}

// NamespaceQuota applies to the namespace of the PVC inside the TENANT cluster,
// as reported by spec.claimRef.namespace.
type NamespaceQuota struct {
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`

	// +optional
	MaxVolumes *int32 `json:"maxVolumes,omitempty"`

	// +optional
	MaxCapacity map[string]resource.Quantity `json:"maxCapacity,omitempty"`
}

// QuotaUsage is the operator's accounting. It is derived, not authoritative:
// every resync rebuilds it by listing ProxmoxVolumes, so a crash mid-reservation
// self-heals rather than leaking budget.
type QuotaUsage struct {
	// Volumes currently counted.
	//
	// +optional
	Volumes int32 `json:"volumes"`

	// Capacity per Proxmox storage name.
	//
	// +optional
	Capacity map[string]resource.Quantity `json:"capacity,omitempty"`
}

// NamespaceUsage is QuotaUsage for one tenant-side namespace.
type NamespaceUsage struct {
	Namespace string `json:"namespace"`

	// +optional
	Volumes int32 `json:"volumes"`

	// +optional
	Capacity map[string]resource.Quantity `json:"capacity,omitempty"`
}

// TenantClusterStatus is written only by the operator.
type TenantClusterStatus struct {
	// ResolvedVMIDs is the union of spec.vmids and the Pool expansion. Attach
	// authorization checks against this, not against spec.
	//
	// +optional
	// +listType=set
	ResolvedVMIDs []int32 `json:"resolvedVmids,omitempty"`

	// VMIDsObservedAt is when ResolvedVMIDs was last refreshed. The operator
	// refuses an attach on a stale resolution rather than trusting an old one,
	// which is what makes VMID recycling across tenants safe.
	//
	// +optional
	VMIDsObservedAt *metav1.Time `json:"vmidsObservedAt,omitempty"`

	// Used is aggregate accounting for the tenant.
	//
	// +optional
	Used QuotaUsage `json:"used,omitempty"`

	// NamespaceUsed is per-tenant-namespace accounting.
	//
	// +optional
	// +listType=map
	// +listMapKey=namespace
	NamespaceUsed []NamespaceUsage `json:"namespaceUsed,omitempty"`

	// ObservedGeneration is the spec generation this status reflects.
	//
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// TenantCluster registers a tenant Kubernetes cluster with the volume control
// plane and carries its ownership, quota and parameter policy.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=pxtenant;pxtenants,categories=proxmox
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Namespace",type=string,JSONPath=`.spec.namespace`
// +kubebuilder:printcolumn:name="Placeholder",type=integer,JSONPath=`.spec.placeholderVmid`
// +kubebuilder:printcolumn:name="VMIDs",type=string,JSONPath=`.status.resolvedVmids`,priority=1
// +kubebuilder:printcolumn:name="Volumes",type=integer,JSONPath=`.status.used.volumes`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type TenantCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TenantClusterSpec   `json:"spec,omitempty"`
	Status TenantClusterStatus `json:"status,omitempty"`
}

// TenantClusterList is a list of TenantClusters.
//
// +kubebuilder:object:root=true
type TenantClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []TenantCluster `json:"items"`
}
