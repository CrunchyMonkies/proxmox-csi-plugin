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

// ProxmoxStorageSpec identifies one Proxmox storage in one place.
//
// The whole object is written by the operator, spec included -- there is no
// tenant-supplied intent here, only a published catalog. Tenants get read-only
// cluster-scoped access.
//
// This kind exists to keep GetCapacity off the hot path. csi-provisioner calls
// GetCapacity very frequently, and today it is served from a one-minute
// in-process cache. Turning that into a per-call round trip to the management
// apiserver would be a bad trade, so the tenant backend serves it entirely from
// a watch-backed informer over these objects and never issues a live GET.
type ProxmoxStorageSpec struct {
	// +kubebuilder:validation:MinLength=1
	Region string `json:"region"`

	// Zone is the Proxmox node. Empty for storage shared across the cluster.
	//
	// +optional
	Zone string `json:"zone,omitempty"`

	// Storage is the Proxmox storage name.
	//
	// +kubebuilder:validation:MinLength=1
	Storage string `json:"storage"`

	// Shared reports that this storage is visible from every node, in which case
	// volumes on it are not pinned to a zone.
	//
	// +optional
	Shared bool `json:"shared,omitempty"`

	// PluginType is the Proxmox storage backend, e.g. "dir", "lvm", "lvmthin",
	// "zfspool", "nfs", "rbd". The provisioner needs this to determine the disk
	// filename format (file-level storages get a .raw or .qcow2 extension).
	//
	// +optional
	PluginType string `json:"pluginType,omitempty"`

	// Types are the Proxmox content types the storage accepts, e.g. "images",
	// "rootdir".
	//
	// +optional
	// +listType=set
	Types []string `json:"types,omitempty"`
}

// ProxmoxStorageStatus carries the capacity figures.
type ProxmoxStorageStatus struct {
	// Active reports that Proxmox considers the storage online. A tenant must
	// treat an inactive storage as having no capacity rather than trusting the
	// last-known bytes.
	//
	// +optional
	Active bool `json:"active"`

	// +optional
	TotalBytes int64 `json:"totalBytes,omitempty"`

	// +optional
	AvailableBytes int64 `json:"availableBytes,omitempty"`

	// +optional
	UsedBytes int64 `json:"usedBytes,omitempty"`

	// LastSyncTime is when these figures were read from Proxmox. The tenant
	// refuses to serve capacity from an entry older than its staleness limit,
	// which is what bounds the management-outage degradation.
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

// ProxmoxStorage is the published capacity of one Proxmox storage.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=pxstore;pxstores,categories=proxmox
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Region",type=string,JSONPath=`.spec.region`
// +kubebuilder:printcolumn:name="Zone",type=string,JSONPath=`.spec.zone`
// +kubebuilder:printcolumn:name="Storage",type=string,JSONPath=`.spec.storage`
// +kubebuilder:printcolumn:name="Shared",type=boolean,JSONPath=`.spec.shared`
// +kubebuilder:printcolumn:name="Active",type=boolean,JSONPath=`.status.active`
// +kubebuilder:printcolumn:name="Available",type=integer,format=byte,JSONPath=`.status.availableBytes`
// +kubebuilder:printcolumn:name="Total",type=integer,format=byte,JSONPath=`.status.totalBytes`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ProxmoxStorage struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ProxmoxStorageSpec   `json:"spec,omitempty"`
	Status ProxmoxStorageStatus `json:"status,omitempty"`
}

// ProxmoxStorageList is a list of ProxmoxStorages.
//
// +kubebuilder:object:root=true
type ProxmoxStorageList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ProxmoxStorage `json:"items"`
}
