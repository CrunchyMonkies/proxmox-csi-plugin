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

// Package snapshot reconciles ProxmoxVolumeSnapshot records against Proxmox.
//
// Each snapshot is a full disk copy of a source volume. The reconciler copies the
// disk when the CR is created, sets the snapshot ID and readyToUse once the copy
// finishes, and deletes the snapshot disk when the CR is deleted.
//
// Storage restrictions match direct mode's CreateSnapshot: cifs, pbs, rbd and
// shared storage are rejected.
package snapshot

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"
	pvevolume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DefaultSyncPeriod is how often a completed snapshot is re-confirmed.
const DefaultSyncPeriod = 5 * time.Minute

// Reconciler keeps ProxmoxVolumeSnapshot records in step with Proxmox.
type Reconciler struct {
	// Client is the management cluster client.
	Client client.Client
	// Writer mutates Proxmox disks.
	Writer proxmox.Writer
	// SyncPeriod defaults to DefaultSyncPeriod.
	SyncPeriod time.Duration
	// Now defaults to time.Now. Injected so tests do not sleep.
	Now func() time.Time
}

// The permissions this reconciler needs.
//
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxvolumesnapshots,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxvolumesnapshots/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxvolumesnapshots/finalizers,verbs=update
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxvolumes,verbs=get;list;watch
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=tenantclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxstorages,verbs=get;list;watch

// SetupWithManager registers the reconciler.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("proxmoxvolumesnapshot").
		For(&v1alpha1.ProxmoxVolumeSnapshot{}).
		Complete(r)
}

// Reconcile brings one snapshot record in step with Proxmox.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	snap := &v1alpha1.ProxmoxVolumeSnapshot{}
	if err := r.Client.Get(ctx, req.NamespacedName, snap); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !snap.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, snap)
	}

	return r.reconcileCreate(ctx, snap)
}

// reconcileCreate verifies the spec, copies the source disk, and sets the
// snapshot ID.
//
//nolint:cyclop // Flat sequence of checks then one copy.
func (r *Reconciler) reconcileCreate(ctx context.Context, snap *v1alpha1.ProxmoxVolumeSnapshot) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Already done: requeue on the sync period.
	if snap.Status.ReadyToUse {
		return ctrl.Result{RequeueAfter: r.syncPeriod()}, nil
	}

	// Find the source volume.
	srcVol := &v1alpha1.ProxmoxVolume{}
	srcKey := client.ObjectKey{Namespace: snap.Namespace, Name: snap.Spec.SourceVolumeName}

	if err := r.Client.Get(ctx, srcKey, srcVol); err != nil {
		r.setCondition(snap, v1alpha1.ConditionAdmitted, metav1.ConditionFalse, v1alpha1.ReasonSourceNotFound,
			fmt.Sprintf("source volume %s not found: %v", snap.Spec.SourceVolumeName, err))

		return ctrl.Result{}, r.updateStatus(ctx, snap)
	}

	if srcVol.Status.Phase != v1alpha1.VolumePhaseReady {
		r.setCondition(snap, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonReconciling,
			fmt.Sprintf("source volume %s is not ready (phase %s)", srcVol.Name, srcVol.Status.Phase))

		return ctrl.Result{RequeueAfter: r.syncPeriod()}, r.updateStatus(ctx, snap)
	}

	// Find the tenant.
	tenant, err := r.tenant(ctx, snap.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}

	if tenant == nil {
		r.setCondition(snap, v1alpha1.ConditionAdmitted, metav1.ConditionFalse, v1alpha1.ReasonTenantUnknown,
			fmt.Sprintf("no admitted TenantCluster registers namespace %s", snap.Namespace))

		return ctrl.Result{}, r.updateStatus(ctx, snap)
	}

	if tenant.Spec.Mode != v1alpha1.TenantModeEnforce && tenant.Spec.Mode != v1alpha1.TenantModeDecommission {
		r.setCondition(snap, v1alpha1.ConditionAdmitted, metav1.ConditionFalse, v1alpha1.ReasonTenantObserveOnly,
			"snapshots require Enforce mode")

		return ctrl.Result{}, r.updateStatus(ctx, snap)
	}

	// Verify storage is allowed and supports snapshots.
	if !slices.Contains(tenant.Spec.AllowedStorages, srcVol.Spec.Storage) {
		r.setCondition(snap, v1alpha1.ConditionAdmitted, metav1.ConditionFalse, v1alpha1.ReasonStorageNotAllowed,
			fmt.Sprintf("storage %s is not in tenant %s's allowedStorages", srcVol.Spec.Storage, tenant.Name))

		return ctrl.Result{}, r.updateStatus(ctx, snap)
	}

	if err := r.checkStorageForSnapshot(ctx, srcVol.Spec.Region, srcVol.Spec.Storage); err != nil {
		r.setCondition(snap, v1alpha1.ConditionAdmitted, metav1.ConditionFalse, v1alpha1.ReasonInvalidSpec, err.Error())

		return ctrl.Result{}, r.updateStatus(ctx, snap)
	}

	// Build the source volume handle.
	srcPveVol, err := pvevolume.NewVolumeFromVolumeID(srcVol.Status.VolumeID)
	if err != nil {
		r.setCondition(snap, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonInvalidSpec,
			fmt.Sprintf("invalid source volumeID %q: %v", srcVol.Status.VolumeID, err))

		return ctrl.Result{}, r.updateStatus(ctx, snap)
	}

	// Build snapshot disk name: same as direct mode's CreateSnapshot.
	snapshotDiskName := fmt.Sprintf("vm-%d-%s", tenant.Spec.PlaceholderVMID, snap.Name)
	snapPveVol := srcPveVol.CopyVolume(snapshotDiskName)

	// Use snapshot zone if specified, otherwise inherit from source.
	if snap.Spec.Zone != "" {
		snapPveVol.SetZone(snap.Spec.Zone)
	}

	// Add finalizer and labels before copying.
	if err := r.hold(ctx, snap, tenant); err != nil {
		return ctrl.Result{}, err
	}

	// Copy the disk. CopyDisk is idempotent.
	logger.Info("copying disk for snapshot",
		"snapshot", snap.Name, "source", srcVol.Status.DiskName,
		"dest", snapPveVol.Disk(), "zone", snapPveVol.Zone())

	srcDiskName := srcVol.Status.DiskName
	if srcDiskName == "" {
		srcDiskName = srcPveVol.Disk()
	}

	if err := r.Writer.CopyDisk(ctx,
		srcVol.Spec.Region, srcPveVol.Zone(), srcVol.Spec.Storage, srcDiskName,
		snapPveVol.Zone(), srcVol.Spec.Storage, snapPveVol.Disk(),
	); err != nil {
		// Record the target disk name so the delete path can clean up partial
		// copies, then mark the snapshot as Failed. The API returns a final
		// error that lets the sidecar stop retrying this name.
		snap.Status.SnapshotID = snapPveVol.VolumeID()

		r.setCondition(snap, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonProxmoxError,
			fmt.Sprintf("copying disk: %v", err))

		return ctrl.Result{}, errors2(r.updateStatus(ctx, snap), err)
	}

	now := metav1.NewTime(r.now())

	snap.Status.SnapshotID = snapPveVol.VolumeID()
	snap.Status.ReadyToUse = true
	snap.Status.RestoreSizeBytes = srcVol.Status.CapacityBytes
	snap.Status.CreationTime = &now

	r.setCondition(snap, v1alpha1.ConditionAdmitted, metav1.ConditionTrue, v1alpha1.ReasonAccepted,
		fmt.Sprintf("snapshot of %s by tenant %s", srcVol.Name, tenant.Name))
	r.setCondition(snap, v1alpha1.ConditionReady, metav1.ConditionTrue, v1alpha1.ReasonAccepted,
		fmt.Sprintf("disk %s on %s", snapPveVol.Disk(), srcVol.Spec.Storage))

	logger.Info("snapshot created",
		"snapshot", snap.Name, "snapshotID", snap.Status.SnapshotID)

	return ctrl.Result{RequeueAfter: r.syncPeriod()}, r.updateStatus(ctx, snap)
}

// reconcileDelete removes the snapshot disk and releases the finalizer.
//
// Handles three states:
//   - snapshotID set + readyToUse: a completed snapshot, delete the disk.
//   - snapshotID set + !readyToUse: a partial copy from a failed attempt.
//     Try to delete it; if NotFound, that is fine.
//   - no snapshotID: nothing was ever written, just release.
func (r *Reconciler) reconcileDelete(ctx context.Context, snap *v1alpha1.ProxmoxVolumeSnapshot) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(snap, v1alpha1.SnapshotFinalizer) {
		return ctrl.Result{}, nil
	}

	logger := log.FromContext(ctx)

	if snap.Status.SnapshotID != "" {
		snapVol, err := pvevolume.NewVolumeFromVolumeID(snap.Status.SnapshotID)
		if err != nil {
			logger.Error(err, "invalid snapshotID, releasing finalizer",
				"snapshot", snap.Name, "snapshotID", snap.Status.SnapshotID)

			controllerutil.RemoveFinalizer(snap, v1alpha1.SnapshotFinalizer)

			return ctrl.Result{}, r.Client.Update(ctx, snap)
		}

		logger.Info("deleting snapshot disk",
			"snapshot", snap.Name, "disk", snapVol.Disk(), "zone", snapVol.Zone(),
			"partial", !snap.Status.ReadyToUse)

		if err := r.Writer.DeleteDisk(ctx,
			snapVol.Region(), snapVol.Zone(), snapVol.Storage(), snapVol.Disk(),
		); err != nil {
			// A partial copy that was never completed may not exist as a disk
			// on the storage. Treat "not found" as success; propagate anything
			// else so the reconciler retries.
			if !isNotFound(err) {
				r.setCondition(snap, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonProxmoxError,
					fmt.Sprintf("deleting snapshot disk: %v", err))

				return ctrl.Result{RequeueAfter: r.syncPeriod()}, errors2(r.updateStatus(ctx, snap), err)
			}

			logger.V(3).Info("snapshot disk not found, releasing finalizer",
				"snapshot", snap.Name, "disk", snapVol.Disk())
		}
	}

	controllerutil.RemoveFinalizer(snap, v1alpha1.SnapshotFinalizer)

	if err := r.Client.Update(ctx, snap); err != nil {
		return ctrl.Result{}, fmt.Errorf("releasing %s/%s: %w", snap.Namespace, snap.Name, err)
	}

	logger.Info("snapshot deleted", "snapshot", snap.Name)

	return ctrl.Result{}, nil
}

// isNotFound reports whether an error from Proxmox means the disk does not
// exist. Proxmox returns HTTP 500 with a message containing "does not exist"
// for storage entries that are already gone.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}

	msg := err.Error()

	return strings.Contains(msg, "does not exist") ||
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "no such")
}

// checkStorageForSnapshot validates that the storage supports snapshot copies.
// Direct mode rejects cifs, pbs, rbd, and shared storage.
func (r *Reconciler) checkStorageForSnapshot(ctx context.Context, region, storageName string) error {
	list := &v1alpha1.ProxmoxStorageList{}
	if err := r.Client.List(ctx, list); err != nil {
		return fmt.Errorf("listing storages: %w", err)
	}

	for i := range list.Items {
		ps := &list.Items[i]

		if ps.Spec.Region != region || ps.Spec.Storage != storageName {
			continue
		}

		switch ps.Spec.PluginType {
		case "cifs", "pbs":
			return fmt.Errorf("storage type %s does not support snapshots", ps.Spec.PluginType)
		case "rbd":
			return fmt.Errorf("storage type rbd (ceph) does not support snapshots")
		}

		if ps.Spec.Shared {
			return fmt.Errorf("shared storage does not support snapshots")
		}

		return nil
	}

	return fmt.Errorf("storage %s not found in the catalog for region %s", storageName, region)
}

// hold labels the record and takes the finalizer.
func (r *Reconciler) hold(ctx context.Context, snap *v1alpha1.ProxmoxVolumeSnapshot, tenant *v1alpha1.TenantCluster) error {
	patched := controllerutil.AddFinalizer(snap, v1alpha1.SnapshotFinalizer)

	if snap.Labels == nil {
		snap.Labels = map[string]string{}
	}

	if snap.Labels[v1alpha1.LabelTenant] != tenant.Name {
		snap.Labels[v1alpha1.LabelTenant] = tenant.Name
		patched = true
	}

	if !patched {
		return nil
	}

	if err := r.Client.Update(ctx, snap); err != nil {
		return fmt.Errorf("holding %s/%s: %w", snap.Namespace, snap.Name, err)
	}

	return nil
}

// tenant returns the admitted registration for a namespace, or nil.
func (r *Reconciler) tenant(ctx context.Context, namespace string) (*v1alpha1.TenantCluster, error) {
	list := &v1alpha1.TenantClusterList{}
	if err := r.Client.List(ctx, list); err != nil {
		return nil, fmt.Errorf("listing tenants: %w", err)
	}

	for i := range list.Items {
		tenant := &list.Items[i]
		if tenant.Spec.Namespace != namespace {
			continue
		}

		if apimeta.IsStatusConditionTrue(tenant.Status.Conditions, v1alpha1.ConditionAdmitted) {
			return tenant, nil
		}
	}

	return nil, nil //nolint:nilnil
}

// setCondition records one condition with this reconciler's clock.
func (r *Reconciler) setCondition(snap *v1alpha1.ProxmoxVolumeSnapshot, conditionType string, condStatus metav1.ConditionStatus, reason, message string) {
	apimeta.SetStatusCondition(&snap.Status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             condStatus,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.NewTime(r.now()),
		ObservedGeneration: snap.Generation,
	})
}

// updateStatus writes the status subresource.
func (r *Reconciler) updateStatus(ctx context.Context, snap *v1alpha1.ProxmoxVolumeSnapshot) error {
	snap.Status.ObservedGeneration = snap.Generation

	if err := r.Client.Status().Update(ctx, snap); err != nil {
		return fmt.Errorf("updating %s/%s status: %w", snap.Namespace, snap.Name, err)
	}

	return nil
}

func (r *Reconciler) syncPeriod() time.Duration {
	if r.SyncPeriod > 0 {
		return r.SyncPeriod
	}

	return DefaultSyncPeriod
}

func (r *Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}

	return time.Now()
}

// errors2 joins a status-write failure with the cause that produced it.
func errors2(first, second error) error {
	switch {
	case first == nil:
		return second
	case second == nil:
		return first
	}

	return fmt.Errorf("%w (while reporting: %w)", second, first)
}
