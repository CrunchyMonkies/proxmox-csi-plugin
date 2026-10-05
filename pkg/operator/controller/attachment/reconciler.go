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

// Package attachment reconciles ProxmoxVolumeAttachment records against Proxmox.
//
// Each attachment binds one volume to one VM. The reconciler runs only on the
// leader, under the cluster-wide VM lock, so two concurrent attaches to the
// same VM serialize the way they do in the direct-mode CSI controller.
package attachment

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/helpers/ptr"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"
	pvevolume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DefaultSyncPeriod is how often an attached volume is re-confirmed.
const DefaultSyncPeriod = 5 * time.Minute

// Reconciler keeps ProxmoxVolumeAttachment records in step with Proxmox.
type Reconciler struct {
	// Client is the management cluster client.
	Client client.Client
	// Writer mutates Proxmox disks.
	Writer proxmox.Writer
	// VMLock serializes operations on the same VM.
	VMLock *proxmox.VMLock
	// ReassignVolumeOnAttach mirrors the cloud config feature flag.
	ReassignVolumeOnAttach bool
	// SyncPeriod defaults to DefaultSyncPeriod.
	SyncPeriod time.Duration
	// Now defaults to time.Now. Injected so tests do not sleep.
	Now func() time.Time
}

// The permissions this reconciler needs.
//
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxvolumeattachments,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxvolumeattachments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxvolumeattachments/finalizers,verbs=update
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxvolumes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxvolumes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=tenantclusters,verbs=get;list;watch

// SetupWithManager registers the reconciler.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("proxmoxvolumeattachment").
		For(&v1alpha1.ProxmoxVolumeAttachment{}).
		Complete(r)
}

// Reconcile brings one attachment record in step with Proxmox.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	att := &v1alpha1.ProxmoxVolumeAttachment{}
	if err := r.Client.Get(ctx, req.NamespacedName, att); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !att.DeletionTimestamp.IsZero() {
		return r.reconcileDetach(ctx, att)
	}

	return r.reconcileAttach(ctx, att)
}

// reconcileAttach verifies the spec, attaches the disk, and records the result.
//
//nolint:cyclop // Flat sequence of checks then one attach, same rationale as volume reconciler.
func (r *Reconciler) reconcileAttach(ctx context.Context, att *v1alpha1.ProxmoxVolumeAttachment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Already attached — nothing to do.
	if att.Status.Attached {
		return ctrl.Result{RequeueAfter: r.syncPeriod()}, nil
	}

	// Find the volume.
	vol := &v1alpha1.ProxmoxVolume{}
	volKey := client.ObjectKey{Namespace: att.Namespace, Name: att.Spec.VolumeName}

	if err := r.Client.Get(ctx, volKey, vol); err != nil {
		r.setCondition(att, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonInvalidSpec,
			fmt.Sprintf("volume %s not found: %v", att.Spec.VolumeName, err))

		return ctrl.Result{}, r.updateStatus(ctx, att)
	}

	// Verify volumeID matches.
	if vol.Status.VolumeID != att.Spec.VolumeID {
		r.setCondition(att, v1alpha1.ConditionAdmitted, metav1.ConditionFalse, v1alpha1.ReasonVolumeIDMismatch,
			fmt.Sprintf("attachment volumeID %s does not match volume status.volumeID %s",
				att.Spec.VolumeID, vol.Status.VolumeID))

		return ctrl.Result{}, r.updateStatus(ctx, att)
	}

	// Verify the volume is ready.
	if vol.Status.Phase != v1alpha1.VolumePhaseReady {
		r.setCondition(att, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonReconciling,
			fmt.Sprintf("volume %s is not ready (phase %s)", vol.Name, vol.Status.Phase))

		return ctrl.Result{RequeueAfter: r.syncPeriod()}, r.updateStatus(ctx, att)
	}

	// Find the tenant.
	tenant, err := r.tenant(ctx, att.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}

	if tenant == nil {
		r.setCondition(att, v1alpha1.ConditionAdmitted, metav1.ConditionFalse, v1alpha1.ReasonTenantUnknown,
			fmt.Sprintf("no admitted TenantCluster registers namespace %s", att.Namespace))

		return ctrl.Result{}, r.updateStatus(ctx, att)
	}

	if tenant.Spec.Mode != v1alpha1.TenantModeEnforce && tenant.Spec.Mode != v1alpha1.TenantModeDecommission {
		r.setCondition(att, v1alpha1.ConditionAdmitted, metav1.ConditionFalse, v1alpha1.ReasonTenantObserveOnly,
			"attach requires Enforce mode")

		return ctrl.Result{}, r.updateStatus(ctx, att)
	}

	// Verify VMID is in resolvedVmids.
	if !slices.Contains(tenant.Status.ResolvedVMIDs, att.Spec.VMID) {
		r.setCondition(att, v1alpha1.ConditionAdmitted, metav1.ConditionFalse, v1alpha1.ReasonVMIDNotOwned,
			fmt.Sprintf("vmid %d is not in tenant %s's resolvedVmids", att.Spec.VMID, tenant.Name))

		return ctrl.Result{}, r.updateStatus(ctx, att)
	}

	// Add finalizer and labels.
	if err := r.hold(ctx, att, tenant); err != nil {
		return ctrl.Result{}, err
	}

	// Build the volume handle for Proxmox operations.
	pveVol, err := pvevolume.NewVolumeFromVolumeID(vol.Status.VolumeID)
	if err != nil {
		r.setCondition(att, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonInvalidSpec,
			fmt.Sprintf("invalid volumeID %q: %v", vol.Status.VolumeID, err))

		return ctrl.Result{}, r.updateStatus(ctx, att)
	}

	vmid := int(att.Spec.VMID)
	region := pveVol.Region()

	r.VMLock.Lock(region, vmid)
	defer r.VMLock.Unlock(region, vmid)

	// Reassign volume ownership if enabled.
	if r.ReassignVolumeOnAttach && pveVol.Node() != "" && pveVol.VMID() != strconv.Itoa(vmid) {
		renamed, renameErr := r.Writer.RenameDisk(ctx, region, pveVol, vmid)
		if renameErr != nil {
			logger.Error(renameErr, "failed to rename volume, attaching under current name",
				"volume", vol.Name, "vmid", vmid)
		} else {
			logger.V(4).Info("volume renamed for attach",
				"volume", vol.Name, "disk", renamed.Disk(), "vmid", vmid)

			// Update the volume's status to reflect the rename.
			vol.Status.DiskName = renamed.Disk()
			vol.Status.OwnerVMID = att.Spec.VMID

			if updateErr := r.Client.Status().Update(ctx, vol); updateErr != nil {
				logger.Error(updateErr, "failed to update volume status after rename",
					"volume", vol.Name)
			}

			pveVol = renamed
		}
	}

	options, err := diskOptions(vol, att.Spec.Readonly)
	if err != nil {
		r.setCondition(att, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonParameterRejected,
			fmt.Sprintf("volume parameters: %v", err))

		return ctrl.Result{}, r.updateStatus(ctx, att)
	}

	result, attachErr := r.Writer.AttachDisk(ctx, region, vmid, pveVol, options)
	if attachErr != nil {
		r.setCondition(att, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonProxmoxError,
			fmt.Sprintf("attaching disk: %v", attachErr))

		return ctrl.Result{RequeueAfter: r.syncPeriod()}, r.updateStatus(ctx, att)
	}

	now := metav1.NewTime(r.now())

	att.Status.Attached = true
	att.Status.DevicePath = result.DevicePath
	att.Status.LUN = strconv.Itoa(result.Lun)
	att.Status.AttachedAt = &now
	att.Status.ObservedGeneration = att.Generation

	r.setCondition(att, v1alpha1.ConditionAdmitted, metav1.ConditionTrue, v1alpha1.ReasonAccepted,
		fmt.Sprintf("attached to VM %d", vmid))
	r.setCondition(att, v1alpha1.ConditionReady, metav1.ConditionTrue, v1alpha1.ReasonAccepted,
		fmt.Sprintf("disk %s on VM %d, lun %d", pveVol.Disk(), vmid, result.Lun))

	logger.Info("volume attached",
		"volume", vol.Name, "vmid", vmid, "lun", result.Lun, "devicePath", result.DevicePath)

	return ctrl.Result{RequeueAfter: r.syncPeriod()}, r.updateStatus(ctx, att)
}

// reconcileDetach detaches the disk from the VM and removes the finalizer.
func (r *Reconciler) reconcileDetach(ctx context.Context, att *v1alpha1.ProxmoxVolumeAttachment) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(att, v1alpha1.AttachmentFinalizer) {
		return ctrl.Result{}, nil
	}

	logger := log.FromContext(ctx)

	// Find the volume to get the disk name.
	vol := &v1alpha1.ProxmoxVolume{}
	volKey := client.ObjectKey{Namespace: att.Namespace, Name: att.Spec.VolumeName}

	if err := r.Client.Get(ctx, volKey, vol); err != nil {
		// Volume is gone — the disk might already be gone too.
		logger.Info("volume not found during detach, releasing finalizer",
			"attachment", att.Name, "volume", att.Spec.VolumeName)

		controllerutil.RemoveFinalizer(att, v1alpha1.AttachmentFinalizer)

		return ctrl.Result{}, r.Client.Update(ctx, att)
	}

	if vol.Status.VolumeID == "" {
		logger.Info("volume has no volumeID, releasing finalizer",
			"attachment", att.Name, "volume", att.Spec.VolumeName)

		controllerutil.RemoveFinalizer(att, v1alpha1.AttachmentFinalizer)

		return ctrl.Result{}, r.Client.Update(ctx, att)
	}

	// Build the volume handle from the volumeID for region/zone/storage, but
	// use the current disk name from the volume status: when reassignVolumeOnAttach
	// is on, the disk name carries the attached VM's VMID while the volumeID
	// still carries the placeholder.
	baseVol, err := pvevolume.NewVolumeFromVolumeID(vol.Status.VolumeID)
	if err != nil {
		logger.Error(err, "invalid volumeID in volume status, releasing finalizer",
			"attachment", att.Name, "volumeID", vol.Status.VolumeID)

		controllerutil.RemoveFinalizer(att, v1alpha1.AttachmentFinalizer)

		return ctrl.Result{}, r.Client.Update(ctx, att)
	}

	// Use the current disk name if available (it may differ from the volumeID
	// after a rename), otherwise fall back to what the volumeID carries.
	diskName := vol.Status.DiskName
	if diskName == "" {
		diskName = baseVol.Disk()
	}

	pveVol := pvevolume.NewVolume(baseVol.Region(), baseVol.Zone(), baseVol.Storage(), diskName)

	vmid := int(att.Spec.VMID)
	region := pveVol.Region()

	r.VMLock.Lock(region, vmid)
	defer r.VMLock.Unlock(region, vmid)

	// Detach the disk.
	if detachErr := r.Writer.DetachDisk(ctx, region, vmid, pveVol); detachErr != nil {
		r.setCondition(att, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonProxmoxError,
			fmt.Sprintf("detaching disk: %v", detachErr))

		return ctrl.Result{RequeueAfter: r.syncPeriod()}, errors2(r.updateStatus(ctx, att), detachErr)
	}

	// Rename back to the placeholder VMID if reassignVolumeOnAttach is on.
	if r.ReassignVolumeOnAttach && pveVol.Node() != "" {
		tenant, tenantErr := r.tenant(ctx, att.Namespace)
		if tenantErr != nil {
			logger.Error(tenantErr, "failed to look up tenant for rename-back")
		}

		placeholderVMID := 0
		if tenant != nil {
			placeholderVMID = int(tenant.Spec.PlaceholderVMID)
		}

		curVMID, parseErr := strconv.Atoi(pveVol.VMID())
		if parseErr != nil {
			curVMID = 0
		}

		if placeholderVMID > 0 && curVMID != placeholderVMID {
			renamed, renameErr := r.Writer.RenameDisk(ctx, region, pveVol, placeholderVMID)
			if renameErr != nil {
				logger.Error(renameErr, "failed to rename volume back after detach",
					"volume", vol.Name, "vmid", vmid)
			} else {
				logger.V(4).Info("volume renamed back after detach",
					"volume", vol.Name, "disk", renamed.Disk(), "placeholderVMID", placeholderVMID)

				// Clear the unused disk entry left by the detach.
				if clearErr := r.Writer.ClearUnusedDisk(ctx, region, vmid, renamed); clearErr != nil {
					logger.Error(clearErr, "failed to clear unused disk entry",
						"volume", vol.Name, "vmid", vmid)
				}

				// Update the volume's status.
				vol.Status.DiskName = renamed.Disk()
				vol.Status.OwnerVMID = int32(placeholderVMID)

				if updateErr := r.Client.Status().Update(ctx, vol); updateErr != nil {
					logger.Error(updateErr, "failed to update volume status after rename back",
						"volume", vol.Name)
				}
			}
		}
	}

	controllerutil.RemoveFinalizer(att, v1alpha1.AttachmentFinalizer)

	if err := r.Client.Update(ctx, att); err != nil {
		return ctrl.Result{}, fmt.Errorf("releasing %s/%s: %w", att.Namespace, att.Name, err)
	}

	logger.Info("volume detached",
		"volume", vol.Name, "vmid", vmid, "attachment", att.Name)

	return ctrl.Result{}, nil
}

// hold labels the record and takes the finalizer.
func (r *Reconciler) hold(ctx context.Context, att *v1alpha1.ProxmoxVolumeAttachment, tenant *v1alpha1.TenantCluster) error {
	patched := controllerutil.AddFinalizer(att, v1alpha1.AttachmentFinalizer)

	if att.Labels == nil {
		att.Labels = map[string]string{}
	}

	if att.Labels[v1alpha1.LabelTenant] != tenant.Name {
		att.Labels[v1alpha1.LabelTenant] = tenant.Name
		patched = true
	}

	if !patched {
		return nil
	}

	if err := r.Client.Update(ctx, att); err != nil {
		return fmt.Errorf("holding %s/%s: %w", att.Namespace, att.Name, err)
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
func (r *Reconciler) setCondition(att *v1alpha1.ProxmoxVolumeAttachment, conditionType string, condStatus metav1.ConditionStatus, reason, message string) {
	apimeta.SetStatusCondition(&att.Status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             condStatus,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.NewTime(r.now()),
		ObservedGeneration: att.Generation,
	})
}

// updateStatus writes the status subresource.
func (r *Reconciler) updateStatus(ctx context.Context, att *v1alpha1.ProxmoxVolumeAttachment) error {
	att.Status.ObservedGeneration = att.Generation

	if err := r.Client.Status().Update(ctx, att); err != nil {
		return fmt.Errorf("updating %s/%s status: %w", att.Namespace, att.Name, err)
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

// diskOptions builds the drive options for an attach exactly as the CSI
// controller's ControllerPublishVolume does -- creation parameters through
// ExtractParameters and ToCFG, VolumeAttributesClass parameters merged over
// them -- but from the operator's own record of the volume, never from the
// tenant's request. These carry cache, aio, ssd, backup and the per-volume
// throttles, so attaching without them is a silent regression.
func diskOptions(vol *v1alpha1.ProxmoxVolume, readonly bool) (map[string]string, error) {
	params, err := csi.ExtractParameters(vol.Spec.Parameters)
	if err != nil {
		return nil, err
	}

	if readonly {
		params.ReadOnly = ptr.Ptr(true)
	}

	cfg := params.ToCFG()

	if len(vol.Spec.MutableParameters) > 0 {
		vac, err := csi.ExtractModifyVolumeParameters(vol.Spec.MutableParameters)
		if err != nil {
			return nil, err
		}

		cfg = vac.MergeMap(cfg)
	}

	return cfg, nil
}
