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

package volume

import (
	"context"
	"crypto/sha256"
	"fmt"
	"maps"
	"slices"
	"strconv"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	toolsproxmox "github.com/sergelogvinov/proxmox-csi-plugin/pkg/tools/proxmox"
	pvevolume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// reconcileProvision handles a non-adopt volume: validate, create the disk, set
// status to Ready; or, when the volume is already Ready, handle expand and
// modify operations.
//
//nolint:cyclop // Flat sequence of refusals, same rationale as reconcileAdopt.
func (r *Reconciler) reconcileProvision(ctx context.Context, vol *v1alpha1.ProxmoxVolume) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// A Ready volume may need expansion or a mutable-parameter update.
	if vol.Status.Phase == v1alpha1.VolumePhaseReady && vol.Status.VolumeID != "" {
		return r.reconcileReadyVolume(ctx, vol)
	}

	tenant, err := r.tenant(ctx, vol.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}

	// --- Refusals, in order ---

	if tenant == nil {
		return r.reject(ctx, vol, v1alpha1.ReasonTenantUnknown,
			fmt.Sprintf("no admitted TenantCluster registers namespace %s", vol.Namespace))
	}

	if tenant.Spec.Mode == v1alpha1.TenantModeSuspended {
		return r.reject(ctx, vol, v1alpha1.ReasonTenantSuspended,
			fmt.Sprintf("tenant %s is suspended", tenant.Name))
	}

	if tenant.Spec.Mode == v1alpha1.TenantModeObserve {
		return r.reject(ctx, vol, v1alpha1.ReasonTenantObserveOnly,
			"provisioning requires Enforce mode")
	}

	if vol.Spec.Region != tenant.Spec.Region {
		return r.reject(ctx, vol, v1alpha1.ReasonInvalidSpec,
			fmt.Sprintf("region %s does not match tenant %s region %s", vol.Spec.Region, tenant.Name, tenant.Spec.Region))
	}

	if !slices.Contains(tenant.Spec.AllowedStorages, vol.Spec.Storage) {
		return r.reject(ctx, vol, v1alpha1.ReasonStorageNotAllowed,
			fmt.Sprintf("storage %s is not in tenant %s allowedStorages", vol.Spec.Storage, tenant.Name))
	}

	// Parameter policy.
	if err := checkParameterPolicy(tenant.Spec.ParameterPolicy, vol.Spec.Parameters, vol.Spec.CapacityBytes); err != nil {
		return r.reject(ctx, vol, v1alpha1.ReasonParameterRejected, err.Error())
	}

	// Quota.
	if qErr := checkQuota(tenant, vol); qErr != nil {
		return r.reject(ctx, vol, qErr.reason, qErr.message)
	}

	// Namespace quota requires a claim ref.
	if len(tenant.Spec.NamespaceQuotas) > 0 && vol.Spec.ClaimRef.Namespace == "" {
		return r.reject(ctx, vol, v1alpha1.ReasonMissingClaimRef,
			"tenant has namespaceQuotas but volume has no claimRef; start the provisioner with --extra-create-metadata")
	}

	if qErr := checkNamespaceQuota(tenant, vol); qErr != nil {
		return r.reject(ctx, vol, qErr.reason, qErr.message)
	}

	// Duplicate check.
	if other, err := r.duplicate(ctx, vol); err != nil {
		return ctrl.Result{}, err
	} else if other != nil {
		return r.rejectDuplicate(ctx, vol, other)
	}

	// --- Resolve storage type ---

	pluginType, err := r.storagePluginType(ctx, vol.Spec.Region, vol.Spec.Zone, vol.Spec.Storage)
	if err != nil {
		return r.reject(ctx, vol, v1alpha1.ReasonInvalidSpec,
			fmt.Sprintf("cannot determine storage type for %s: %v", vol.Spec.Storage, err))
	}

	// --- Create the disk ---

	pvName := vol.Name
	diskName := fmt.Sprintf("vm-%d-%s", tenant.Spec.PlaceholderVMID, pvName)

	requestedFormat := vol.Spec.Parameters["storageFormat"]
	format := toolsproxmox.DiskFormat(pluginType, requestedFormat)

	diskVol := pvevolume.NewVolume(vol.Spec.Region, vol.Spec.Zone, vol.Spec.Storage, diskName, format)
	volSizeBytes := csi.RoundUpSizeBytes(vol.Spec.CapacityBytes, csi.MinChunkSizeBytes)

	// Add finalizer before creating the disk.
	if err := r.hold(ctx, vol, tenant); err != nil {
		return ctrl.Result{}, err
	}

	// Source copy or fresh creation -- idempotent.
	if vol.Spec.Source != nil {
		if err := r.provisionFromSource(ctx, vol, diskVol); err != nil {
			return ctrl.Result{}, err
		}
	} else {
		// Check if disk already exists (idempotent).
		disks, err := r.Proxmox.ListDisks(ctx, vol.Spec.Region, vol.Spec.Zone, vol.Spec.Storage)
		if err != nil {
			r.setCondition(vol, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonProxmoxError, err.Error())

			return ctrl.Result{}, errors2(r.updateStatus(ctx, vol), err)
		}

		exists := false

		for _, d := range disks {
			if d.Name == diskVol.Disk() || d.Name == diskVol.DiskName() {
				exists = true

				break
			}
		}

		if !exists {
			logger.Info("creating disk", "volume", vol.Name, "disk", diskVol.Disk(), "size", volSizeBytes)

			// diskVol.Disk(), not diskName: on file-level storage it carries the
			// "<vmid>/" directory and the format suffix the filename needs, exactly
			// as the CSI controller passes its volume to CreateVolume.
			if err := r.Writer.CreateDisk(ctx, vol.Spec.Region, vol.Spec.Zone, vol.Spec.Storage, diskVol.Disk(), volSizeBytes); err != nil {
				r.setCondition(vol, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonProxmoxError,
					fmt.Sprintf("creating disk: %v", err))

				vol.Status.Phase = v1alpha1.VolumePhaseFailed

				return ctrl.Result{}, errors2(r.updateStatus(ctx, vol), err)
			}
		}
	}

	// Build volumeID exactly as the CSI controller does.
	volumeID := diskVol.VolumeID()

	now := metav1.NewTime(r.now())

	vol.Status.VolumeID = volumeID
	vol.Status.DiskName = diskVol.Disk()
	vol.Status.OwnerVMID = tenant.Spec.PlaceholderVMID
	vol.Status.CapacityBytes = volSizeBytes
	vol.Status.AccessibleTopology = []string{vol.Spec.Zone}
	vol.Status.Adopted = false
	vol.Status.Phase = v1alpha1.VolumePhaseReady
	vol.Status.LastSyncTime = &now

	r.setCondition(vol, v1alpha1.ConditionAdmitted, metav1.ConditionTrue, v1alpha1.ReasonAccepted,
		fmt.Sprintf("provisioned by tenant %s", tenant.Name))
	r.setCondition(vol, v1alpha1.ConditionReady, metav1.ConditionTrue, v1alpha1.ReasonAccepted,
		fmt.Sprintf("disk %s on %s, %d bytes", diskVol.Disk(), vol.Spec.Storage, volSizeBytes))

	return ctrl.Result{RequeueAfter: r.syncPeriod()}, r.updateStatus(ctx, vol)
}

// AppliedMutableParamsAnnotation tracks the hash of mutable parameters that
// have been applied to the attached VM's disk. Compared on each reconcile to
// avoid re-applying the same options. Exported so the API service can check it.
const AppliedMutableParamsAnnotation = v1alpha1.GroupName + "/applied-mutable-params"

// reconcileReadyVolume handles a provisioned volume that is already Ready.
// It checks for pending expansion and mutable-parameter changes.
func (r *Reconciler) reconcileReadyVolume(ctx context.Context, vol *v1alpha1.ProxmoxVolume) (ctrl.Result, error) {
	roundedBytes := csi.RoundUpSizeBytes(vol.Spec.CapacityBytes, csi.MinChunkSizeBytes)

	// --- Expand ---
	if roundedBytes > vol.Status.CapacityBytes {
		return r.reconcileExpand(ctx, vol, roundedBytes)
	}

	// --- Modify (mutable parameters changed on an attached volume) ---
	if len(vol.Spec.MutableParameters) > 0 {
		currentHash := MutableParamsHash(vol.Spec.MutableParameters)
		appliedHash := vol.Annotations[AppliedMutableParamsAnnotation]

		if currentHash != appliedHash {
			return r.reconcileModify(ctx, vol, currentHash)
		}
	}

	return ctrl.Result{RequeueAfter: r.syncPeriod()}, nil
}

// reconcileModify applies mutable parameters (VolumeAttributesClass) to the
// live disk on the attached VM, matching direct mode ControllerModifyVolume:
//   - Attached: UpdateDisk with ExtractModifyVolumeParameters(params).ToCFG()
//   - Detached: success (parameters applied at next attach via diskOptions)
func (r *Reconciler) reconcileModify(ctx context.Context, vol *v1alpha1.ProxmoxVolume, paramsHash string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Find an attachment for this volume.
	attachments := &v1alpha1.ProxmoxVolumeAttachmentList{}
	if err := r.Client.List(ctx, attachments, client.InNamespace(vol.Namespace)); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing attachments: %w", err)
	}

	var att *v1alpha1.ProxmoxVolumeAttachment

	for i := range attachments.Items {
		a := &attachments.Items[i]
		if a.Spec.VolumeName == vol.Name && a.Status.Attached {
			att = a

			break
		}
	}

	if att == nil {
		// Detached: nothing to update on the VM. Record the hash so the API
		// can return OK immediately. The parameters will be applied at the
		// next attach via diskOptions, same as direct mode.
		logger.V(3).Info("volume not attached, mutable parameters will apply at attach",
			"volume", vol.Name)

		return r.recordAppliedParams(ctx, vol, paramsHash)
	}

	// Attached: apply the parameters to the live disk.
	pveVol, err := pvevolume.NewVolumeFromVolumeID(vol.Status.VolumeID)
	if err != nil {
		r.setCondition(vol, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonInvalidSpec,
			fmt.Sprintf("invalid volumeID %q: %v", vol.Status.VolumeID, err))

		return ctrl.Result{}, r.updateStatus(ctx, vol)
	}

	// Build the options exactly as direct mode does: ExtractModifyVolumeParameters → ToCFG.
	params, err := csi.ExtractModifyVolumeParameters(vol.Spec.MutableParameters)
	if err != nil {
		r.setCondition(vol, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonParameterRejected,
			fmt.Sprintf("invalid mutable parameters: %v", err))

		return ctrl.Result{}, r.updateStatus(ctx, vol)
	}

	vmid := int(att.Spec.VMID)
	region := pveVol.Region()
	cfg := params.ToCFG()

	logger.Info("updating disk options",
		"volume", vol.Name, "vmid", vmid, "parameters", cfg)

	if err := r.Writer.UpdateDisk(ctx, region, vmid, pveVol, cfg); err != nil {
		r.setCondition(vol, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonProxmoxError,
			fmt.Sprintf("updating disk options: %v", err))

		return ctrl.Result{RequeueAfter: r.syncPeriod()}, errors2(r.updateStatus(ctx, vol), err)
	}

	logger.Info("disk options updated",
		"volume", vol.Name, "vmid", vmid)

	return r.recordAppliedParams(ctx, vol, paramsHash)
}

// recordAppliedParams stamps the hash of the applied mutable parameters so
// subsequent reconciles do not re-apply them.
func (r *Reconciler) recordAppliedParams(ctx context.Context, vol *v1alpha1.ProxmoxVolume, hash string) (ctrl.Result, error) {
	if vol.Annotations == nil {
		vol.Annotations = map[string]string{}
	}

	vol.Annotations[AppliedMutableParamsAnnotation] = hash

	if err := r.Client.Update(ctx, vol); err != nil {
		return ctrl.Result{}, fmt.Errorf("recording applied params on %s/%s: %w", vol.Namespace, vol.Name, err)
	}

	return ctrl.Result{RequeueAfter: r.syncPeriod()}, nil
}

// MutableParamsHash returns a deterministic hash of the mutable parameters map.
// Exported so the API service can compare with the applied annotation.
func MutableParamsHash(params map[string]string) string {
	keys := slices.Sorted(maps.Keys(params))

	h := sha256.New()

	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(params[k]))
		h.Write([]byte{0})
	}

	return fmt.Sprintf("%x", h.Sum(nil)[:16])
}

// reconcileExpand resizes the backing disk for a provisioned volume.
//
// Behavior matches direct mode ControllerExpandVolume:
//   - If the volume is attached, resize on the attached VM and report
//     node_expansion_required (reflected as status.capacityBytes update).
//   - If detached, direct mode returns an error ("cannot resize unpublished").
//     The operator matches that behavior by leaving status.capacityBytes
//     unchanged and requeueing: the API returns Unavailable until the
//     attachment reconciler attaches the disk and the expand can proceed.
func (r *Reconciler) reconcileExpand(ctx context.Context, vol *v1alpha1.ProxmoxVolume, sizeBytes int64) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	pveVol, err := pvevolume.NewVolumeFromVolumeID(vol.Status.VolumeID)
	if err != nil {
		r.setCondition(vol, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonInvalidSpec,
			fmt.Sprintf("invalid volumeID %q: %v", vol.Status.VolumeID, err))

		return ctrl.Result{}, r.updateStatus(ctx, vol)
	}

	// Find an attachment for this volume to get the VMID and LUN.
	attachments := &v1alpha1.ProxmoxVolumeAttachmentList{}
	if err := r.Client.List(ctx, attachments, client.InNamespace(vol.Namespace)); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing attachments: %w", err)
	}

	var att *v1alpha1.ProxmoxVolumeAttachment

	for i := range attachments.Items {
		a := &attachments.Items[i]
		if a.Spec.VolumeName == vol.Name && a.Status.Attached {
			att = a

			break
		}
	}

	if att == nil {
		// Detached volume: direct mode returns "cannot resize unpublished".
		// Requeue and wait for attachment.
		logger.V(3).Info("volume not attached, cannot resize unpublished volume",
			"volume", vol.Name, "volumeID", vol.Status.VolumeID)

		return ctrl.Result{RequeueAfter: r.syncPeriod()}, nil
	}

	vmid := int(att.Spec.VMID)
	region := pveVol.Region()

	// Determine the device from the LUN.
	lun := att.Status.LUN
	device := "scsi" + lun

	sizeStr := fmt.Sprintf("%dM", sizeBytes/csi.MiB)

	logger.Info("resizing disk",
		"volume", vol.Name, "vmid", vmid, "device", device, "size", sizeStr)

	if err := r.Writer.ResizeDisk(ctx, region, vmid, pveVol.Zone(), device, sizeStr); err != nil {
		r.setCondition(vol, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonProxmoxError,
			fmt.Sprintf("resizing disk: %v", err))

		return ctrl.Result{RequeueAfter: r.syncPeriod()}, errors2(r.updateStatus(ctx, vol), err)
	}

	now := metav1.NewTime(r.now())

	vol.Status.CapacityBytes = sizeBytes
	vol.Status.LastSyncTime = &now

	r.setCondition(vol, v1alpha1.ConditionReady, metav1.ConditionTrue, v1alpha1.ReasonAccepted,
		fmt.Sprintf("disk expanded to %d bytes", sizeBytes))

	logger.Info("volume expanded",
		"volume", vol.Name, "vmid", vmid, "size", sizeBytes)

	return ctrl.Result{RequeueAfter: r.syncPeriod()}, r.updateStatus(ctx, vol)
}

// provisionFromSource copies an existing volume or snapshot disk for clone/restore
// operations.
func (r *Reconciler) provisionFromSource(
	ctx context.Context,
	vol *v1alpha1.ProxmoxVolume,
	destVol *pvevolume.Volume,
) error {
	if vol.Spec.Source.VolumeName != "" {
		return r.provisionFromVolume(ctx, vol, destVol)
	}

	if vol.Spec.Source.SnapshotName != "" {
		return r.provisionFromSnapshot(ctx, vol, destVol)
	}

	return fmt.Errorf("source volume or snapshot name is required")
}

// provisionFromVolume copies an existing volume's disk for clone operations.
func (r *Reconciler) provisionFromVolume(
	ctx context.Context,
	vol *v1alpha1.ProxmoxVolume,
	destVol *pvevolume.Volume,
) error {
	srcVol := &v1alpha1.ProxmoxVolume{}
	srcKey := client.ObjectKey{Namespace: vol.Namespace, Name: vol.Spec.Source.VolumeName}

	if err := r.Client.Get(ctx, srcKey, srcVol); err != nil {
		r.setCondition(vol, v1alpha1.ConditionAdmitted, metav1.ConditionFalse, v1alpha1.ReasonSourceNotFound,
			fmt.Sprintf("source volume %s not found: %v", vol.Spec.Source.VolumeName, err))

		vol.Status.Phase = v1alpha1.VolumePhaseRejected

		return r.updateStatus(ctx, vol)
	}

	if srcVol.Status.Phase != v1alpha1.VolumePhaseReady {
		r.setCondition(vol, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonReconciling,
			fmt.Sprintf("source volume %s is not ready", vol.Spec.Source.VolumeName))

		vol.Status.Phase = v1alpha1.VolumePhasePending

		return r.updateStatus(ctx, vol)
	}

	return r.Writer.CopyDisk(ctx,
		srcVol.Spec.Region, srcVol.Spec.Zone, srcVol.Spec.Storage, srcVol.Status.DiskName,
		vol.Spec.Zone, vol.Spec.Storage, destVol.Disk())
}

// provisionFromSnapshot restores a volume from a ProxmoxVolumeSnapshot.
//
// The source may be identified by snapshot CR name or by snapshotID (the volume
// handle). The remote controller sends the snapshotID because that is what the
// CSI sidecar carries; the API passes it through as SnapshotName. We try the
// name as a CR lookup first, then fall back to resolving by snapshotID.
func (r *Reconciler) provisionFromSnapshot(
	ctx context.Context,
	vol *v1alpha1.ProxmoxVolume,
	destVol *pvevolume.Volume,
) error {
	snap := &v1alpha1.ProxmoxVolumeSnapshot{}
	snapKey := client.ObjectKey{Namespace: vol.Namespace, Name: vol.Spec.Source.SnapshotName}

	err := r.Client.Get(ctx, snapKey, snap)
	if err != nil {
		// The name might be a snapshotID (handle) rather than a CR name.
		// List snapshots in the namespace and find by status.snapshotID.
		list := &v1alpha1.ProxmoxVolumeSnapshotList{}
		if listErr := r.Client.List(ctx, list, client.InNamespace(vol.Namespace)); listErr != nil {
			r.setCondition(vol, v1alpha1.ConditionAdmitted, metav1.ConditionFalse, v1alpha1.ReasonSourceNotFound,
				fmt.Sprintf("listing snapshots: %v", listErr))

			vol.Status.Phase = v1alpha1.VolumePhaseRejected

			return r.updateStatus(ctx, vol)
		}

		found := false

		for i := range list.Items {
			if list.Items[i].Status.SnapshotID == vol.Spec.Source.SnapshotName {
				snap = &list.Items[i]
				found = true

				break
			}
		}

		if !found {
			r.setCondition(vol, v1alpha1.ConditionAdmitted, metav1.ConditionFalse, v1alpha1.ReasonSourceNotFound,
				fmt.Sprintf("snapshot %s not found", vol.Spec.Source.SnapshotName))

			vol.Status.Phase = v1alpha1.VolumePhaseRejected

			return r.updateStatus(ctx, vol)
		}
	}

	if !snap.Status.ReadyToUse {
		r.setCondition(vol, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonReconciling,
			fmt.Sprintf("snapshot %s is not ready", snap.Name))

		vol.Status.Phase = v1alpha1.VolumePhasePending

		return r.updateStatus(ctx, vol)
	}

	// Parse the snapshotID to get the disk location for the copy.
	snapVol, parseErr := pvevolume.NewVolumeFromVolumeID(snap.Status.SnapshotID)
	if parseErr != nil {
		r.setCondition(vol, v1alpha1.ConditionAdmitted, metav1.ConditionFalse, v1alpha1.ReasonInvalidSpec,
			fmt.Sprintf("invalid snapshotID %q: %v", snap.Status.SnapshotID, parseErr))

		vol.Status.Phase = v1alpha1.VolumePhaseRejected

		return r.updateStatus(ctx, vol)
	}

	return r.Writer.CopyDisk(ctx,
		snapVol.Region(), snapVol.Zone(), snapVol.Storage(), snapVol.Disk(),
		vol.Spec.Zone, vol.Spec.Storage, destVol.Disk())
}

// reconcileDeleteProvisioned handles deletion of a provisioned (non-adopted) volume.
func (r *Reconciler) reconcileDeleteProvisioned(ctx context.Context, vol *v1alpha1.ProxmoxVolume) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Check if any ProxmoxVolumeAttachment references this volume.
	attachments := &v1alpha1.ProxmoxVolumeAttachmentList{}
	if err := r.Client.List(ctx, attachments, client.InNamespace(vol.Namespace)); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing attachments: %w", err)
	}

	for i := range attachments.Items {
		att := &attachments.Items[i]
		if att.Spec.VolumeName == vol.Name {
			r.setCondition(vol, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonReconciling,
				fmt.Sprintf("attachment %s still references this volume", att.Name))

			vol.Status.Phase = v1alpha1.VolumePhaseDeleting

			return ctrl.Result{RequeueAfter: r.syncPeriod()}, r.updateStatus(ctx, vol)
		}
	}

	// Delete the actual disk.
	if vol.Status.DiskName != "" {
		logger.Info("deleting disk for provisioned volume",
			"volume", vol.Name, "namespace", vol.Namespace, "diskName", vol.Status.DiskName)

		if err := r.Writer.DeleteDisk(ctx, vol.Spec.Region, vol.Spec.Zone, vol.Spec.Storage, vol.Status.DiskName); err != nil {
			r.setCondition(vol, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonProxmoxError,
				fmt.Sprintf("deleting disk: %v", err))

			vol.Status.Phase = v1alpha1.VolumePhaseDeleting

			return ctrl.Result{RequeueAfter: r.syncPeriod()}, errors2(r.updateStatus(ctx, vol), err)
		}
	}

	controllerutil.RemoveFinalizer(vol, v1alpha1.VolumeFinalizer)

	if err := r.Client.Update(ctx, vol); err != nil {
		return ctrl.Result{}, fmt.Errorf("releasing %s/%s: %w", vol.Namespace, vol.Name, err)
	}

	return ctrl.Result{}, nil
}

// quotaError is a rejection with its gRPC-mapped reason.
type quotaError struct {
	reason  string
	message string
}

func (e *quotaError) Error() string { return e.message }

// checkParameterPolicy validates parameters against the tenant's policy.
func checkParameterPolicy(policy *v1alpha1.ParameterPolicy, params map[string]string, capacityBytes int64) error {
	if policy == nil {
		return nil
	}

	// Check allowed keys.
	if policy.Allowed != nil {
		for key := range params {
			if !slices.Contains(policy.Allowed, key) {
				return fmt.Errorf("parameter %q is not in the allowed list", key)
			}
		}
	}

	// Check maxInt bounds.
	for key, max := range policy.MaxInt {
		val, ok := params[key]
		if !ok {
			continue
		}

		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return fmt.Errorf("parameter %q must be an integer: %v", key, err)
		}

		if n > max {
			return fmt.Errorf("parameter %q value %d exceeds maximum %d", key, n, max)
		}
	}

	// Check maxVolumeBytes.
	if policy.MaxVolumeBytes != nil {
		maxBytes := policy.MaxVolumeBytes.Value()
		if capacityBytes > maxBytes {
			return fmt.Errorf("requested capacity %d exceeds maxVolumeBytes %d", capacityBytes, maxBytes)
		}
	}

	return nil
}

// checkQuota validates the volume against the tenant's aggregate quota.
func checkQuota(tenant *v1alpha1.TenantCluster, vol *v1alpha1.ProxmoxVolume) *quotaError {
	if tenant.Spec.Quota == nil {
		return nil
	}

	if tenant.Spec.Quota.MaxVolumes != nil {
		if tenant.Status.Used.Volumes+1 > *tenant.Spec.Quota.MaxVolumes {
			return &quotaError{
				reason:  v1alpha1.ReasonQuotaExceeded,
				message: fmt.Sprintf("tenant %s has %d volumes, max is %d", tenant.Name, tenant.Status.Used.Volumes, *tenant.Spec.Quota.MaxVolumes),
			}
		}
	}

	if maxCap, ok := tenant.Spec.Quota.MaxCapacity[vol.Spec.Storage]; ok {
		used := int64(0)

		if usedQ, ok := tenant.Status.Used.Capacity[vol.Spec.Storage]; ok {
			used = usedQ.Value()
		}

		if used+vol.Spec.CapacityBytes > maxCap.Value() {
			return &quotaError{
				reason: v1alpha1.ReasonQuotaExceeded,
				message: fmt.Sprintf("tenant %s storage %s would use %d bytes, max is %s",
					tenant.Name, vol.Spec.Storage, used+vol.Spec.CapacityBytes, maxCap.String()),
			}
		}
	}

	return nil
}

// checkNamespaceQuota validates the volume against per-namespace quotas.
func checkNamespaceQuota(tenant *v1alpha1.TenantCluster, vol *v1alpha1.ProxmoxVolume) *quotaError {
	if len(tenant.Spec.NamespaceQuotas) == 0 || vol.Spec.ClaimRef.Namespace == "" {
		return nil
	}

	var nsQuota *v1alpha1.NamespaceQuota

	for i := range tenant.Spec.NamespaceQuotas {
		if tenant.Spec.NamespaceQuotas[i].Namespace == vol.Spec.ClaimRef.Namespace {
			nsQuota = &tenant.Spec.NamespaceQuotas[i]

			break
		}
	}

	if nsQuota == nil {
		// No quota configured for this namespace means unlimited.
		return nil
	}

	// Find usage for this namespace.
	var nsUsage *v1alpha1.NamespaceUsage

	for i := range tenant.Status.NamespaceUsed {
		if tenant.Status.NamespaceUsed[i].Namespace == vol.Spec.ClaimRef.Namespace {
			nsUsage = &tenant.Status.NamespaceUsed[i]

			break
		}
	}

	currentVolumes := int32(0)
	currentCapacity := int64(0)

	if nsUsage != nil {
		currentVolumes = nsUsage.Volumes

		if nsCap, ok := nsUsage.Capacity[vol.Spec.Storage]; ok {
			currentCapacity = nsCap.Value()
		}
	}

	if nsQuota.MaxVolumes != nil && currentVolumes+1 > *nsQuota.MaxVolumes {
		return &quotaError{
			reason: v1alpha1.ReasonNamespaceQuota,
			message: fmt.Sprintf("namespace %s has %d volumes, max is %d",
				vol.Spec.ClaimRef.Namespace, currentVolumes, *nsQuota.MaxVolumes),
		}
	}

	if maxCap, ok := nsQuota.MaxCapacity[vol.Spec.Storage]; ok {
		if currentCapacity+vol.Spec.CapacityBytes > maxCap.Value() {
			return &quotaError{
				reason: v1alpha1.ReasonNamespaceQuota,
				message: fmt.Sprintf("namespace %s storage %s would use %d bytes, max is %s",
					vol.Spec.ClaimRef.Namespace, vol.Spec.Storage, currentCapacity+vol.Spec.CapacityBytes, maxCap.String()),
			}
		}
	}

	return nil
}

// storagePluginType looks up the Proxmox storage backend type from the published
// ProxmoxStorage catalog. Returns an error if the storage is unknown.
func (r *Reconciler) storagePluginType(ctx context.Context, region, zone, storageName string) (string, error) {
	list := &v1alpha1.ProxmoxStorageList{}
	if err := r.Client.List(ctx, list); err != nil {
		return "", fmt.Errorf("listing storages: %w", err)
	}

	for i := range list.Items {
		ps := &list.Items[i]

		if ps.Spec.Region != region || ps.Spec.Storage != storageName {
			continue
		}

		// For non-shared storage, also match the zone.
		if !ps.Spec.Shared && zone != "" && ps.Spec.Zone != zone {
			continue
		}

		if ps.Spec.PluginType == "" {
			return "", fmt.Errorf("storage %s has no pluginType in the catalog", storageName)
		}

		return ps.Spec.PluginType, nil
	}

	return "", fmt.Errorf("storage %s not found in the catalog for region %s", storageName, region)
}
