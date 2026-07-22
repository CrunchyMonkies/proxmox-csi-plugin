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

package migrator

import (
	"context"
	"fmt"

	tools "github.com/sergelogvinov/proxmox-csi-plugin/pkg/tools/kubernetes"
	volume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
)

// migrationAnnotations are stripped from the recreated PVC/PV so a completed
// migration cannot re-trigger itself.
var migrationAnnotations = []string{
	AnnotationMigrate,
	AnnotationMigrateNode,
	AnnotationMigrateForce,
	AnnotationMigrateStorage,
	AnnotationMigratePhase,
	AnnotationMigrateMessage,
	AnnotationMigrateAttempts,
	AnnotationMigrateStartedAt,
	AnnotationMigrateState,
}

// replacePVTopology rewires the PV/PVC pair so it points at the migrated disk's
// new node and storage, using the documented "Reserving a PersistentVolume"
// (claimRef pre-bind) pattern instead of racing a controller to recreate the PVC.
//
// The disk has already been physically copied to the target at this point, so the
// sequence is built to make the moved copy structurally impossible to lose:
//
//  1. Force the OLD PV to reclaimPolicy=Retain, so nothing that follows (deleting
//     the PVC or the PV object) can ever trigger the external provisioner to delete
//     a backing disk.
//  2. Build a fresh reserved PV that carries the target volume handle, target-zone
//     node affinity and a claimRef to the PVC's identity with an EMPTY UID — an
//     empty-UID claimRef is what makes Kubernetes bind a *future* PVC of that
//     name/namespace to this PV in preference to dynamically provisioning a new,
//     empty volume. Create it BEFORE the old PVC is deleted, so the reservation is
//     already in place when a controller (ArgoCD selfHeal, StatefulSet, operator)
//     instantly recreates the PVC.
//  3. Delete the old PVC and then the old (now Retain) PV object — neither deletes
//     a disk.
//  4. Recreate the PVC. For an unmanaged PVC the migrator creates it (pre-bound via
//     spec.volumeName); for a controller-managed PVC the controller's own recreate
//     wins the create and binds to the reserved PV via the claimRef. A create that
//     returns AlreadyExists is therefore success, not an error — there is no PVC
//     update fallback (a bound PVC's volumeName is immutable and the migrator's
//     RBAC intentionally omits the update verb).
//  5. Verify the PVC is bound to the reserved PV. If it bound to a different PV a
//     controller provisioned an empty volume first — return a clear error; the
//     moved disk is safe (its PV is Retain and Available).
//  6. Once bound, restore the PV's intended final reclaim policy.
//
// The source disk copy left at the origin is reclaimed by the caller (Migrate)
// only after this returns success: forcing the old PV to Retain (step 1) stops the
// provisioner from auto-deleting it, so the source stays put as the safety net
// until the moved copy is proven bound here.
func (m *Migrator) replacePVTopology(
	ctx context.Context,
	namespace string,
	pvc *corev1.PersistentVolumeClaim,
	pv *corev1.PersistentVolume,
	targetVol *volume.Volume,
) error {
	// The reclaim policy the migrated volume should end up with: whatever the
	// original PV had (typically Delete for dynamically provisioned volumes, so a
	// later PVC deletion still reclaims the disk). The reserved PV is created as
	// Retain and flipped back to this only after it is safely bound.
	finalPolicy := pv.Spec.PersistentVolumeReclaimPolicy
	if finalPolicy == "" {
		finalPolicy = corev1.PersistentVolumeReclaimDelete
	}

	// 1. Guard the moved disk before anything destructive: with Retain, deleting
	// the old PVC or PV object can never delete a backing disk.
	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		if err := m.setPVReclaimPolicy(ctx, pv.Name, corev1.PersistentVolumeReclaimRetain); err != nil {
			return fmt.Errorf("failed to set old PV %s to Retain before rewire: %v", pv.Name, err)
		}
	}

	newPVC := pvc.DeepCopy()
	newPVC.ObjectMeta.UID = ""
	newPVC.ObjectMeta.ResourceVersion = ""
	newPVC.ObjectMeta.DeletionTimestamp = nil
	newPVC.ObjectMeta.DeletionGracePeriodSeconds = nil

	for _, a := range migrationAnnotations {
		delete(newPVC.ObjectMeta.Annotations, a)
	}

	newPVC.Status = corev1.PersistentVolumeClaimStatus{}
	newPVC.Spec.Resources.Requests = corev1.ResourceList{
		corev1.ResourceStorage: pvc.Status.Capacity[corev1.ResourceStorage],
	}

	// A fresh PV name (never the old one): the reserved PV must be created while
	// the old PV still exists so the reservation predates any controller-driven
	// PVC recreation, which rules out reusing the name.
	newPVName := "pvc-" + string(uuid.NewUUID())

	newPV := pv.DeepCopy()
	newPV.ObjectMeta.Name = newPVName
	newPV.ObjectMeta.UID = ""
	newPV.ObjectMeta.ResourceVersion = ""
	newPV.ObjectMeta.DeletionTimestamp = nil
	newPV.ObjectMeta.DeletionGracePeriodSeconds = nil

	for _, a := range migrationAnnotations {
		delete(newPV.ObjectMeta.Annotations, a)
	}

	newPV.Status = corev1.PersistentVolumeStatus{}
	newPV.Spec.CSI.VolumeHandle = targetVol.VolumeID()
	// Reserved and safe until bound; restored to finalPolicy after the bind.
	newPV.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain

	if newPV.Spec.NodeAffinity == nil {
		newPV.Spec.NodeAffinity = &corev1.VolumeNodeAffinity{}
	}

	newPV.Spec.NodeAffinity.Required = &corev1.NodeSelector{
		NodeSelectorTerms: []corev1.NodeSelectorTerm{
			{
				MatchExpressions: []corev1.NodeSelectorRequirement{
					{
						Key:      corev1.LabelTopologyRegion,
						Operator: "In",
						Values:   []string{targetVol.Region()},
					},
					{
						Key:      corev1.LabelTopologyZone,
						Operator: "In",
						Values:   []string{targetVol.Zone()},
					},
				},
			},
		},
	}

	// The claimRef with an EMPTY UID is the reservation: Kubernetes binds a future
	// PVC of this namespace/name to this PV ahead of dynamic provisioning.
	newPV.Spec.ClaimRef = &corev1.ObjectReference{
		Kind:       "PersistentVolumeClaim",
		APIVersion: "v1",
		Namespace:  namespace,
		Name:       pvc.Name,
	}

	// Pre-bind the migrator's own recreate deterministically for an unmanaged PVC;
	// a controller's recreate (no volumeName) still binds via the claimRef.
	newPVC.Spec.VolumeName = newPVName

	if err := validateReservedPV(newPV, newPVC); err != nil {
		return err
	}

	// 2. Create the reservation before the old PVC is deleted, so it is already in
	// place when a controller recreates the PVC.
	if _, err := m.KClient.CoreV1().PersistentVolumes().Create(ctx, newPV, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create reserved PV %s: %v", newPVName, err)
	}

	// 3. Delete the old PVC then the old (Retain) PV object.
	policy := metav1.DeletePropagationForeground
	if err := m.KClient.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, metav1.DeleteOptions{PropagationPolicy: &policy}); err != nil {
		return fmt.Errorf("failed to delete PVC: %v", err)
	}

	if err := m.KClient.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete old PV %s: %v", pv.Name, err)
	}

	if err := tools.PVWaitDelete(ctx, m.KClient, pv.Name); err != nil {
		return fmt.Errorf("failed to wait for old PV %s deletion: %v", pv.Name, err)
	}

	// 4. Recreate the PVC. AlreadyExists means a controller recreated it first —
	// that is success, the claimRef binds it to the reserved PV. No update fallback.
	if _, err := m.KClient.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, newPVC, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create PVC: %v", err)
	}

	// 5. Verify the PVC bound to the reserved PV and not to a controller-provisioned
	// empty volume.
	if err := tools.PVCWaitBound(ctx, m.KClient, namespace, pvc.Name, newPVName); err != nil {
		return err
	}

	// 6. Restore the intended final reclaim policy now that the PV is bound.
	if finalPolicy != corev1.PersistentVolumeReclaimRetain {
		if err := m.setPVReclaimPolicy(ctx, newPVName, finalPolicy); err != nil {
			return fmt.Errorf("failed to restore reclaim policy on PV %s: %v", newPVName, err)
		}
	}

	return nil
}

// validateReservedPV enforces the invariants the Kubernetes binder needs to bind
// the reserved PV to the recreated PVC: matching storage class, sufficient
// capacity, matching access modes and volume mode, and an empty-UID claimRef.
func validateReservedPV(pv *corev1.PersistentVolume, pvc *corev1.PersistentVolumeClaim) error {
	pvcClass := ""
	if pvc.Spec.StorageClassName != nil {
		pvcClass = *pvc.Spec.StorageClassName
	}

	if pv.Spec.StorageClassName != pvcClass {
		return fmt.Errorf("reserved PV storageClassName %q does not match PVC storageClassName %q; it would not bind",
			pv.Spec.StorageClassName, pvcClass)
	}

	pvCap := pv.Spec.Capacity[corev1.ResourceStorage]
	if req, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok && pvCap.Cmp(req) < 0 {
		return fmt.Errorf("reserved PV capacity %s is smaller than PVC request %s", pvCap.String(), req.String())
	}

	if !accessModesEqual(pv.Spec.AccessModes, pvc.Spec.AccessModes) {
		return fmt.Errorf("reserved PV access modes %v do not match PVC access modes %v", pv.Spec.AccessModes, pvc.Spec.AccessModes)
	}

	if !volumeModesEqual(pv.Spec.VolumeMode, pvc.Spec.VolumeMode) {
		return fmt.Errorf("reserved PV volumeMode %v does not match PVC volumeMode %v", pv.Spec.VolumeMode, pvc.Spec.VolumeMode)
	}

	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != "" {
		return fmt.Errorf("reserved PV claimRef must be set with an empty UID to pre-bind the PVC")
	}

	return nil
}

func accessModesEqual(a, b []corev1.PersistentVolumeAccessMode) bool {
	if len(a) != len(b) {
		return false
	}

	set := make(map[corev1.PersistentVolumeAccessMode]struct{}, len(a))
	for _, m := range a {
		set[m] = struct{}{}
	}

	for _, m := range b {
		if _, ok := set[m]; !ok {
			return false
		}
	}

	return true
}

// volumeModesEqual treats a nil volumeMode as the default (Filesystem), matching
// the Kubernetes binder.
func volumeModesEqual(a, b *corev1.PersistentVolumeMode) bool {
	da := corev1.PersistentVolumeFilesystem
	if a != nil {
		da = *a
	}

	db := corev1.PersistentVolumeFilesystem
	if b != nil {
		db = *b
	}

	return da == db
}
