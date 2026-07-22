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
	"time"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	tools "github.com/sergelogvinov/proxmox-csi-plugin/pkg/tools/kubernetes"
	volume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
)

// maxClassReconciliations bounds the adaptive class reconciliation during the
// bind wait: an owner that keeps recreating the PVC with ever-different
// storage classes gets at most this many re-reservations before the rewire
// fails loudly (with the data PV still protected by Retain).
const maxClassReconciliations = 2

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

// Kubernetes PV-controller / scheduler bookkeeping annotation keys, declared
// here because upstream keeps them in internal packages.
const (
	annBindCompleted          = "pv.kubernetes.io/bind-completed"
	annBoundByController      = "pv.kubernetes.io/bound-by-controller"
	annSelectedNode           = "volume.kubernetes.io/selected-node"
	annStorageProvisioner     = "volume.kubernetes.io/storage-provisioner"
	annBetaStorageProvisioner = "volume.beta.kubernetes.io/storage-provisioner"
)

// binderBookkeepingAnnotations must be stripped from the recreated PVC: they
// are the difference between a fresh manifest and a copy of a previously BOUND
// claim. In particular:
//
//   - pv.kubernetes.io/bind-completed: with this present and spec.volumeName
//     EMPTY, the PV controller treats the claim as having LOST its volume —
//     phase Lost, and it is NEVER considered for binding again. The reserved
//     PV then sits Available (correct empty-UID claimRef and all) while the
//     bind wait times out.
//   - pv.kubernetes.io/bound-by-controller: same bookkeeping family; stale on
//     a recreate.
//   - volume.kubernetes.io/selected-node: the WaitForFirstConsumer scheduler
//     pin. Copied over, it would pin binding to the OLD node — typically the
//     very node being evacuated.
//   - volume.kubernetes.io/storage-provisioner (+ the legacy beta key):
//     provisioner bookkeeping; stripped for clean fresh-manifest semantics
//     (they are re-set if ever needed).
var binderBookkeepingAnnotations = []string{
	annBindCompleted,
	annBoundByController,
	annSelectedNode,
	annStorageProvisioner,
	annBetaStorageProvisioner,
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
//  4. Recreate the PVC. Binding is via the reserved PV's claimRef ONLY, for both
//     the unmanaged case (the migrator creates the PVC) and the managed case (a
//     controller's own recreate wins the create): neither PVC carries a
//     spec.volumeName. Setting volumeName as well as the empty-UID claimRef is a
//     double pre-bind the apiserver does NOT complete — it leaves the PVC Lost and
//     the PV Available with a never-populated claimRef UID — so the migrator's own
//     recreate must NOT set volumeName either; it lets the binder populate the UID
//     and bind. A create that returns AlreadyExists is therefore success, not an
//     error — there is no PVC update fallback (a bound PVC's volumeName is immutable
//     and the migrator's RBAC intentionally omits the update verb).
//  5. Verify the PVC is bound to the reserved PV. The wait is ADAPTIVE: an owner
//     manifest may pin a storageClassName that differs from the reserved PV's
//     (e.g. StatefulSet volumeClaimTemplates), in which case the recreated PVC
//     could never bind the reservation. The wait then re-reserves the data PV
//     under the class the recreated PVC actually carries (bounded, see
//     maxClassReconciliations) instead of failing outright. If the PVC bound a
//     different PV of the SAME class, a provisioner won a race the reservation
//     should have prevented — return a clear error; the moved disk is safe (its
//     PV is Retain and Available).
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

	// The racing-pair safety guard compares a racing PV's creation time against
	// this mark. It is captured WITHOUT truncation so a PV stamped in the same
	// integer second as the start (creationTimestamp is second-granular) does
	// NOT slip through as "fresh": deleteRacingPair requires the racing PV to be
	// created strictly after this instant before it will delete it.
	started := time.Now()

	newPVC := pvc.DeepCopy()
	newPVC.ObjectMeta.UID = ""
	newPVC.ObjectMeta.ResourceVersion = ""
	newPVC.ObjectMeta.DeletionTimestamp = nil
	newPVC.ObjectMeta.DeletionGracePeriodSeconds = nil
	// Clear the copied bound volumeName: the recreated PVC must bind via the
	// reserved PV's claimRef ONLY. Carrying a volumeName (whether the old bound
	// name or the reservation's) alongside the empty-UID claimRef is a double
	// pre-bind the apiserver leaves in Lost/Available (see step 4 above).
	newPVC.Spec.VolumeName = ""

	for _, a := range migrationAnnotations {
		delete(newPVC.ObjectMeta.Annotations, a)
	}

	// Strip the binder's bookkeeping too — newPVC is a DeepCopy of the OLD
	// bound claim, and a recreate must look like a fresh manifest. Keeping
	// pv.kubernetes.io/bind-completed alongside the cleared volumeName makes
	// the PV controller mark the recreated claim Lost and never bind it (the
	// reservation stays Available while the bind wait times out), and a copied
	// volume.kubernetes.io/selected-node would pin a WaitForFirstConsumer bind
	// to the node being evacuated. See binderBookkeepingAnnotations.
	for _, a := range binderBookkeepingAnnotations {
		delete(newPVC.ObjectMeta.Annotations, a)
	}

	newPVC.Status = corev1.PersistentVolumeClaimStatus{}
	newPVC.Spec.Resources.Requests = corev1.ResourceList{
		corev1.ResourceStorage: pvc.Status.Capacity[corev1.ResourceStorage],
	}

	// Resume-safe reservation: a previous interrupted attempt may already have
	// left exactly this reservation — a Retain, Available (unbound) PV that
	// carries the moved data handle and a claimRef to this PVC. Adopt it rather
	// than creating a second Retain PV of the same disk (an orphan duplicate
	// across retries/resumes).
	existingReservation := m.findExistingReservation(ctx, namespace, pvc.Name, targetVol.VolumeID())

	// A fresh PV name (never the old one) unless a prior reservation is reused:
	// the reserved PV must be created while the old PV still exists so the
	// reservation predates any controller-driven PVC recreation, which rules out
	// reusing the old PV's name.
	newPVName := "pvc-" + string(uuid.NewUUID())
	if existingReservation != nil {
		newPVName = existingReservation.Name
	}

	newPV := pv.DeepCopy()
	newPV.ObjectMeta.Name = newPVName
	newPV.ObjectMeta.UID = ""
	newPV.ObjectMeta.ResourceVersion = ""
	newPV.ObjectMeta.CreationTimestamp = metav1.Time{}
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

	// The StorageClass of the rewired pair: when exactly one of the driver's
	// StorageClasses names the TARGET storage, both the reserved PV and the
	// recreated PVC adopt it. A controller-managed owner (Argo CD, StatefulSet,
	// operator) recreates the PVC from its own manifest, which typically omits
	// storageClassName — admission then fills in the cluster default. If the
	// reserved PV kept the OLD class after a cross-storage move, that recreated
	// PVC's class would not match, the apiserver would refuse the claimRef
	// pre-bind, and the dynamic provisioner would bind a fresh EMPTY volume (a
	// loud PVCWaitBound failure). Aligning both sides with the target storage's
	// class lets the reservation bind for the migrator's own recreate AND for a
	// managed recreate defaulted to the target storage's class. Zero or multiple
	// matching classes fall back to copying the old class unchanged.
	if sc, ok := m.storageClassForStorage(ctx, targetVol.Storage()); ok {
		m.logf("rewired PV/PVC adopt storageclass %s (the class of target storage %s)", sc, targetVol.Storage())

		newPV.Spec.StorageClassName = sc
		newPVC.Spec.StorageClassName = &sc
	} else {
		m.logf("rewired PV/PVC keep storageclass %q (no single storageclass names target storage %s)",
			newPV.Spec.StorageClassName, targetVol.Storage())
	}

	if err := validateReservedPV(newPV, newPVC); err != nil {
		return err
	}

	// Quota pre-flight: when the rewired pair changes StorageClass, the
	// recreated PVC starts charging the NEW class's ResourceQuota dimensions.
	// Abort before any destructive or mutating step if the namespace quota
	// cannot fit it — the old PVC/PV stay untouched and retries stay cheap
	// (a disk copy already on the target is picked up by the resume path).
	if pvcClassName(newPVC) != pvcClassName(pvc) {
		if err := m.checkTargetClassQuota(ctx, namespace, pvcClassName(newPVC), newPVC.Spec.Resources.Requests[corev1.ResourceStorage]); err != nil {
			return err
		}
	}

	// 1. Guard the moved disk before anything destructive: with Retain, deleting
	// the old PVC or PV object can never delete a backing disk.
	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		if err := m.setPVReclaimPolicy(ctx, pv.Name, corev1.PersistentVolumeReclaimRetain); err != nil {
			return fmt.Errorf("failed to set old PV %s to Retain before rewire: %v", pv.Name, err)
		}
	}

	// 2. Create the reservation before the old PVC is deleted, so it is already in
	// place when a controller recreates the PVC. A reused prior reservation is
	// already present, so only create when there is none.
	if existingReservation == nil {
		if _, err := m.KClient.CoreV1().PersistentVolumes().Create(ctx, newPV, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to create reserved PV %s: %v", newPVName, err)
		}
	} else {
		m.logf("reusing existing reserved PV %s for moved data disk %s (resume), not creating a duplicate", newPVName, targetVol.VolumeID())
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
	// empty volume, adapting the reservation's StorageClass to an owner-pinned
	// class when needed. The bound name may differ from newPVName after a
	// re-reservation.
	boundPVName, err := m.waitReservedPVBind(ctx, namespace, pvc.Name, newPV, started)
	if err != nil {
		return err
	}

	// 6. Restore the intended final reclaim policy now that the PV is bound.
	if finalPolicy != corev1.PersistentVolumeReclaimRetain {
		if err := m.setPVReclaimPolicy(ctx, boundPVName, finalPolicy); err != nil {
			return fmt.Errorf("failed to restore reclaim policy on PV %s: %v", boundPVName, err)
		}
	}

	return nil
}

// pvcClassName reads a PVC's storage class the way the Kubernetes binder does:
// a nil storageClassName means "".
func pvcClassName(pvc *corev1.PersistentVolumeClaim) string {
	if pvc.Spec.StorageClassName == nil {
		return ""
	}

	return *pvc.Spec.StorageClassName
}

// waitReservedPVBind waits until the PVC binds the reserved data PV, adapting
// the reservation to the class the recreated PVC actually carries.
//
// A managed owner (StatefulSet volumeClaimTemplates, an operator) may recreate
// the PVC with a PINNED storageClassName that differs from the reserved PV's:
// that PVC can never bind the reservation, and a dynamic provisioner would
// eventually hand it a fresh EMPTY volume. Instead of failing outright, the
// wait reconciles — it re-reserves the data PV under the observed class (and
// removes a racing freshly provisioned empty PVC/PV pair when one already
// bound) — at most maxClassReconciliations times before failing loudly with
// the data PV still Retain-protected.
//
// It returns the name of the reserved PV the PVC finally bound (which differs
// from the initial reservation after a reconciliation).
func (m *Migrator) waitReservedPVBind(ctx context.Context, namespace, pvcName string, reserved *corev1.PersistentVolume, started time.Time) (string, error) {
	timeout := time.After(5 * time.Minute)
	reconciliations := 0

	for {
		pvc, err := m.KClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})

		switch {
		case apierrors.IsNotFound(err):
			// The owner has not recreated the PVC yet: keep waiting.
		case err != nil:
			return "", fmt.Errorf("failed to get PersistentVolumeClaim %s: %v", pvcName, err)
		case pvc.Spec.VolumeName == reserved.Name:
			return reserved.Name, nil
		case pvcClassName(pvc) != reserved.Spec.StorageClassName:
			// The recreated PVC pins a class the reservation does not carry:
			// it can never bind the data PV as reserved. Adapt (bounded).
			if reconciliations >= maxClassReconciliations {
				// Wrap ErrInvalidTarget so the controller classifies this as
				// TERMINAL (a stuck misconfiguration) and stops backoff-looping,
				// consistent with the quota abort.
				return "", fmt.Errorf("%w: PersistentVolumeClaim %s still carries storageclass %q after %d re-reservations; giving up — the migrated disk is preserved on PV %s (Available, Retain)",
					ErrInvalidTarget, pvcName, pvcClassName(pvc), reconciliations, reserved.Name)
			}

			reconciliations++

			if reserved, err = m.reconcileReservedClass(ctx, namespace, pvc, reserved, started); err != nil {
				return "", err
			}

			continue
		case pvc.Spec.VolumeName != "":
			return "", fmt.Errorf("PersistentVolumeClaim %s bound to %s, not the reserved volume %s: a controller provisioned a new volume; the migrated disk is preserved on PV %s (Available, Retain)",
				pvcName, pvc.Spec.VolumeName, reserved.Name, reserved.Name)
		case m.reservedPVCIsIdleWFFC(ctx, namespace, pvc, reserved):
			// A WaitForFirstConsumer PVC with no consumer stays Pending
			// indefinitely — binding is deferred to pod scheduling. The
			// reservation is intact and targets this PVC, so it will bind the
			// moved data disk the moment a consumer schedules, exactly as
			// intended. Treat that as success instead of a false timeout.
			m.logf("PVC %s/%s is a WaitForFirstConsumer claim with no consumer; the reserved data PV %s will bind when a consumer schedules", namespace, pvcName, reserved.Name)

			return reserved.Name, nil
		}

		select {
		case <-time.After(2 * time.Second):
		case <-timeout:
			return "", fmt.Errorf("timeout waiting for PersistentVolumeClaim %s to bind to %s", pvcName, reserved.Name)
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

// reconcileReservedClass adapts the reservation to the storage class the
// recreated PVC actually carries: it removes a racing freshly provisioned
// empty PVC/PV pair when the PVC already bound one, then replaces the reserved
// data PV with one carrying the observed class (same volume handle, same
// empty-UID claimRef), so the owner's recreated PVC binds the DATA disk.
func (m *Migrator) reconcileReservedClass(
	ctx context.Context,
	namespace string,
	pvc *corev1.PersistentVolumeClaim,
	reserved *corev1.PersistentVolume,
	started time.Time,
) (*corev1.PersistentVolume, error) {
	observed := pvcClassName(pvc)

	m.logf("recreated PVC carries class %q, re-reserving data PV with class %q", observed, observed)

	if pvc.Spec.VolumeName != "" {
		// The PVC already bound a different volume: a dynamic provisioner won
		// the race with a fresh EMPTY volume. Delete the racing pair (its
		// Delete reclaim policy cleans up the empty disk) so the owner's next
		// recreate binds the data PV instead.
		if err := m.deleteRacingPair(ctx, namespace, pvc, reserved.Spec.CSI.VolumeHandle, started); err != nil {
			return nil, err
		}
	}

	return m.reReserveDataPV(ctx, reserved, observed)
}

// deleteRacingPair deletes a recreated PVC and the freshly provisioned empty PV
// it bound before the reservation could take effect. Safety invariants, asserted
// here rather than assumed and refused (surfacing as a loud failure with the
// data PV still Retain-protected) when any fails:
//   - the racing PV must NOT reference the migrated data disk;
//   - its claimRef must point back at THIS migration's PVC, so a foreign
//     just-provisioned PV whose name was guessed can never be deleted;
//   - it must have been created STRICTLY AFTER this migration's rewire started —
//     an older or same-second PV is somebody's data, not our race loser.
func (m *Migrator) deleteRacingPair(ctx context.Context, namespace string, pvc *corev1.PersistentVolumeClaim, dataVolumeHandle string, started time.Time) error {
	fresh, err := m.KClient.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get racing PV %s: %v", pvc.Spec.VolumeName, err)
	}

	// Invariant: never delete a PV whose volume handle is the moved data disk.
	if fresh.Spec.CSI != nil && fresh.Spec.CSI.VolumeHandle == dataVolumeHandle {
		return fmt.Errorf("refusing to delete PV %s: it references the migrated data disk %s", fresh.Name, dataVolumeHandle)
	}

	// Invariant (defense in depth): the racing PV must claim THIS migration's
	// PVC. Refusing a PV whose claimRef points elsewhere closes the window where
	// a foreign, freshly provisioned PV whose name happened to match could be
	// deleted.
	ref := fresh.Spec.ClaimRef
	if ref == nil || ref.Namespace != namespace || ref.Name != pvc.Name {
		return fmt.Errorf("refusing to delete PV %s: its claimRef (%v) does not point back at the migrating PVC %s/%s",
			fresh.Name, ref, namespace, pvc.Name)
	}

	// Invariant: only a volume provisioned STRICTLY AFTER the rewire started can
	// be the racing empty one. CreationTimestamp is second-granular while
	// `started` is not, so a PV stamped in the same integer second as (or before)
	// the start is treated as NOT fresh and refused — closing the same-second
	// slip-through. An older PV is somebody's data, not our race loser.
	if !fresh.CreationTimestamp.Time.After(started) {
		return fmt.Errorf("refusing to delete PV %s: created %s, not strictly after the rewire started at %s — not a freshly provisioned racing volume",
			fresh.Name, fresh.CreationTimestamp.Format(time.RFC3339), started.Format(time.RFC3339))
	}

	m.logf("deleting racing PVC %s and its freshly provisioned empty PV %s (volume handle %q)", pvc.Name, fresh.Name, volumeHandleOf(fresh))

	if err := m.KClient.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete racing PVC %s: %v", pvc.Name, err)
	}

	if err := m.KClient.CoreV1().PersistentVolumes().Delete(ctx, fresh.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete racing PV %s: %v", fresh.Name, err)
	}

	return nil
}

// reReserveDataPV replaces the reserved data PV with a fresh reservation
// carrying the given storage class (same volume handle, same empty-UID
// claimRef). Safety invariants, asserted in code: the PV being replaced must
// still be the unbound (Available), Retain reservation of the data disk; and
// the replacement is created BEFORE the outdated reservation is deleted, so
// the moved disk is referenced by a Retain PV at every instant.
func (m *Migrator) reReserveDataPV(ctx context.Context, reserved *corev1.PersistentVolume, class string) (*corev1.PersistentVolume, error) {
	current, err := m.KClient.CoreV1().PersistentVolumes().Get(ctx, reserved.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get reserved PV %s: %v", reserved.Name, err)
	}

	// Invariants: only ever replace the reservation while the data disk is
	// provably safe — Retain, unbound, and still carrying the data handle.
	if current.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		return nil, fmt.Errorf("refusing to replace reserved PV %s: reclaim policy is %s, not Retain", current.Name, current.Spec.PersistentVolumeReclaimPolicy)
	}

	if current.Status.Phase == corev1.VolumeBound || (current.Spec.ClaimRef != nil && current.Spec.ClaimRef.UID != "") {
		return nil, fmt.Errorf("refusing to replace reserved PV %s: it is already bound", current.Name)
	}

	if volumeHandleOf(current) != volumeHandleOf(reserved) {
		return nil, fmt.Errorf("refusing to replace PV %s: volume handle %q is not the migrated data disk %q", current.Name, volumeHandleOf(current), volumeHandleOf(reserved))
	}

	next := current.DeepCopy()
	next.ObjectMeta.Name = "pvc-" + string(uuid.NewUUID())
	next.ObjectMeta.UID = ""
	next.ObjectMeta.ResourceVersion = ""
	next.ObjectMeta.CreationTimestamp = metav1.Time{}
	next.Status = corev1.PersistentVolumeStatus{}
	next.Spec.StorageClassName = class

	// Create the replacement before deleting the outdated reservation: the
	// moved data disk stays referenced by a Retain PV at all times.
	if _, err := m.KClient.CoreV1().PersistentVolumes().Create(ctx, next, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("failed to create re-reserved PV %s (class %s): %v", next.Name, class, err)
	}

	if err := m.KClient.CoreV1().PersistentVolumes().Delete(ctx, current.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to delete outdated reserved PV %s: %v", current.Name, err)
	}

	return next, nil
}

// volumeHandleOf returns the CSI volume handle of a PV ("" for non-CSI PVs).
func volumeHandleOf(pv *corev1.PersistentVolume) string {
	if pv.Spec.CSI == nil {
		return ""
	}

	return pv.Spec.CSI.VolumeHandle
}

// findExistingReservation returns a reservation for the moved data disk that a
// previous (interrupted) attempt may have left: a Retain, Available (unbound,
// empty-UID claimRef) PV whose CSI volume handle is dataHandle and whose
// claimRef targets namespace/pvcName. Reusing it instead of creating a second
// reservation prevents a duplicate Retain PV of the same disk across
// retries/resumes. Returns nil when none matches (the common first-attempt case).
func (m *Migrator) findExistingReservation(ctx context.Context, namespace, pvcName, dataHandle string) *corev1.PersistentVolume {
	pvs, err := m.KClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		m.logf("failed to list persistentvolumes to look for an existing reservation, will create a fresh one: %v", err)

		return nil
	}

	for i := range pvs.Items {
		pv := &pvs.Items[i]

		if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
			continue
		}

		if pv.Status.Phase == corev1.VolumeBound {
			continue
		}

		if volumeHandleOf(pv) != dataHandle {
			continue
		}

		ref := pv.Spec.ClaimRef
		if ref == nil || ref.UID != "" || ref.Namespace != namespace || ref.Name != pvcName {
			continue
		}

		return pv
	}

	return nil
}

// reservedPVCIsIdleWFFC reports whether the recreated PVC is a legitimately
// Pending WaitForFirstConsumer claim with no consumer: still unbound, its
// reservation intact, its class WFFC, and no pod using it. Such a claim never
// binds on its own (binding is deferred to pod scheduling), so waiting for a
// bind would time out as a false failure; the intact reservation binds the
// moved data disk the moment a consumer schedules.
func (m *Migrator) reservedPVCIsIdleWFFC(ctx context.Context, namespace string, pvc *corev1.PersistentVolumeClaim, reserved *corev1.PersistentVolume) bool {
	// Only a still-Pending, unbound recreate qualifies; a set volumeName is
	// handled by the bind/rebind cases above (fast for Immediate and for the
	// migrator's own pre-bound recreate).
	if pvc.Spec.VolumeName != "" {
		return false
	}

	// The reservation must still target this PVC.
	ref := reserved.Spec.ClaimRef
	if ref == nil || ref.Namespace != namespace || ref.Name != pvc.Name {
		return false
	}

	// Only defer for a WaitForFirstConsumer class; an Immediate class binds the
	// reservation promptly, so a persistent Pending there is a genuine failure.
	if !m.isWaitForFirstConsumer(ctx, pvcClassName(pvc)) {
		return false
	}

	// A consumer would trigger binding; if one already references the PVC it is
	// not idle and we keep waiting for the (imminent) bind instead. A PENDING
	// consumer counts — the force-migration/reactive case leaves the workload's
	// pod Pending (scheduled or not yet) waiting for exactly this volume, and it
	// is what makes the WaitForFirstConsumer reservation bind — so use
	// PVCConsumers (which counts Pending pods), NOT PVCPodUsage (which excludes
	// them). Only a genuinely consumer-less claim takes the idle-WFFC success
	// path; with a consumer we wait for the real bind.
	pods, err := tools.PVCConsumers(ctx, m.KClient, namespace, pvc.Name)

	return err == nil && len(pods) == 0
}

// isWaitForFirstConsumer reports whether the named StorageClass uses the
// WaitForFirstConsumer volume binding mode. It reads via List (the migrator's
// RBAC grants list, not get, on storageclasses); an unreadable or unknown class
// is reported as not-WFFC so the caller keeps its normal bind wait.
func (m *Migrator) isWaitForFirstConsumer(ctx context.Context, className string) bool {
	if className == "" {
		return false
	}

	scs, err := m.KClient.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		m.logf("failed to list storageclasses to resolve the volume binding mode of %q: %v", className, err)

		return false
	}

	for i := range scs.Items {
		sc := &scs.Items[i]
		if sc.Name == className {
			return sc.VolumeBindingMode != nil && *sc.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer
		}
	}

	return false
}

// checkTargetClassQuota is a best-effort pre-flight run before the rewire's
// first destructive step when the recreated PVC will carry a DIFFERENT
// StorageClass: if a namespace ResourceQuota caps the new class's
// requests.storage or persistentvolumeclaims dimensions without headroom for
// this volume, the recreated PVC would be rejected by the quota admission and
// the migration would strand mid-rewire. A failed quota LIST is only warned
// about (the CLI may run with narrower RBAC than the in-cluster migrator).
func (m *Migrator) checkTargetClassQuota(ctx context.Context, namespace, newClass string, request resource.Quantity) error {
	quotas, err := m.KClient.CoreV1().ResourceQuotas(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		m.logf("WARNING: failed to list resourcequotas in namespace %s, skipping the target-class quota pre-flight: %v", namespace, err)

		return nil
	}

	storageRes := corev1.ResourceName(newClass + ".storageclass.storage.k8s.io/requests.storage")
	countRes := corev1.ResourceName(newClass + ".storageclass.storage.k8s.io/persistentvolumeclaims")

	for i := range quotas.Items {
		quota := &quotas.Items[i]

		hard := quota.Status.Hard
		if len(hard) == 0 {
			hard = quota.Spec.Hard
		}

		if limit, ok := hard[storageRes]; ok {
			free := limit.DeepCopy()
			used := quota.Status.Used[storageRes]
			free.Sub(used)

			if free.Cmp(request) < 0 {
				return fmt.Errorf("%w: resourcequota %s/%s leaves %s of %s for storageclass %s requests.storage, the migrated volume needs %s; raise the quota or choose another target class",
					ErrInvalidTarget, namespace, quota.Name, free.String(), limit.String(), newClass, request.String())
			}
		}

		if limit, ok := hard[countRes]; ok {
			used := quota.Status.Used[countRes]
			if used.Cmp(limit) >= 0 {
				return fmt.Errorf("%w: resourcequota %s/%s allows no more persistentvolumeclaims of storageclass %s (%s of %s used); raise the quota or choose another target class",
					ErrInvalidTarget, namespace, quota.Name, newClass, used.String(), limit.String())
			}
		}
	}

	return nil
}

// storageClassForStorage returns the name of the driver's StorageClass whose
// "storage" parameter equals storageID, when exactly ONE such class exists.
// With zero or multiple matches the choice would be a guess, so the caller
// keeps the old class instead.
func (m *Migrator) storageClassForStorage(ctx context.Context, storageID string) (string, bool) {
	scs, err := m.KClient.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		m.logf("failed to list storageclasses, keeping the old storageclass: %v", err)

		return "", false
	}

	matches := []string{}

	for i := range scs.Items {
		sc := &scs.Items[i]
		if sc.Provisioner == csi.DriverName && sc.Parameters[csi.StorageIDKey] == storageID {
			matches = append(matches, sc.Name)
		}
	}

	if len(matches) != 1 {
		return "", false
	}

	return matches[0], true
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
