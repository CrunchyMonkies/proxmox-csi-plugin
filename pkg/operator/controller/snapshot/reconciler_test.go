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

package snapshot_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	csipkg "github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/controller/snapshot"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"
	pvevolume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	gib = 1024 * 1024 * 1024

	tenantName      = "bne1-cluster1"
	tenantNamespace = "tenant-bne1-cluster1"
)

var at = time.Date(2026, time.October, 5, 12, 0, 0, 0, time.UTC) //nolint:gochecknoglobals

// --- fakeWriter ---

type fakeWriter struct {
	copies  []copyCall
	deletes []deleteCall
	copyErr error
	delErr  error
}

type copyCall struct {
	srcRegion, srcZone, srcStorage, srcDisk string
	dstZone, dstStorage, dstDisk            string
}

type deleteCall struct {
	region, zone, storage, disk string
}

func (f *fakeWriter) CreateDisk(_ context.Context, _, _, _, _ string, _ int64) error { return nil }

func (f *fakeWriter) CopyDisk(_ context.Context, srcRegion, srcZone, srcStorage, srcDisk, dstZone, dstStorage, dstDisk string) error {
	f.copies = append(f.copies, copyCall{srcRegion, srcZone, srcStorage, srcDisk, dstZone, dstStorage, dstDisk})

	return f.copyErr
}

func (f *fakeWriter) DeleteDisk(_ context.Context, region, zone, storage, disk string) error {
	f.deletes = append(f.deletes, deleteCall{region, zone, storage, disk})

	return f.delErr
}

func (f *fakeWriter) AttachDisk(_ context.Context, _ string, _ int, _ *pvevolume.Volume, _ map[string]string) (proxmox.AttachResult, error) {
	return proxmox.AttachResult{}, nil
}

func (f *fakeWriter) DetachDisk(_ context.Context, _ string, _ int, _ *pvevolume.Volume) error {
	return nil
}

func (f *fakeWriter) ResizeDisk(_ context.Context, _ string, _ int, _, _, _ string) error {
	return nil
}

func (f *fakeWriter) UpdateDisk(_ context.Context, _ string, _ int, _ *pvevolume.Volume, _ map[string]string) error {
	return nil
}

func (f *fakeWriter) RenameDisk(_ context.Context, _ string, _ *pvevolume.Volume, _ int) (*pvevolume.Volume, error) {
	return nil, nil
}

func (f *fakeWriter) ClearUnusedDisk(_ context.Context, _ string, _ int, _ *pvevolume.Volume) error {
	return nil
}

// --- helpers ---

func enforceTenant(mutators ...func(*v1alpha1.TenantCluster)) *v1alpha1.TenantCluster {
	obj := &v1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: tenantName},
		Spec: v1alpha1.TenantClusterSpec{
			Namespace:       tenantNamespace,
			Region:          "bne",
			Subject:         "pvx:" + tenantName,
			PlaceholderVMID: 9991,
			AllowedStorages: []string{"local-lvm"},
			Mode:            v1alpha1.TenantModeEnforce,
		},
		Status: v1alpha1.TenantClusterStatus{
			ResolvedVMIDs: []int32{100, 101},
			Conditions: []metav1.Condition{{
				Type:               v1alpha1.ConditionAdmitted,
				Status:             metav1.ConditionTrue,
				Reason:             v1alpha1.ReasonAccepted,
				LastTransitionTime: metav1.NewTime(at),
			}},
		},
	}

	for _, m := range mutators {
		m(obj)
	}

	return obj
}

func readyVolume(name string) *v1alpha1.ProxmoxVolume {
	return &v1alpha1.ProxmoxVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         tenantNamespace,
			Finalizers:        []string{v1alpha1.VolumeFinalizer},
			CreationTimestamp: metav1.NewTime(at.Add(-time.Hour)),
		},
		Spec: v1alpha1.ProxmoxVolumeSpec{
			Region:        "bne",
			Zone:          "pve-1",
			Storage:       "local-lvm",
			CapacityBytes: 1 * gib,
		},
		Status: v1alpha1.ProxmoxVolumeStatus{
			VolumeID:      "bne/pve-1/local-lvm/vm-9991-" + name,
			DiskName:      "vm-9991-" + name,
			OwnerVMID:     9991,
			CapacityBytes: 1 * gib,
			Phase:         v1alpha1.VolumePhaseReady,
		},
	}
}

func snapshotRequest(name, sourceName string, mutators ...func(*v1alpha1.ProxmoxVolumeSnapshot)) *v1alpha1.ProxmoxVolumeSnapshot {
	obj := &v1alpha1.ProxmoxVolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: tenantNamespace,
		},
		Spec: v1alpha1.ProxmoxVolumeSnapshotSpec{
			SourceVolumeName: sourceName,
		},
	}

	for _, m := range mutators {
		m(obj)
	}

	return obj
}

func storageObject(storage, pluginType string, shared bool) *v1alpha1.ProxmoxStorage {
	return &v1alpha1.ProxmoxStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "bne-pve1-" + storage},
		Spec: v1alpha1.ProxmoxStorageSpec{
			Region:     "bne",
			Zone:       "pve-1",
			Storage:    storage,
			PluginType: pluginType,
			Shared:     shared,
		},
		Status: v1alpha1.ProxmoxStorageStatus{Active: true},
	}
}

func newReconciler(t *testing.T, w *fakeWriter, objects ...client.Object) (*snapshot.Reconciler, client.Client) {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.ProxmoxVolumeSnapshot{}, &v1alpha1.ProxmoxVolume{}, &v1alpha1.TenantCluster{}).
		WithObjects(objects...).
		Build()

	return &snapshot.Reconciler{
		Client: c,
		Writer: w,
		Now:    func() time.Time { return at },
	}, c
}

func reconcileSnap(t *testing.T, r *snapshot.Reconciler, name string) ctrl.Result {
	t.Helper()

	result, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: tenantNamespace, Name: name},
	})
	require.NoError(t, err)

	return result
}

func getSnap(t *testing.T, c client.Client, name string) *v1alpha1.ProxmoxVolumeSnapshot {
	t.Helper()

	obj := &v1alpha1.ProxmoxVolumeSnapshot{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: tenantNamespace, Name: name}, obj))

	return obj
}

func snapCondition(t *testing.T, obj *v1alpha1.ProxmoxVolumeSnapshot, conditionType string) *metav1.Condition {
	t.Helper()

	found := apimeta.FindStatusCondition(obj.Status.Conditions, conditionType)
	require.NotNil(t, found, "condition %s should be set", conditionType)

	return found
}

// --- Create tests ---

// TestSnapshotCreateGoldenDiskName verifies that the snapshot disk name and
// snapshotID match what direct mode's CreateSnapshot produces.
//
// Direct mode: vol.CopyVolume(fmt.Sprintf("vm-%d-%s", d.vmID, name))
// where vmID is the controller's placeholder VMID and name is the snapshot
// name. The resulting snapshotID is the VolumeID() of the copy.
func TestSnapshotCreateGoldenDiskName(t *testing.T) {
	vol := readyVolume("pvc-abc")
	snap := snapshotRequest("snapshot-001", "pvc-abc")

	w := &fakeWriter{}
	r, c := newReconciler(t, w,
		enforceTenant(), vol, snap, storageObject("local-lvm", "lvmthin", false))

	reconcileSnap(t, r, "snapshot-001")

	// Compute the expected snapshot name the same way direct mode does.
	srcVol, err := pvevolume.NewVolumeFromVolumeID("bne/pve-1/local-lvm/vm-9991-pvc-abc")
	require.NoError(t, err)

	expectedSnap := srcVol.CopyVolume(fmt.Sprintf("vm-%d-%s", int32(9991), "snapshot-001"))
	expectedSnapshotID := expectedSnap.VolumeID()

	obj := getSnap(t, c, "snapshot-001")
	assert.Equal(t, expectedSnapshotID, obj.Status.SnapshotID,
		"snapshotID must match what direct mode's CreateSnapshot produces")
	assert.True(t, obj.Status.ReadyToUse)
	assert.Equal(t, int64(1*gib), obj.Status.RestoreSizeBytes)
	assert.NotNil(t, obj.Status.CreationTime)

	// Verify conditions are set correctly.
	assert.Equal(t, metav1.ConditionTrue, snapCondition(t, obj, v1alpha1.ConditionAdmitted).Status)
	assert.Equal(t, metav1.ConditionTrue, snapCondition(t, obj, v1alpha1.ConditionReady).Status)

	// The copy call must use the expected disk name.
	require.Len(t, w.copies, 1)
	assert.Equal(t, "vm-9991-pvc-abc", w.copies[0].srcDisk)
	assert.Equal(t, expectedSnap.Disk(), w.copies[0].dstDisk)
	assert.Equal(t, "pve-1", w.copies[0].dstZone)
	assert.Equal(t, "local-lvm", w.copies[0].dstStorage)
}

func TestSnapshotCreateIdempotent(t *testing.T) {
	vol := readyVolume("pvc-abc")
	snap := snapshotRequest("snap-idem", "pvc-abc")

	w := &fakeWriter{}
	r, _ := newReconciler(t, w,
		enforceTenant(), vol, snap, storageObject("local-lvm", "lvmthin", false))

	// First reconcile: copies the disk.
	reconcileSnap(t, r, "snap-idem")
	require.Len(t, w.copies, 1)

	// Second reconcile: already readyToUse, no second copy.
	reconcileSnap(t, r, "snap-idem")
	assert.Len(t, w.copies, 1, "second reconcile must not copy again")
}

// --- Storage restriction tests ---

func TestSnapshotRefusedCIFS(t *testing.T) {
	vol := readyVolume("pvc-cifs")
	vol.Spec.Storage = "cifs-share"
	snap := snapshotRequest("snap-cifs", "pvc-cifs")

	tenant := enforceTenant(func(tc *v1alpha1.TenantCluster) {
		tc.Spec.AllowedStorages = append(tc.Spec.AllowedStorages, "cifs-share")
	})

	w := &fakeWriter{}
	r, c := newReconciler(t, w, tenant, vol, snap, storageObject("cifs-share", "cifs", false))

	reconcileSnap(t, r, "snap-cifs")

	obj := getSnap(t, c, "snap-cifs")
	cond := snapCondition(t, obj, v1alpha1.ConditionAdmitted)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Contains(t, cond.Message, "cifs")
	assert.Empty(t, w.copies)
}

func TestSnapshotRefusedPBS(t *testing.T) {
	vol := readyVolume("pvc-pbs")
	vol.Spec.Storage = "pbs-store"
	snap := snapshotRequest("snap-pbs", "pvc-pbs")

	tenant := enforceTenant(func(tc *v1alpha1.TenantCluster) {
		tc.Spec.AllowedStorages = append(tc.Spec.AllowedStorages, "pbs-store")
	})

	w := &fakeWriter{}
	r, c := newReconciler(t, w, tenant, vol, snap, storageObject("pbs-store", "pbs", false))

	reconcileSnap(t, r, "snap-pbs")

	obj := getSnap(t, c, "snap-pbs")
	cond := snapCondition(t, obj, v1alpha1.ConditionAdmitted)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Contains(t, cond.Message, "pbs")
	assert.Empty(t, w.copies)
}

func TestSnapshotRefusedRBD(t *testing.T) {
	vol := readyVolume("pvc-rbd")
	vol.Spec.Storage = "rbd-pool"
	snap := snapshotRequest("snap-rbd", "pvc-rbd")

	tenant := enforceTenant(func(tc *v1alpha1.TenantCluster) {
		tc.Spec.AllowedStorages = append(tc.Spec.AllowedStorages, "rbd-pool")
	})

	w := &fakeWriter{}
	r, c := newReconciler(t, w, tenant, vol, snap, storageObject("rbd-pool", "rbd", false))

	reconcileSnap(t, r, "snap-rbd")

	obj := getSnap(t, c, "snap-rbd")
	cond := snapCondition(t, obj, v1alpha1.ConditionAdmitted)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Contains(t, cond.Message, "rbd")
	assert.Empty(t, w.copies)
}

func TestSnapshotRefusedShared(t *testing.T) {
	vol := readyVolume("pvc-shared")
	vol.Spec.Storage = "nfs-share"
	snap := snapshotRequest("snap-shared", "pvc-shared")

	tenant := enforceTenant(func(tc *v1alpha1.TenantCluster) {
		tc.Spec.AllowedStorages = append(tc.Spec.AllowedStorages, "nfs-share")
	})

	w := &fakeWriter{}
	r, c := newReconciler(t, w, tenant, vol, snap, storageObject("nfs-share", "nfs", true))

	reconcileSnap(t, r, "snap-shared")

	obj := getSnap(t, c, "snap-shared")
	cond := snapCondition(t, obj, v1alpha1.ConditionAdmitted)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Contains(t, cond.Message, "shared")
	assert.Empty(t, w.copies)
}

// --- Tenant mode tests ---

func TestSnapshotRefusedObserveMode(t *testing.T) {
	vol := readyVolume("pvc-obs")
	snap := snapshotRequest("snap-obs", "pvc-obs")

	tenant := enforceTenant(func(tc *v1alpha1.TenantCluster) {
		tc.Spec.Mode = v1alpha1.TenantModeObserve
	})

	w := &fakeWriter{}
	r, c := newReconciler(t, w, tenant, vol, snap, storageObject("local-lvm", "lvmthin", false))

	reconcileSnap(t, r, "snap-obs")

	obj := getSnap(t, c, "snap-obs")
	cond := snapCondition(t, obj, v1alpha1.ConditionAdmitted)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, v1alpha1.ReasonTenantObserveOnly, cond.Reason)
	assert.Empty(t, w.copies)
}

func TestSnapshotRefusedSuspendedMode(t *testing.T) {
	vol := readyVolume("pvc-sus")
	snap := snapshotRequest("snap-sus", "pvc-sus")

	tenant := enforceTenant(func(tc *v1alpha1.TenantCluster) {
		tc.Spec.Mode = v1alpha1.TenantModeSuspended
	})

	w := &fakeWriter{}
	r, c := newReconciler(t, w, tenant, vol, snap, storageObject("local-lvm", "lvmthin", false))

	reconcileSnap(t, r, "snap-sus")

	obj := getSnap(t, c, "snap-sus")
	cond := snapCondition(t, obj, v1alpha1.ConditionAdmitted)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, v1alpha1.ReasonTenantObserveOnly, cond.Reason)
	assert.Empty(t, w.copies)
}

// --- Delete tests ---

func TestSnapshotDeleteRemovesDisk(t *testing.T) {
	srcVol, err := pvevolume.NewVolumeFromVolumeID("bne/pve-1/local-lvm/vm-9991-pvc-abc")
	require.NoError(t, err)

	expectedSnap := srcVol.CopyVolume(fmt.Sprintf("vm-%d-%s", int32(9991), "snap-del"))

	snap := snapshotRequest("snap-del", "pvc-abc", func(s *v1alpha1.ProxmoxVolumeSnapshot) {
		s.Finalizers = []string{v1alpha1.SnapshotFinalizer}
		s.DeletionTimestamp = &metav1.Time{Time: at}
		s.Status.SnapshotID = expectedSnap.VolumeID()
		s.Status.ReadyToUse = true
	})

	w := &fakeWriter{}
	r, _ := newReconciler(t, w, enforceTenant(), snap)

	reconcileSnap(t, r, "snap-del")

	require.Len(t, w.deletes, 1)
	assert.Equal(t, expectedSnap.Disk(), w.deletes[0].disk)
	assert.Equal(t, "pve-1", w.deletes[0].zone)
	assert.Equal(t, "local-lvm", w.deletes[0].storage)
}

func TestSnapshotDeleteNotFoundIsNoOp(t *testing.T) {
	// Reconciling a snapshot that does not exist: the controller ignores NotFound.
	w := &fakeWriter{}
	r, _ := newReconciler(t, w, enforceTenant())

	result, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: tenantNamespace, Name: "snap-gone"},
	})
	require.NoError(t, err)
	assert.Zero(t, result.RequeueAfter)
	assert.Empty(t, w.deletes)
}

// --- Source not found ---

func TestSnapshotSourceVolumeNotFound(t *testing.T) {
	snap := snapshotRequest("snap-orphan", "pvc-nonexistent")

	w := &fakeWriter{}
	r, c := newReconciler(t, w, enforceTenant(), snap, storageObject("local-lvm", "lvmthin", false))

	reconcileSnap(t, r, "snap-orphan")

	obj := getSnap(t, c, "snap-orphan")
	cond := snapCondition(t, obj, v1alpha1.ConditionAdmitted)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, v1alpha1.ReasonSourceNotFound, cond.Reason)
	assert.Empty(t, w.copies)
}

// --- Namespace scoping: the snapshot resolves sources within its own namespace ---

func TestSnapshotCannotCrossNamespaces(t *testing.T) {
	// Source volume is in a different namespace.
	vol := readyVolume("pvc-other")
	vol.Namespace = "other-ns"

	snap := snapshotRequest("snap-cross", "pvc-other")

	w := &fakeWriter{}
	r, c := newReconciler(t, w, enforceTenant(), vol, snap, storageObject("local-lvm", "lvmthin", false))

	reconcileSnap(t, r, "snap-cross")

	obj := getSnap(t, c, "snap-cross")
	cond := snapCondition(t, obj, v1alpha1.ConditionAdmitted)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, v1alpha1.ReasonSourceNotFound, cond.Reason,
		"source volume in another namespace must not be found")
	assert.Empty(t, w.copies)
}

// --- Failure handling tests ---

func TestSnapshotCopyFailureSetsSnapshotIDAndError(t *testing.T) {
	vol := readyVolume("pvc-fail")
	snap := snapshotRequest("snap-fail", "pvc-fail")

	w := &fakeWriter{copyErr: fmt.Errorf("SSH connection refused")}
	r, c := newReconciler(t, w, enforceTenant(), vol, snap, storageObject("local-lvm", "lvmthin", false))

	// Reconcile returns an error (from the copy failure).
	_, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: tenantNamespace, Name: "snap-fail"},
	})
	require.Error(t, err, "copy failure must propagate as a reconcile error")

	obj := getSnap(t, c, "snap-fail")

	// The snapshotID must be set even on failure so the delete path can
	// clean up the partial disk.
	assert.NotEmpty(t, obj.Status.SnapshotID,
		"snapshotID must be recorded for partial cleanup")
	assert.False(t, obj.Status.ReadyToUse,
		"readyToUse must be false on copy failure")

	// The Ready condition must report the failure.
	cond := snapCondition(t, obj, v1alpha1.ConditionReady)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, v1alpha1.ReasonProxmoxError, cond.Reason)
	assert.Contains(t, cond.Message, "SSH connection refused")
}

func TestSnapshotDeletePartialDiskNotFound(t *testing.T) {
	// A partial snapshot whose target disk does not exist on the storage.
	srcVol, err := pvevolume.NewVolumeFromVolumeID("bne/pve-1/local-lvm/vm-9991-pvc-abc")
	require.NoError(t, err)

	expectedSnap := srcVol.CopyVolume(fmt.Sprintf("vm-%d-%s", int32(9991), "snap-partial"))

	snap := snapshotRequest("snap-partial", "pvc-abc", func(s *v1alpha1.ProxmoxVolumeSnapshot) {
		s.Finalizers = []string{v1alpha1.SnapshotFinalizer}
		s.DeletionTimestamp = &metav1.Time{Time: at}
		s.Status.SnapshotID = expectedSnap.VolumeID()
		s.Status.ReadyToUse = false // Partial -- copy failed.
	})

	// DeleteDisk returns "not found" for the partial disk.
	w := &fakeWriter{delErr: fmt.Errorf("volume 'local-lvm:vm-9991/vm-9991-snap-partial' does not exist")}
	r, _ := newReconciler(t, w, enforceTenant(), snap)

	reconcileSnap(t, r, "snap-partial")

	// The delete was attempted and the "not found" was tolerated.
	require.Len(t, w.deletes, 1)

	// The finalizer was removed -- the snapshot is gone.
	err = r.Client.Get(t.Context(), client.ObjectKey{Namespace: tenantNamespace, Name: "snap-partial"}, &v1alpha1.ProxmoxVolumeSnapshot{})
	assert.True(t, apierrors.IsNotFound(err), "snapshot should be deleted after finalizer removal")
}

func TestSnapshotDeleteRetainsOnRealError(t *testing.T) {
	srcVol, err := pvevolume.NewVolumeFromVolumeID("bne/pve-1/local-lvm/vm-9991-pvc-abc")
	require.NoError(t, err)

	expectedSnap := srcVol.CopyVolume(fmt.Sprintf("vm-%d-%s", int32(9991), "snap-stuck"))

	snap := snapshotRequest("snap-stuck", "pvc-abc", func(s *v1alpha1.ProxmoxVolumeSnapshot) {
		s.Finalizers = []string{v1alpha1.SnapshotFinalizer}
		s.DeletionTimestamp = &metav1.Time{Time: at}
		s.Status.SnapshotID = expectedSnap.VolumeID()
		s.Status.ReadyToUse = false
	})

	// DeleteDisk returns a real error (not "not found").
	w := &fakeWriter{delErr: fmt.Errorf("connection refused")}
	r, _ := newReconciler(t, w, enforceTenant(), snap)

	_, err = r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: tenantNamespace, Name: "snap-stuck"},
	})
	require.Error(t, err, "real delete error must propagate")

	// Object still has finalizer (not released on error).
	obj := &v1alpha1.ProxmoxVolumeSnapshot{}
	require.NoError(t, r.Client.Get(t.Context(), client.ObjectKey{Namespace: tenantNamespace, Name: "snap-stuck"}, obj))
	assert.Contains(t, obj.Finalizers, v1alpha1.SnapshotFinalizer)
}

// Unused import guard.
var _ = csipkg.DriverName
