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

package volume_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/controller/volume"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	gib = 1024 * 1024 * 1024

	tenantName      = "bne1-cluster1"
	tenantNamespace = "tenant-bne1-cluster1"

	// The handle as a PersistentVolume on bne1-cluster1 carries it today: the
	// legacy 9999 placeholder, and a zone because local-lvm is not shared.
	handle = "bne/pve-1/local-lvm/vm-9999-pvc-abc"
)

// at is the reconciler's frozen clock.
var at = time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC) //nolint:gochecknoglobals

var errProxmoxDown = errors.New("connection refused")

// fakeDisks stands in for Proxmox.
//
// It has no method that writes, which is the point rather than a convenience:
// "adoption issues zero Proxmox writes" is asserted by the DiskReader interface
// having nothing to call, and the call counter below covers the other half --
// that a refusal happens before the hypervisor is contacted at all.
type fakeDisks struct {
	disks map[string][]proxmox.Disk
	errs  map[string]error
	calls int
}

func (f *fakeDisks) ListDisks(_ context.Context, region, _, storage string) ([]proxmox.Disk, error) {
	f.calls++

	key := region + "/" + storage

	if err, ok := f.errs[key]; ok {
		return nil, err
	}

	return f.disks[key], nil
}

// onLocalLVM builds a hypervisor holding one disk under the given name.
func onLocalLVM(names ...string) *fakeDisks {
	disks := make([]proxmox.Disk, 0, len(names))

	for _, name := range names {
		disks = append(disks, proxmox.Disk{
			Region: "bne", Node: "pve-1", Storage: "local-lvm", Name: name, VMID: 9999, SizeBytes: 4 * gib,
		})
	}

	return &fakeDisks{disks: map[string][]proxmox.Disk{"bne/local-lvm": disks}}
}

func newReconciler(t *testing.T, disks *fakeDisks, objects ...client.Object) (*volume.Reconciler, client.Client) {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.ProxmoxVolume{}, &v1alpha1.TenantCluster{}).
		// The same index function production registers. An index that existed
		// only here would make every duplicate-claim assertion below a test of
		// the test.
		WithIndex(&v1alpha1.ProxmoxVolume{}, volume.ClaimIndex, volume.IndexClaim).
		WithObjects(objects...).
		Build()

	return &volume.Reconciler{
		Client:  c,
		Proxmox: disks,
		Now:     func() time.Time { return at },
	}, c
}

// admittedTenant builds the registration the volumes below belong to.
func admittedTenant(mutators ...func(*v1alpha1.TenantCluster)) *v1alpha1.TenantCluster {
	obj := &v1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: tenantName},
		Spec: v1alpha1.TenantClusterSpec{
			Namespace:             tenantNamespace,
			Region:                "bne",
			Subject:               "pvx:" + tenantName,
			PlaceholderVMID:       9991,
			LegacyControllerVMIDs: []int32{9999},
			AllowedStorages:       []string{"local-lvm"},
			Mode:                  v1alpha1.TenantModeObserve,
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

	for _, mutate := range mutators {
		mutate(obj)
	}

	return obj
}

// adopting builds an adoption request for the shared handle.
func adopting(name string, mutators ...func(*v1alpha1.ProxmoxVolume)) *v1alpha1.ProxmoxVolume {
	obj := &v1alpha1.ProxmoxVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         tenantNamespace,
			CreationTimestamp: metav1.NewTime(at.Add(-time.Hour)),
		},
		Spec: v1alpha1.ProxmoxVolumeSpec{
			Region:        "bne",
			Zone:          "pve-1",
			Storage:       "local-lvm",
			CapacityBytes: 4 * gib,
			Adopt:         true,
			AdoptVolumeID: handle,
		},
	}

	for _, mutate := range mutators {
		mutate(obj)
	}

	return obj
}

func reconcile(t *testing.T, r *volume.Reconciler, name string) ctrl.Result {
	t.Helper()

	result, err := r.Reconcile(t.Context(), request(name))
	require.NoError(t, err)

	return result
}

func request(name string) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKey{Namespace: tenantNamespace, Name: name}}
}

func get(t *testing.T, c client.Client, name string) *v1alpha1.ProxmoxVolume {
	t.Helper()

	obj := &v1alpha1.ProxmoxVolume{}
	require.NoError(t, c.Get(t.Context(), request(name).NamespacedName, obj))

	return obj
}

func condition(t *testing.T, obj *v1alpha1.ProxmoxVolume, conditionType string) *metav1.Condition {
	t.Helper()

	found := apimeta.FindStatusCondition(obj.Status.Conditions, conditionType)
	require.NotNil(t, found, "condition %s should be set", conditionType)

	return found
}

func TestAdoptRecordsWhatProxmoxSays(t *testing.T) {
	disks := onLocalLVM("vm-9999-pvc-abc")
	r, c := newReconciler(t, disks, admittedTenant(), adopting("pvc-abc"))

	result := reconcile(t, r, "pvc-abc")
	assert.Positive(t, result.RequeueAfter, "an adopted volume is re-confirmed on a timer")

	obj := get(t, c, "pvc-abc")

	// Byte-identical to what the tenant asked to adopt. Every existing
	// PersistentVolume on bne1-cluster1 has this string in spec.csi.volumeHandle
	// and cannot be edited, so this single assertion is the migration's central
	// property: adoption is bookkeeping, not a rename.
	assert.Equal(t, handle, obj.Status.VolumeID)

	assert.Equal(t, "vm-9999-pvc-abc", obj.Status.DiskName)
	assert.Equal(t, int32(9999), obj.Status.OwnerVMID)
	assert.Equal(t, int64(4*gib), obj.Status.CapacityBytes)
	assert.True(t, obj.Status.Adopted)
	assert.Equal(t, v1alpha1.VolumePhaseReady, obj.Status.Phase)
	assert.Equal(t, at.Unix(), obj.Status.LastSyncTime.Unix())

	assert.Equal(t, metav1.ConditionTrue, condition(t, obj, v1alpha1.ConditionAdmitted).Status)
	assert.Equal(t, metav1.ConditionTrue, condition(t, obj, v1alpha1.ConditionReady).Status)
	assert.Equal(t, metav1.ConditionFalse, condition(t, obj, v1alpha1.ConditionDegraded).Status)

	assert.True(t, controllerutil.ContainsFinalizer(obj, v1alpha1.VolumeFinalizer))
	assert.Equal(t, tenantName, obj.Labels[v1alpha1.LabelTenant])
	assert.Equal(t, "local-lvm", obj.Labels[v1alpha1.LabelStorage])
}

func TestAdoptASharedVolume(t *testing.T) {
	// A shared storage pins its volumes to no node, so the handle carries an
	// empty zone and the directory plugin prefixes the disk with its own vmid --
	// two shapes the parsing has to survive, and the ones every rbd volume in the
	// estate is in.
	const shared = "bne//rbd/9999/vm-9999-pvc-shared.raw"

	disks := &fakeDisks{disks: map[string][]proxmox.Disk{"bne/rbd": {{
		Region: "bne", Node: "pve-1", Storage: "rbd", Name: "9999/vm-9999-pvc-shared.raw", VMID: 9999, SizeBytes: gib,
	}}}}

	r, c := newReconciler(t, disks,
		admittedTenant(func(o *v1alpha1.TenantCluster) { o.Spec.AllowedStorages = []string{"rbd"} }),
		adopting("pvc-shared", func(o *v1alpha1.ProxmoxVolume) {
			o.Spec.Zone = ""
			o.Spec.Storage = "rbd"
			o.Spec.AdoptVolumeID = shared
		}))

	reconcile(t, r, "pvc-shared")

	obj := get(t, c, "pvc-shared")

	assert.Equal(t, shared, obj.Status.VolumeID)
	assert.Equal(t, "9999/vm-9999-pvc-shared.raw", obj.Status.DiskName)
	assert.Equal(t, int32(9999), obj.Status.OwnerVMID)
	assert.Equal(t, metav1.ConditionTrue, condition(t, obj, v1alpha1.ConditionReady).Status)
}

func TestAdoptFindsADiskARenameMoved(t *testing.T) {
	// The case that decides whether adoption works on a live cluster at all.
	// With features.reassignVolumeOnAttach on, an attached disk is named for the
	// VM holding it, so every volume with a running pod is on the hypervisor as
	// vm-101-* while its PersistentVolume still says vm-9999-*. Matching on the
	// full name would report AdoptDiskNotFound for exactly the volumes that are
	// in use.
	disks := onLocalLVM("vm-101-pvc-abc")
	r, c := newReconciler(t, disks, admittedTenant(), adopting("pvc-abc"))

	reconcile(t, r, "pvc-abc")

	obj := get(t, c, "pvc-abc")

	assert.Equal(t, handle, obj.Status.VolumeID, "the tenant's handle is republished unchanged")
	assert.Equal(t, "vm-101-pvc-abc", obj.Status.DiskName, "the ledger records the name Proxmox actually has")
	assert.Equal(t, int32(101), obj.Status.OwnerVMID)
	assert.Equal(t, metav1.ConditionTrue, condition(t, obj, v1alpha1.ConditionReady).Status)
}

func TestAdoptRefusesADiskRenamedOntoAVMTheTenantDoesNotOwn(t *testing.T) {
	// Rename tolerance is not a license to adopt whatever happens to carry the
	// suffix. A disk attached to VM 200 belongs to whoever owns VM 200, and
	// recording it here would hand one tenant a handle to another's live volume.
	disks := onLocalLVM("vm-200-pvc-abc")
	r, c := newReconciler(t, disks, admittedTenant(), adopting("pvc-abc"))

	reconcile(t, r, "pvc-abc")

	obj := get(t, c, "pvc-abc")

	assert.Empty(t, obj.Status.VolumeID)
	assert.Equal(t, v1alpha1.VolumePhaseRejected, obj.Status.Phase)
	assert.Equal(t, v1alpha1.ReasonVMIDNotOwned, condition(t, obj, v1alpha1.ConditionAdmitted).Reason)
	assert.False(t, controllerutil.ContainsFinalizer(obj, v1alpha1.VolumeFinalizer))
}

// TestAdoptionIsRefusedBefore reads as the authorization contract: for each way a
// request can be illegitimate, the terminal reason AND that Proxmox was never
// contacted.
func TestAdoptionIsRefusedBefore(t *testing.T) {
	for _, tt := range []struct {
		name    string
		tenant  *v1alpha1.TenantCluster
		volume  *v1alpha1.ProxmoxVolume
		reason  string
		reached bool
	}{
		{
			name:   "no admitted tenant registers the namespace",
			tenant: admittedTenant(func(o *v1alpha1.TenantCluster) { o.Spec.Namespace = "tenant-somebody-else" }),
			volume: adopting("pvc-abc"),
			reason: v1alpha1.ReasonTenantUnknown,
		},
		{
			// An unadmitted registration is not a weaker registration. It is the
			// state a tenant is in while it is losing a uniqueness contest, and
			// honoring it would let the loser adopt.
			name: "the tenant is registered but not admitted",
			tenant: admittedTenant(func(o *v1alpha1.TenantCluster) {
				o.Status.Conditions[0].Status = metav1.ConditionFalse
			}),
			volume: adopting("pvc-abc"),
			reason: v1alpha1.ReasonTenantUnknown,
		},
		{
			name:   "the tenant is suspended",
			tenant: admittedTenant(func(o *v1alpha1.TenantCluster) { o.Spec.Mode = v1alpha1.TenantModeSuspended }),
			volume: adopting("pvc-abc"),
			reason: v1alpha1.ReasonTenantSuspended,
		},
		{
			name:   "adopt is set with no handle",
			tenant: admittedTenant(),
			volume: adopting("pvc-abc", func(o *v1alpha1.ProxmoxVolume) { o.Spec.AdoptVolumeID = "" }),
			reason: v1alpha1.ReasonInvalidSpec,
		},
		{
			name:   "the handle is not a volume handle",
			tenant: admittedTenant(),
			volume: adopting("pvc-abc", func(o *v1alpha1.ProxmoxVolume) { o.Spec.AdoptVolumeID = "local-lvm:vm-9999-pvc-abc" }),
			reason: v1alpha1.ReasonInvalidSpec,
		},
		{
			// Quota is charged against spec.storage. A record whose spec named
			// one storage while its handle named another would charge one
			// budget and occupy space in the other.
			name:   "the spec names a different storage than the handle",
			tenant: admittedTenant(),
			volume: adopting("pvc-abc", func(o *v1alpha1.ProxmoxVolume) { o.Spec.Storage = "rbd" }),
			reason: v1alpha1.ReasonVolumeIDMismatch,
		},
		{
			name:   "the handle names another region than the tenant is registered in",
			tenant: admittedTenant(func(o *v1alpha1.TenantCluster) { o.Spec.Region = "syd" }),
			volume: adopting("pvc-abc"),
			reason: v1alpha1.ReasonInvalidSpec,
		},
		{
			// 9999 is permitted only because this tenant lists it as a legacy
			// placeholder. Without that it is another cluster's disk, and the
			// two clusters currently share the default.
			name: "the handle names a placeholder the tenant has not claimed",
			tenant: admittedTenant(func(o *v1alpha1.TenantCluster) {
				o.Spec.LegacyControllerVMIDs = nil
			}),
			volume: adopting("pvc-abc"),
			reason: v1alpha1.ReasonVMIDNotOwned,
		},
		{
			// The one refusal that has to read the hypervisor, because whether a
			// disk is there is not a question the spec can answer.
			name:    "the disk is not on the storage",
			tenant:  admittedTenant(),
			volume:  adopting("pvc-missing", func(o *v1alpha1.ProxmoxVolume) { o.Spec.AdoptVolumeID = "bne/pve-1/local-lvm/vm-9999-pvc-missing" }),
			reason:  v1alpha1.ReasonAdoptDiskNotFound,
			reached: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			disks := onLocalLVM("vm-9999-pvc-abc")
			r, c := newReconciler(t, disks, tt.tenant, tt.volume)

			reconcile(t, r, tt.volume.Name)

			obj := get(t, c, tt.volume.Name)

			assert.Equal(t, tt.reason, condition(t, obj, v1alpha1.ConditionAdmitted).Reason)
			assert.Equal(t, metav1.ConditionFalse, condition(t, obj, v1alpha1.ConditionAdmitted).Status)
			assert.Empty(t, obj.Status.VolumeID, "a refused volume publishes no handle")
			assert.False(t, controllerutil.ContainsFinalizer(obj, v1alpha1.VolumeFinalizer),
				"a refused volume is not held")

			if tt.reached {
				assert.Equal(t, 1, disks.calls)
			} else {
				assert.Zero(t, disks.calls, "the refusal must not have contacted Proxmox")
			}
		})
	}
}

func TestASecondClaimOnOneDiskIsRefused(t *testing.T) {
	// Runbook step 5. Adopting every cluster before enforcing any is how two
	// clusters that have been sharing a disk are discovered, and this is the
	// check that discovers it -- across namespaces, which is the one rule no
	// per-namespace RBAC or admission policy can express.
	other := &v1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "syd1-mgt1"},
		Spec: v1alpha1.TenantClusterSpec{
			Namespace: "tenant-syd1-mgt1", Region: "bne", PlaceholderVMID: 9992,
			LegacyControllerVMIDs: []int32{9999}, AllowedStorages: []string{"local-lvm"},
			Mode: v1alpha1.TenantModeObserve,
		},
		Status: v1alpha1.TenantClusterStatus{Conditions: []metav1.Condition{{
			Type: v1alpha1.ConditionAdmitted, Status: metav1.ConditionTrue,
			Reason: v1alpha1.ReasonAccepted, LastTransitionTime: metav1.NewTime(at),
		}}},
	}

	// Same handle, later creation. The rule is older-wins, so this one loses
	// however the two reconciles happen to be ordered.
	newcomer := adopting("pvc-abc", func(o *v1alpha1.ProxmoxVolume) {
		o.Namespace = "tenant-syd1-mgt1"
		o.CreationTimestamp = metav1.NewTime(at)
	})

	disks := onLocalLVM("vm-9999-pvc-abc")
	r, c := newReconciler(t, disks, admittedTenant(), other, adopting("pvc-abc"), newcomer)

	_, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: "tenant-syd1-mgt1", Name: "pvc-abc"},
	})
	require.NoError(t, err)

	loser := &v1alpha1.ProxmoxVolume{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: "tenant-syd1-mgt1", Name: "pvc-abc"}, loser))

	assert.Empty(t, loser.Status.VolumeID)
	assert.Equal(t, v1alpha1.ReasonDuplicateClaim, condition(t, loser, v1alpha1.ConditionAdmitted).Reason)
	assert.Equal(t, metav1.ConditionTrue, condition(t, loser, v1alpha1.ConditionDegraded).Status,
		"a shared disk is the finding the runbook stops on, so it is Degraded and not merely refused")
	assert.Zero(t, disks.calls, "the collision is settled from the ledger alone")

	// And the older record is unaffected by the loser's existence.
	reconcile(t, r, "pvc-abc")
	assert.Equal(t, handle, get(t, c, "pvc-abc").Status.VolumeID)
}

func TestAdoptOnAStorageOutsideTheAllowlistIsFlaggedNotRefused(t *testing.T) {
	// Adoption records what already exists. Refusing here would leave the one
	// disk nobody has a record for outside the ledger, which is the opposite of
	// what the allowlist is for -- it governs what may be provisioned next.
	disks := onLocalLVM("vm-9999-pvc-abc")
	r, c := newReconciler(t, disks,
		admittedTenant(func(o *v1alpha1.TenantCluster) { o.Spec.AllowedStorages = []string{"rbd"} }),
		adopting("pvc-abc"))

	reconcile(t, r, "pvc-abc")

	obj := get(t, c, "pvc-abc")

	assert.Equal(t, handle, obj.Status.VolumeID)
	assert.Equal(t, metav1.ConditionTrue, condition(t, obj, v1alpha1.ConditionReady).Status)
	assert.Equal(t, metav1.ConditionTrue, condition(t, obj, v1alpha1.ConditionDegraded).Status)
	assert.Equal(t, v1alpha1.ReasonStorageNotAllowed, condition(t, obj, v1alpha1.ConditionDegraded).Reason)
}

func TestAnUnreadableHypervisorDoesNotRetractAPublishedHandle(t *testing.T) {
	// The failure mode this guards against is the expensive one: a tenant whose
	// PersistentVolume is bound and whose pod is running, having its handle
	// cleared because a content listing timed out.
	adopted := adopting("pvc-abc", func(o *v1alpha1.ProxmoxVolume) {
		o.Finalizers = []string{v1alpha1.VolumeFinalizer}
		o.Status.VolumeID = handle
		o.Status.DiskName = "vm-9999-pvc-abc"
		o.Status.Adopted = true
	})

	disks := &fakeDisks{errs: map[string]error{"bne/local-lvm": errProxmoxDown}}
	r, c := newReconciler(t, disks, admittedTenant(), adopted)

	_, err := r.Reconcile(t.Context(), request("pvc-abc"))
	require.Error(t, err, "an unreachable hypervisor is retried, not accepted as an answer")

	obj := get(t, c, "pvc-abc")

	assert.Equal(t, handle, obj.Status.VolumeID)
	assert.Equal(t, v1alpha1.ReasonProxmoxError, condition(t, obj, v1alpha1.ConditionReady).Reason)
}

func TestAMissingDiskIsReportedNotForgotten(t *testing.T) {
	adopted := adopting("pvc-abc", func(o *v1alpha1.ProxmoxVolume) {
		o.Finalizers = []string{v1alpha1.VolumeFinalizer}
		o.Status.VolumeID = handle
		o.Status.Adopted = true
	})

	r, c := newReconciler(t, onLocalLVM(), admittedTenant(), adopted)

	result := reconcile(t, r, "pvc-abc")
	assert.Positive(t, result.RequeueAfter, "a disk that is not there is rechecked, since it may come back")

	obj := get(t, c, "pvc-abc")

	assert.Equal(t, handle, obj.Status.VolumeID, "the handle survives, so a returning disk is re-confirmed")
	assert.Equal(t, v1alpha1.VolumePhaseFailed, obj.Status.Phase)
	assert.Equal(t, v1alpha1.ReasonAdoptDiskNotFound, condition(t, obj, v1alpha1.ConditionReady).Reason)
	assert.True(t, controllerutil.ContainsFinalizer(obj, v1alpha1.VolumeFinalizer), "and it is still held")
}

func TestAProvisioningRequestInObserveModeIsRejected(t *testing.T) {
	// An Observe-mode tenant cannot provision. The request is perfectly valid,
	// but Observe mode is read-only and provisioning requires Enforce.
	request := adopting("pvc-new", func(o *v1alpha1.ProxmoxVolume) {
		o.Spec.Adopt = false
		o.Spec.AdoptVolumeID = ""
	})

	disks := onLocalLVM("vm-9999-pvc-abc")
	r, c := newReconciler(t, disks, admittedTenant(), request)

	reconcile(t, r, "pvc-new")

	obj := get(t, c, "pvc-new")

	assert.Equal(t, v1alpha1.VolumePhaseRejected, obj.Status.Phase)
	assert.Equal(t, v1alpha1.ReasonTenantObserveOnly, condition(t, obj, v1alpha1.ConditionReady).Reason)
	assert.Equal(t, v1alpha1.ReasonTenantObserveOnly, condition(t, obj, v1alpha1.ConditionAdmitted).Reason)
	assert.Zero(t, disks.calls)
}

func TestDeletingAnAdoptedVolumeReleasesItAndLeavesTheDisk(t *testing.T) {
	deleting := adopting("pvc-abc", func(o *v1alpha1.ProxmoxVolume) {
		o.Finalizers = []string{v1alpha1.VolumeFinalizer}
		o.DeletionTimestamp = &metav1.Time{Time: at}
		o.Status.VolumeID = handle
		o.Status.Adopted = true
	})

	disks := onLocalLVM("vm-9999-pvc-abc")
	r, c := newReconciler(t, disks, admittedTenant(), deleting)

	reconcile(t, r, "pvc-abc")

	// The finalizer was the last one, so releasing it lets the object go.
	err := c.Get(t.Context(), request("pvc-abc").NamespacedName, &v1alpha1.ProxmoxVolume{})
	assert.True(t, apierrors.IsNotFound(err), "the record is released")
	assert.Zero(t, disks.calls, "and the disk is left exactly as it was")
}

func TestDeletingWhileDecommissioningIsBlocked(t *testing.T) {
	// In Decommission mode the record's disappearance is supposed to mean the
	// disk was destroyed. This operator cannot destroy one, so it blocks and
	// says so rather than reporting a reclaim that did not happen.
	deleting := adopting("pvc-abc", func(o *v1alpha1.ProxmoxVolume) {
		o.Finalizers = []string{v1alpha1.VolumeFinalizer}
		o.DeletionTimestamp = &metav1.Time{Time: at}
		o.Status.VolumeID = handle
		o.Status.Adopted = true
	})

	r, c := newReconciler(t, onLocalLVM("vm-9999-pvc-abc"),
		admittedTenant(func(o *v1alpha1.TenantCluster) { o.Spec.Mode = v1alpha1.TenantModeDecommission }),
		deleting)

	reconcile(t, r, "pvc-abc")

	obj := get(t, c, "pvc-abc")

	assert.True(t, controllerutil.ContainsFinalizer(obj, v1alpha1.VolumeFinalizer))
	assert.Equal(t, v1alpha1.VolumePhaseDeleting, obj.Status.Phase)
}

func TestTheClaimIndexCoversRequestsAsWellAsGrants(t *testing.T) {
	// The race the whole check exists for. Two adoptions of one disk, seconds
	// apart, would both read an index containing neither of them if only granted
	// handles were indexed -- the second one's read racing the first one's status
	// write through a cache.
	assert.Equal(t, []string{handle}, volume.IndexClaim(adopting("pvc-abc")),
		"a request is claimed from creation, before any status exists")

	granted := adopting("pvc-abc", func(o *v1alpha1.ProxmoxVolume) {
		o.Spec.AdoptVolumeID = ""
		o.Status.VolumeID = handle
	})
	assert.Equal(t, []string{handle}, volume.IndexClaim(granted))

	assert.Nil(t, volume.IndexClaim(adopting("pvc-abc", func(o *v1alpha1.ProxmoxVolume) {
		o.Spec.Adopt = false
		o.Spec.AdoptVolumeID = ""
	})), "a volume claiming nothing collides with nothing")
}
