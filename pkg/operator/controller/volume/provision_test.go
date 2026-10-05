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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	csipkg "github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/controller/volume"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"
	pvevolume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// fakeWriter records all write operations and can be told to fail.
type fakeWriter struct {
	creates []createCall
	deletes []deleteCall
	copies  []copyCall
	resizes []resizeCall
	updates []updateCall
	err     error

	resizeErr error
	updateErr error
}

type createCall struct {
	region, zone, storage, diskName string
	sizeBytes                       int64
}

type deleteCall struct {
	region, zone, storage, disk string
}

type copyCall struct {
	srcRegion, srcZone, srcStorage, srcDisk string
	dstZone, dstStorage, dstDisk            string
}

type resizeCall struct {
	region string
	vmid   int
	zone   string
	device string
	size   string
}

type updateCall struct {
	region  string
	vmid    int
	disk    string
	options map[string]string
}

func (f *fakeWriter) CreateDisk(_ context.Context, region, zone, storage, diskName string, sizeBytes int64) error {
	f.creates = append(f.creates, createCall{region, zone, storage, diskName, sizeBytes})

	return f.err
}

func (f *fakeWriter) CopyDisk(_ context.Context, srcRegion, srcZone, srcStorage, srcDisk, dstZone, dstStorage, dstDisk string) error {
	f.copies = append(f.copies, copyCall{srcRegion, srcZone, srcStorage, srcDisk, dstZone, dstStorage, dstDisk})

	return f.err
}

func (f *fakeWriter) DeleteDisk(_ context.Context, region, zone, storage, disk string) error {
	f.deletes = append(f.deletes, deleteCall{region, zone, storage, disk})

	return f.err
}

func (f *fakeWriter) AttachDisk(_ context.Context, _ string, _ int, _ *pvevolume.Volume, _ map[string]string) (proxmox.AttachResult, error) {
	return proxmox.AttachResult{}, nil
}

func (f *fakeWriter) DetachDisk(_ context.Context, _ string, _ int, _ *pvevolume.Volume) error {
	return nil
}

func (f *fakeWriter) ResizeDisk(_ context.Context, region string, vmid int, zone, device, size string) error {
	f.resizes = append(f.resizes, resizeCall{region, vmid, zone, device, size})

	return f.resizeErr
}

func (f *fakeWriter) UpdateDisk(_ context.Context, region string, vmid int, vol *pvevolume.Volume, options map[string]string) error {
	f.updates = append(f.updates, updateCall{region, vmid, vol.Disk(), options})

	return f.updateErr
}

func (f *fakeWriter) RenameDisk(_ context.Context, _ string, _ *pvevolume.Volume, _ int) (*pvevolume.Volume, error) {
	return nil, nil
}

func (f *fakeWriter) ClearUnusedDisk(_ context.Context, _ string, _ int, _ *pvevolume.Volume) error {
	return nil
}

// readyProvisionedVolume builds a Ready provisioned volume (not adopted) with
// an active attachment, for expand/modify tests.
func readyProvisionedVolume(name string, mutators ...func(*v1alpha1.ProxmoxVolume)) *v1alpha1.ProxmoxVolume {
	obj := &v1alpha1.ProxmoxVolume{
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
			Adopted:       false,
		},
	}

	for _, mutate := range mutators {
		mutate(obj)
	}

	return obj
}

func attachedVolume(volumeName string, vmid int32) *v1alpha1.ProxmoxVolumeAttachment {
	return &v1alpha1.ProxmoxVolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pva-" + volumeName,
			Namespace: tenantNamespace,
		},
		Spec: v1alpha1.ProxmoxVolumeAttachmentSpec{
			VolumeName: volumeName,
			VolumeID:   "bne/pve-1/local-lvm/vm-9991-" + volumeName,
			NodeID:     "node1/" + strconv.Itoa(int(vmid)),
			VMID:       vmid,
		},
		Status: v1alpha1.ProxmoxVolumeAttachmentStatus{
			Attached: true,
			LUN:      "2",
		},
	}
}

func enforceTenant(mutators ...func(*v1alpha1.TenantCluster)) *v1alpha1.TenantCluster {
	return admittedTenant(append([]func(*v1alpha1.TenantCluster){
		func(t *v1alpha1.TenantCluster) {
			t.Spec.Mode = v1alpha1.TenantModeEnforce
		},
	}, mutators...)...)
}

func provisionRequest(name string, mutators ...func(*v1alpha1.ProxmoxVolume)) *v1alpha1.ProxmoxVolume {
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
			CapacityBytes: 1 * gib,
		},
	}

	for _, mutate := range mutators {
		mutate(obj)
	}

	return obj
}

// storageObject returns a ProxmoxStorage catalog entry for the given storage.
func storageObject(storage, pluginType string) *v1alpha1.ProxmoxStorage {
	return &v1alpha1.ProxmoxStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "bne-pve1-" + storage},
		Spec: v1alpha1.ProxmoxStorageSpec{
			Region:     "bne",
			Zone:       "pve-1",
			Storage:    storage,
			PluginType: pluginType,
		},
		Status: v1alpha1.ProxmoxStorageStatus{Active: true},
	}
}

func newProvisionReconciler(t *testing.T, disks *fakeDisks, writer *fakeWriter, objects ...client.Object) (*volume.Reconciler, client.Client) {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.ProxmoxVolume{}, &v1alpha1.TenantCluster{}, &v1alpha1.ProxmoxVolumeAttachment{}, &v1alpha1.ProxmoxVolumeSnapshot{}).
		WithIndex(&v1alpha1.ProxmoxVolume{}, volume.ClaimIndex, volume.IndexClaim).
		WithObjects(objects...).
		Build()

	return &volume.Reconciler{
		Client:  c,
		Proxmox: disks,
		Writer:  writer,
		Now:     func() time.Time { return at },
	}, c
}

// --- Refusal tests ---

func TestProvisionRefusedTenantSuspended(t *testing.T) {
	req := provisionRequest("pvc-new")
	tenant := enforceTenant(func(tc *v1alpha1.TenantCluster) {
		tc.Spec.Mode = v1alpha1.TenantModeSuspended
	})

	disks := onLocalLVM()
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w, tenant, req)

	reconcile(t, r, "pvc-new")

	obj := get(t, c, "pvc-new")
	assert.Equal(t, v1alpha1.VolumePhaseRejected, obj.Status.Phase)
	assert.Equal(t, v1alpha1.ReasonTenantSuspended, condition(t, obj, v1alpha1.ConditionAdmitted).Reason)
	assert.Empty(t, w.creates)
}

func TestProvisionRefusedRegionMismatch(t *testing.T) {
	req := provisionRequest("pvc-new", func(v *v1alpha1.ProxmoxVolume) {
		v.Spec.Region = "syd" // Tenant is in bne.
	})

	disks := onLocalLVM()
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w, enforceTenant(), req)

	reconcile(t, r, "pvc-new")

	obj := get(t, c, "pvc-new")
	assert.Equal(t, v1alpha1.VolumePhaseRejected, obj.Status.Phase)
	assert.Equal(t, v1alpha1.ReasonInvalidSpec, condition(t, obj, v1alpha1.ConditionAdmitted).Reason)
	assert.Empty(t, w.creates)
}

func TestProvisionRefusedStorageNotAllowed(t *testing.T) {
	req := provisionRequest("pvc-new", func(v *v1alpha1.ProxmoxVolume) {
		v.Spec.Storage = "ceph" // Not in allowedStorages.
	})

	disks := onLocalLVM()
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w, enforceTenant(), req)

	reconcile(t, r, "pvc-new")

	obj := get(t, c, "pvc-new")
	assert.Equal(t, v1alpha1.VolumePhaseRejected, obj.Status.Phase)
	assert.Equal(t, v1alpha1.ReasonStorageNotAllowed, condition(t, obj, v1alpha1.ConditionAdmitted).Reason)
	assert.Empty(t, w.creates)
}

func TestProvisionRefusedParameterPolicy(t *testing.T) {
	req := provisionRequest("pvc-new", func(v *v1alpha1.ProxmoxVolume) {
		v.Spec.Parameters = map[string]string{"forbidden": "value"}
	})

	tenant := enforceTenant(func(tc *v1alpha1.TenantCluster) {
		tc.Spec.ParameterPolicy = &v1alpha1.ParameterPolicy{
			Allowed: []string{"cache", "ssd"},
		}
	})

	disks := onLocalLVM()
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w, tenant, req)

	reconcile(t, r, "pvc-new")

	obj := get(t, c, "pvc-new")
	assert.Equal(t, v1alpha1.VolumePhaseRejected, obj.Status.Phase)
	assert.Equal(t, v1alpha1.ReasonParameterRejected, condition(t, obj, v1alpha1.ConditionAdmitted).Reason)
	assert.Empty(t, w.creates)
}

func TestProvisionRefusedQuotaExceeded(t *testing.T) {
	req := provisionRequest("pvc-new")
	maxVol := int32(0)

	tenant := enforceTenant(func(tc *v1alpha1.TenantCluster) {
		tc.Spec.Quota = &v1alpha1.TenantQuota{MaxVolumes: &maxVol}
	})

	disks := onLocalLVM()
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w, tenant, req)

	reconcile(t, r, "pvc-new")

	obj := get(t, c, "pvc-new")
	assert.Equal(t, v1alpha1.VolumePhaseRejected, obj.Status.Phase)
	assert.Equal(t, v1alpha1.ReasonQuotaExceeded, condition(t, obj, v1alpha1.ConditionAdmitted).Reason)
	assert.Empty(t, w.creates)
}

func TestProvisionRefusedNamespaceQuotaNoClaimRef(t *testing.T) {
	req := provisionRequest("pvc-new") // No claimRef.

	tenant := enforceTenant(func(tc *v1alpha1.TenantCluster) {
		maxVol := int32(10)
		tc.Spec.NamespaceQuotas = []v1alpha1.NamespaceQuota{
			{Namespace: "default", MaxVolumes: &maxVol},
		}
	})

	disks := onLocalLVM()
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w, tenant, req)

	reconcile(t, r, "pvc-new")

	obj := get(t, c, "pvc-new")
	assert.Equal(t, v1alpha1.VolumePhaseRejected, obj.Status.Phase)
	assert.Equal(t, v1alpha1.ReasonMissingClaimRef, condition(t, obj, v1alpha1.ConditionAdmitted).Reason)
	assert.Empty(t, w.creates)
}

func TestProvisionRefusedMaxVolumeBytes(t *testing.T) {
	req := provisionRequest("pvc-new", func(v *v1alpha1.ProxmoxVolume) {
		v.Spec.CapacityBytes = 100 * gib
	})

	maxBytes := resource.MustParse("10Gi")

	tenant := enforceTenant(func(tc *v1alpha1.TenantCluster) {
		tc.Spec.ParameterPolicy = &v1alpha1.ParameterPolicy{
			MaxVolumeBytes: &maxBytes,
		}
	})

	disks := onLocalLVM()
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w, tenant, req)

	reconcile(t, r, "pvc-new")

	obj := get(t, c, "pvc-new")
	assert.Equal(t, v1alpha1.VolumePhaseRejected, obj.Status.Phase)
	assert.Equal(t, v1alpha1.ReasonParameterRejected, condition(t, obj, v1alpha1.ConditionAdmitted).Reason)
	assert.Empty(t, w.creates)
}

// --- Happy path ---

func TestProvisionHappyPathCreatesDiskOnce(t *testing.T) {
	req := provisionRequest("pvc-new")

	disks := onLocalLVM() // No existing disks.
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w, enforceTenant(), req, storageObject("local-lvm", "lvmthin"))

	// First reconcile: creates the disk.
	reconcile(t, r, "pvc-new")

	obj := get(t, c, "pvc-new")
	assert.Equal(t, v1alpha1.VolumePhaseReady, obj.Status.Phase)
	assert.Equal(t, "bne/pve-1/local-lvm/vm-9991-pvc-new", obj.Status.VolumeID)
	assert.Equal(t, int32(9991), obj.Status.OwnerVMID)
	assert.False(t, obj.Status.Adopted)
	assert.Contains(t, obj.Status.AccessibleTopology, "pve-1")
	require.Len(t, w.creates, 1)
	assert.Equal(t, "vm-9991-pvc-new", w.creates[0].diskName)

	// Second reconcile: idempotent, disk already exists in ListDisks.
	// Add the disk to the fake so ListDisks finds it.
	disks.disks["bne/local-lvm"] = append(disks.disks["bne/local-lvm"], proxmox.Disk{
		Region: "bne", Node: "pve-1", Storage: "local-lvm",
		Name: "vm-9991-pvc-new", VMID: 9991, SizeBytes: 1 * gib,
	})

	reconcile(t, r, "pvc-new")

	// No additional create calls.
	assert.Len(t, w.creates, 1)
}

// --- Delete tests ---

func TestDeleteProvisionedVolumeDeletesDisk(t *testing.T) {
	deleting := provisionRequest("pvc-del", func(v *v1alpha1.ProxmoxVolume) {
		v.Finalizers = []string{v1alpha1.VolumeFinalizer}
		v.DeletionTimestamp = &metav1.Time{Time: at}
		v.Status.VolumeID = "bne/pve-1/local-lvm/vm-9991-pvc-del"
		v.Status.DiskName = "vm-9991-pvc-del"
		v.Status.Adopted = false
	})

	disks := onLocalLVM()
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w, enforceTenant(), deleting)

	reconcile(t, r, "pvc-del")

	// The disk was deleted.
	require.Len(t, w.deletes, 1)
	assert.Equal(t, "vm-9991-pvc-del", w.deletes[0].disk)

	// The object is gone.
	err := c.Get(t.Context(), request("pvc-del").NamespacedName, &v1alpha1.ProxmoxVolume{})
	assert.True(t, apierrors.IsNotFound(err))
}

func TestDeleteAdoptedVolumeKeepsDisk(t *testing.T) {
	deleting := adopting("pvc-adopted", func(v *v1alpha1.ProxmoxVolume) {
		v.Finalizers = []string{v1alpha1.VolumeFinalizer}
		v.DeletionTimestamp = &metav1.Time{Time: at}
		v.Status.VolumeID = handle
		v.Status.Adopted = true
	})

	disks := onLocalLVM("vm-9999-pvc-adopted")
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w, admittedTenant(), deleting)

	reconcile(t, r, "pvc-adopted")

	// No disk delete for adopted volumes.
	assert.Empty(t, w.deletes)

	// The object is gone (finalizer removed).
	err := c.Get(t.Context(), request("pvc-adopted").NamespacedName, &v1alpha1.ProxmoxVolume{})
	assert.True(t, apierrors.IsNotFound(err))
}

func TestDeleteRefusedWhileAttachmentExists(t *testing.T) {
	deleting := provisionRequest("pvc-attached", func(v *v1alpha1.ProxmoxVolume) {
		v.Finalizers = []string{v1alpha1.VolumeFinalizer}
		v.DeletionTimestamp = &metav1.Time{Time: at}
		v.Status.VolumeID = "bne/pve-1/local-lvm/vm-9991-pvc-attached"
		v.Status.DiskName = "vm-9991-pvc-attached"
		v.Status.Adopted = false
	})

	attachment := &v1alpha1.ProxmoxVolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "att-1",
			Namespace: tenantNamespace,
		},
		Spec: v1alpha1.ProxmoxVolumeAttachmentSpec{
			VolumeName: "pvc-attached",
		},
	}

	disks := onLocalLVM()
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w, enforceTenant(), deleting, attachment)

	reconcile(t, r, "pvc-attached")

	// Disk NOT deleted while attachment exists.
	assert.Empty(t, w.deletes)

	// Object still exists, phase is Deleting.
	obj := get(t, c, "pvc-attached")
	assert.Equal(t, v1alpha1.VolumePhaseDeleting, obj.Status.Phase)
}

func TestProvisionRefusedNamespaceQuotaExceeded(t *testing.T) {
	maxVol := int32(0)
	req := provisionRequest("pvc-new", func(v *v1alpha1.ProxmoxVolume) {
		v.Spec.ClaimRef = v1alpha1.TenantClaimRef{
			Name:      "my-pvc",
			Namespace: "production",
			PVName:    "pvc-new",
		}
	})

	tenant := enforceTenant(func(tc *v1alpha1.TenantCluster) {
		tc.Spec.NamespaceQuotas = []v1alpha1.NamespaceQuota{
			{Namespace: "production", MaxVolumes: &maxVol},
		}
	})

	disks := onLocalLVM()
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w, tenant, req)

	reconcile(t, r, "pvc-new")

	obj := get(t, c, "pvc-new")
	assert.Equal(t, v1alpha1.VolumePhaseRejected, obj.Status.Phase)
	assert.Equal(t, v1alpha1.ReasonNamespaceQuota, condition(t, obj, v1alpha1.ConditionAdmitted).Reason)
	assert.Empty(t, w.creates)
}

func TestProvisionRefusedObserveMode(t *testing.T) {
	// Covers the Observe refusal via the provisioning path (same as reconciler_test.go
	// but through the provision reconciler with a writer).
	req := provisionRequest("pvc-obs")

	disks := onLocalLVM()
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w, admittedTenant(), req)

	reconcile(t, r, "pvc-obs")

	obj := get(t, c, "pvc-obs")
	assert.Equal(t, v1alpha1.VolumePhaseRejected, obj.Status.Phase)
	assert.Equal(t, v1alpha1.ReasonTenantObserveOnly, condition(t, obj, v1alpha1.ConditionAdmitted).Reason)
	assert.Empty(t, w.creates)
}

func TestProvisionIdempotentSecondReconcileNoCreate(t *testing.T) {
	// Verify the second reconcile does not issue a second create.
	req := provisionRequest("pvc-idem")

	disks := onLocalLVM()
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w, enforceTenant(), req, storageObject("local-lvm", "lvmthin"))

	// First reconcile: creates the disk.
	reconcile(t, r, "pvc-idem")

	obj := get(t, c, "pvc-idem")
	assert.Equal(t, v1alpha1.VolumePhaseReady, obj.Status.Phase)
	require.Len(t, w.creates, 1)

	// Add the disk to the storage so ListDisks finds it on the second reconcile.
	disks.disks["bne/local-lvm"] = append(disks.disks["bne/local-lvm"], proxmox.Disk{
		Region: "bne", Node: "pve-1", Storage: "local-lvm",
		Name: "vm-9991-pvc-idem", VMID: 9991, SizeBytes: 1 * gib,
	})

	// Second reconcile: idempotent.
	reconcile(t, r, "pvc-idem")

	// No additional create calls.
	assert.Len(t, w.creates, 1, "second reconcile must not issue another CreateDisk")
}

// TestProvisionNilParameterPolicyAllowsAllParameters confirms that a nil
// ParameterPolicy (the default) does not filter any StorageClass parameters.
// This is what syd1's production StorageClass relies on: cache, aio, ssd,
// backup, diskMBps, diskIOPS, storageFormat all reach vol.Spec.Parameters
// unfiltered.
func TestProvisionNilParameterPolicyAllowsAllParameters(t *testing.T) {
	syd1Params := map[string]string{
		"storageFormat": "raw",
		"ssd":           "true",
		"backup":        "true",
		"cache":         "none",
		"aio":           "native",
		"diskMBps":      "150",
		"diskIOPS":      "3000",
	}

	req := provisionRequest("pvc-syd1", func(v *v1alpha1.ProxmoxVolume) {
		v.Spec.Parameters = syd1Params
	})

	// enforceTenant() has no ParameterPolicy (nil).
	tenant := enforceTenant()

	disks := onLocalLVM()
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w, tenant, req, storageObject("local-lvm", "lvm"))

	reconcile(t, r, "pvc-syd1")

	obj := get(t, c, "pvc-syd1")
	assert.Equal(t, v1alpha1.VolumePhaseReady, obj.Status.Phase,
		"nil ParameterPolicy must not reject any parameters")

	// Every parameter from the StorageClass must survive in spec.parameters.
	for key, want := range syd1Params {
		assert.Equal(t, want, obj.Spec.Parameters[key],
			"spec.parameters[%s] must be preserved", key)
	}

	require.Len(t, w.creates, 1, "disk must be created")
}

// --- Modify tests ---

// TestModifyVolumeAttachedUpdatesGolden verifies that the options passed to
// Writer.UpdateDisk are exactly ExtractModifyVolumeParameters(m).ToCFG() for
// a syd1-like VolumeAttributesClass. This golden test ensures the operator's
// modify path produces the same Proxmox request body as direct mode.
func TestModifyVolumeAttachedUpdatesGolden(t *testing.T) {
	syd1VAC := map[string]string{"diskMBps": "200", "diskIOPS": "5000"}

	vol := readyProvisionedVolume("pvc-mod", func(v *v1alpha1.ProxmoxVolume) {
		v.Spec.MutableParameters = syd1VAC
	})

	att := attachedVolume("pvc-mod", 411)

	disks := onLocalLVM()
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w,
		enforceTenant(), vol, att, storageObject("local-lvm", "lvmthin"))

	reconcile(t, r, "pvc-mod")

	// The golden expectation: what direct mode's ControllerModifyVolume sends.
	golden, err := csipkg.ExtractModifyVolumeParameters(syd1VAC)
	require.NoError(t, err)

	goldenCFG := golden.ToCFG()

	require.Len(t, w.updates, 1, "UpdateDisk must be called once")
	assert.Equal(t, goldenCFG, w.updates[0].options,
		"options passed to UpdateDisk must match ExtractModifyVolumeParameters(m).ToCFG()")
	assert.Equal(t, 411, w.updates[0].vmid)
	assert.Equal(t, "bne", w.updates[0].region)

	// The applied-params annotation should be set.
	obj := get(t, c, "pvc-mod")
	assert.NotEmpty(t, obj.Annotations[volume.AppliedMutableParamsAnnotation])

	// Second reconcile: idempotent, no second UpdateDisk.
	reconcile(t, r, "pvc-mod")
	assert.Len(t, w.updates, 1, "second reconcile must not re-apply unchanged mutable parameters")
}

func TestModifyVolumeDetachedRecordsHash(t *testing.T) {
	syd1VAC := map[string]string{"diskMBps": "200", "diskIOPS": "5000"}

	vol := readyProvisionedVolume("pvc-detached", func(v *v1alpha1.ProxmoxVolume) {
		v.Spec.MutableParameters = syd1VAC
	})

	disks := onLocalLVM()
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w,
		enforceTenant(), vol, storageObject("local-lvm", "lvmthin"))

	reconcile(t, r, "pvc-detached")

	// No UpdateDisk call: volume is not attached.
	assert.Empty(t, w.updates, "detached volume must not call UpdateDisk")

	// But the applied-params hash is set so the API returns OK immediately.
	obj := get(t, c, "pvc-detached")
	assert.NotEmpty(t, obj.Annotations[volume.AppliedMutableParamsAnnotation])
}

// --- Expand tests ---

func TestExpandAttachedVolumeResizesDisk(t *testing.T) {
	vol := readyProvisionedVolume("pvc-exp", func(v *v1alpha1.ProxmoxVolume) {
		v.Spec.CapacityBytes = 2 * gib // Request expansion from 1Gi to 2Gi.
	})

	att := attachedVolume("pvc-exp", 411)

	disks := onLocalLVM()
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w,
		enforceTenant(), vol, att, storageObject("local-lvm", "lvmthin"))

	reconcile(t, r, "pvc-exp")

	require.Len(t, w.resizes, 1, "ResizeDisk must be called")
	assert.Equal(t, "bne", w.resizes[0].region)
	assert.Equal(t, 411, w.resizes[0].vmid)
	assert.Equal(t, "scsi2", w.resizes[0].device, "device must use the LUN from the attachment status")
	assert.Equal(t, "2048M", w.resizes[0].size, "size must be rounded up and formatted as <n>M")

	obj := get(t, c, "pvc-exp")
	assert.Equal(t, int64(2*gib), obj.Status.CapacityBytes)
	assert.Equal(t, v1alpha1.VolumePhaseReady, obj.Status.Phase)
}

func TestExpandDetachedVolumeRequeues(t *testing.T) {
	vol := readyProvisionedVolume("pvc-exp-detached", func(v *v1alpha1.ProxmoxVolume) {
		v.Spec.CapacityBytes = 2 * gib
	})

	disks := onLocalLVM()
	w := &fakeWriter{}
	r, c := newProvisionReconciler(t, disks, w,
		enforceTenant(), vol, storageObject("local-lvm", "lvmthin"))

	result := reconcile(t, r, "pvc-exp-detached")

	assert.Empty(t, w.resizes, "detached volume must not call ResizeDisk")
	assert.Positive(t, result.RequeueAfter, "should requeue waiting for attachment")

	obj := get(t, c, "pvc-exp-detached")
	assert.Equal(t, int64(1*gib), obj.Status.CapacityBytes, "capacityBytes must not change yet")
}
