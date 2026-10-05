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

package attachment_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	csipkg "github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	ptrpkg "github.com/sergelogvinov/proxmox-csi-plugin/pkg/helpers/ptr"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/controller/attachment"
	proxmox "github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"
	pvevolume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	tenantName      = "test-tenant"
	tenantNamespace = "tenant-ns"
	volumeID        = "bne/pve-1/local/vm-9997-pvc-abc"
)

var at = time.Date(2026, time.October, 5, 12, 0, 0, 0, time.UTC)

// fakeWriter records write calls and can be told to fail.
type fakeWriter struct {
	attaches  []attachCall
	detaches  []detachCall
	renames   []renameCall
	clears    []clearCall
	attachErr error
	detachErr error
	renameErr error
}

type attachCall struct {
	region  string
	vmid    int
	disk    string
	options map[string]string
}

type detachCall struct {
	region string
	vmid   int
	disk   string
}

type renameCall struct {
	region     string
	disk       string
	targetVMID int
}

type clearCall struct {
	region string
	vmid   int
}

func (f *fakeWriter) CreateDisk(_ context.Context, _, _, _, _ string, _ int64) error { return nil }
func (f *fakeWriter) CopyDisk(_ context.Context, _, _, _, _, _, _, _ string) error   { return nil }
func (f *fakeWriter) DeleteDisk(_ context.Context, _, _, _, _ string) error          { return nil }

func (f *fakeWriter) ResizeDisk(_ context.Context, _ string, _ int, _, _, _ string) error {
	return nil
}

func (f *fakeWriter) UpdateDisk(_ context.Context, _ string, _ int, _ *pvevolume.Volume, _ map[string]string) error {
	return nil
}

func (f *fakeWriter) AttachDisk(_ context.Context, region string, vmid int, vol *pvevolume.Volume, options map[string]string) (proxmox.AttachResult, error) {
	f.attaches = append(f.attaches, attachCall{region, vmid, vol.Disk(), options})

	if f.attachErr != nil {
		return proxmox.AttachResult{}, f.attachErr
	}

	return proxmox.AttachResult{
		DevicePath: "/dev/disk/by-id/wwn-0x5056432d494430310a",
		Lun:        1,
	}, nil
}

func (f *fakeWriter) DetachDisk(_ context.Context, region string, vmid int, vol *pvevolume.Volume) error {
	f.detaches = append(f.detaches, detachCall{region, vmid, vol.Disk()})

	return f.detachErr
}

func (f *fakeWriter) RenameDisk(_ context.Context, region string, vol *pvevolume.Volume, targetVMID int) (*pvevolume.Volume, error) {
	f.renames = append(f.renames, renameCall{region, vol.Disk(), targetVMID})

	if f.renameErr != nil {
		return nil, f.renameErr
	}

	newName := fmt.Sprintf("vm-%d-%s", targetVMID, vol.DiskSuffix())
	renamed := pvevolume.NewVolume(vol.Region(), vol.Zone(), vol.Storage(), newName)

	return renamed, nil
}

func (f *fakeWriter) ClearUnusedDisk(_ context.Context, region string, vmid int, _ *pvevolume.Volume) error {
	f.clears = append(f.clears, clearCall{region, vmid})

	return nil
}

func enforceTenant(mutators ...func(*v1alpha1.TenantCluster)) *v1alpha1.TenantCluster {
	obj := &v1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: tenantName},
		Spec: v1alpha1.TenantClusterSpec{
			Namespace:       tenantNamespace,
			Region:          "bne",
			Subject:         "pvx:test-client",
			PlaceholderVMID: 9997,
			AllowedStorages: []string{"local"},
			Mode:            v1alpha1.TenantModeEnforce,
		},
		Status: v1alpha1.TenantClusterStatus{
			ResolvedVMIDs: []int32{411, 421, 422, 423},
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

func readyVolume() *v1alpha1.ProxmoxVolume {
	return &v1alpha1.ProxmoxVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-abc",
			Namespace: tenantNamespace,
		},
		Spec: v1alpha1.ProxmoxVolumeSpec{
			Region:        "bne",
			Zone:          "pve-1",
			Storage:       "local",
			CapacityBytes: 1073741824,
		},
		Status: v1alpha1.ProxmoxVolumeStatus{
			VolumeID:      volumeID,
			DiskName:      "vm-9997-pvc-abc",
			OwnerVMID:     9997,
			CapacityBytes: 1073741824,
			Phase:         v1alpha1.VolumePhaseReady,
		},
	}
}

func attachmentRequest(vmid int32) *v1alpha1.ProxmoxVolumeAttachment {
	return &v1alpha1.ProxmoxVolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pva-test",
			Namespace: tenantNamespace,
		},
		Spec: v1alpha1.ProxmoxVolumeAttachmentSpec{
			VolumeName: "pvc-abc",
			VolumeID:   volumeID,
			NodeID:     "syd1-pub1-n1/" + strconv.Itoa(int(vmid)),
			VMID:       vmid,
		},
	}
}

func newReconciler(t *testing.T, writer *fakeWriter, reassign bool, objects ...client.Object) (*attachment.Reconciler, client.Client) {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(
			&v1alpha1.ProxmoxVolumeAttachment{},
			&v1alpha1.ProxmoxVolume{},
			&v1alpha1.TenantCluster{},
		).
		WithObjects(objects...).
		Build()

	return &attachment.Reconciler{
		Client:                 c,
		Writer:                 writer,
		VMLock:                 proxmox.NewVMLock(),
		ReassignVolumeOnAttach: reassign,
		Now:                    func() time.Time { return at },
	}, c
}

const testAttachmentName = "pva-test"

func reconcileAttachment(t *testing.T, r *attachment.Reconciler) ctrl.Result {
	t.Helper()

	result, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: tenantNamespace, Name: testAttachmentName},
	})
	require.NoError(t, err)

	return result
}

func getAttachment(t *testing.T, c client.Client) *v1alpha1.ProxmoxVolumeAttachment {
	t.Helper()

	obj := &v1alpha1.ProxmoxVolumeAttachment{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: tenantNamespace, Name: testAttachmentName}, obj))

	return obj
}

// --- Attach tests ---

func TestAttachSucceeds(t *testing.T) {
	w := &fakeWriter{}
	r, c := newReconciler(t, w, false,
		enforceTenant(), readyVolume(), attachmentRequest(411))

	result := reconcileAttachment(t, r)
	assert.Positive(t, result.RequeueAfter)

	att := getAttachment(t, c)
	assert.True(t, att.Status.Attached)
	assert.Equal(t, "/dev/disk/by-id/wwn-0x5056432d494430310a", att.Status.DevicePath)
	assert.Equal(t, "1", att.Status.LUN)
	assert.NotNil(t, att.Status.AttachedAt)

	require.Len(t, w.attaches, 1)
	assert.Equal(t, "bne", w.attaches[0].region)
	assert.Equal(t, 411, w.attaches[0].vmid)
	assert.Empty(t, w.renames, "no rename without reassignVolumeOnAttach")
}

func TestAttachWithReassign(t *testing.T) {
	w := &fakeWriter{}
	r, c := newReconciler(t, w, true,
		enforceTenant(), readyVolume(), attachmentRequest(411))

	result := reconcileAttachment(t, r)
	assert.Positive(t, result.RequeueAfter)

	att := getAttachment(t, c)
	assert.True(t, att.Status.Attached)

	require.Len(t, w.renames, 1, "should rename before attach")
	assert.Equal(t, 411, w.renames[0].targetVMID)
	require.Len(t, w.attaches, 1)
}

func TestAttachRefusedVMIDNotOwned(t *testing.T) {
	w := &fakeWriter{}
	att := attachmentRequest(999) // Not in resolvedVmids
	r, c := newReconciler(t, w, false,
		enforceTenant(), readyVolume(), att)

	reconcileAttachment(t, r)

	obj := getAttachment(t, c)
	cond := apimeta.FindStatusCondition(obj.Status.Conditions, v1alpha1.ConditionAdmitted)
	require.NotNil(t, cond)
	assert.Equal(t, v1alpha1.ReasonVMIDNotOwned, cond.Reason)
	assert.False(t, obj.Status.Attached)
	assert.Empty(t, w.attaches)
}

func TestAttachRefusedVolumeIDMismatch(t *testing.T) {
	w := &fakeWriter{}
	att := attachmentRequest(411)
	att.Spec.VolumeID = "wrong/volume/id/handle"

	r, c := newReconciler(t, w, false,
		enforceTenant(), readyVolume(), att)

	reconcileAttachment(t, r)

	obj := getAttachment(t, c)
	cond := apimeta.FindStatusCondition(obj.Status.Conditions, v1alpha1.ConditionAdmitted)
	require.NotNil(t, cond)
	assert.Equal(t, v1alpha1.ReasonVolumeIDMismatch, cond.Reason)
	assert.Empty(t, w.attaches)
}

func TestAttachRefusedObserveMode(t *testing.T) {
	w := &fakeWriter{}
	tenant := enforceTenant(func(tc *v1alpha1.TenantCluster) {
		tc.Spec.Mode = v1alpha1.TenantModeObserve
	})

	r, c := newReconciler(t, w, false,
		tenant, readyVolume(), attachmentRequest(411))

	reconcileAttachment(t, r)

	obj := getAttachment(t, c)
	cond := apimeta.FindStatusCondition(obj.Status.Conditions, v1alpha1.ConditionAdmitted)
	require.NotNil(t, cond)
	assert.Equal(t, v1alpha1.ReasonTenantObserveOnly, cond.Reason)
	assert.Empty(t, w.attaches)
}

// --- Detach tests ---

func TestDetachSucceeds(t *testing.T) {
	w := &fakeWriter{}
	att := attachmentRequest(411)
	att.Finalizers = []string{v1alpha1.AttachmentFinalizer}
	att.DeletionTimestamp = &metav1.Time{Time: at}

	r, _ := newReconciler(t, w, false,
		enforceTenant(), readyVolume(), att)

	reconcileAttachment(t, r)

	// After removing the finalizer, the fake client deletes the object
	// (DeletionTimestamp is set and no finalizers remain). This is correct.

	require.Len(t, w.detaches, 1)
	assert.Equal(t, 411, w.detaches[0].vmid)
	assert.Empty(t, w.renames, "no rename-back without reassignVolumeOnAttach")
}

func TestDetachWithRenameBack(t *testing.T) {
	w := &fakeWriter{}
	att := attachmentRequest(411)
	att.Finalizers = []string{v1alpha1.AttachmentFinalizer}
	att.DeletionTimestamp = &metav1.Time{Time: at}

	// Volume is currently owned by VMID 411 (attached).
	vol := readyVolume()
	vol.Status.DiskName = "vm-411-pvc-abc"
	vol.Status.OwnerVMID = 411

	r, _ := newReconciler(t, w, true,
		enforceTenant(), vol, att)

	reconcileAttachment(t, r)

	require.Len(t, w.detaches, 1)
	// Rename back to placeholder VMID 9997 after detach.
	require.Len(t, w.renames, 1)
	assert.Equal(t, 9997, w.renames[0].targetVMID)
	// Clear the unused entry after the rename.
	require.Len(t, w.clears, 1)
	assert.Equal(t, 411, w.clears[0].vmid)
}

func TestDetachVolumeGone(t *testing.T) {
	w := &fakeWriter{}
	att := attachmentRequest(411)
	att.Finalizers = []string{v1alpha1.AttachmentFinalizer}
	att.DeletionTimestamp = &metav1.Time{Time: at}

	// No volume object -- just tenant and attachment.
	r, _ := newReconciler(t, w, false,
		enforceTenant(), att)

	reconcileAttachment(t, r)

	// Attachment should be gone (finalizer removed + DeletionTimestamp set).
	assert.Empty(t, w.detaches, "no detach call when volume object is missing")
}

// --- Golden tests: the operator produces the same disk options as direct mode ---

// TestAttachDiskOptionsMatchDirectMode is the golden test. For each StorageClass
// parameter set (including syd1's production values), the options the operator
// passes to Writer.AttachDisk must equal the cfg map that direct mode's
// ControllerPublishVolume passes to attachVolume via ExtractParameters → ToCFG.
func TestAttachDiskOptionsMatchDirectMode(t *testing.T) {
	tests := []struct {
		name       string
		parameters map[string]string
		readonly   bool
		// wantCFG is what ExtractParameters(parameters).ToCFG() produces -- the
		// ground truth from pkg/csi.
		wantCFG map[string]string
	}{
		{
			name: "syd1 production StorageClass",
			parameters: map[string]string{
				"storageFormat": "raw",
				"ssd":           "true",
				"backup":        "true",
				"cache":         "none",
				"aio":           "native",
				"diskMBps":      "150",
				"diskIOPS":      "3000",
			},
			wantCFG: map[string]string{
				"aio":       "native",
				"backup":    "1",
				"cache":     "none",
				"discard":   "on",
				"iothread":  "1",
				"iops_rd":   "3000",
				"iops_wr":   "3000",
				"mbps_rd":   "150",
				"mbps_wr":   "150",
				"replicate": "0",
				"ssd":       "1",
			},
		},
		{
			name: "minimal StorageClass (no optional params)",
			parameters: map[string]string{
				"storageFormat": "raw",
			},
			wantCFG: map[string]string{
				"backup":    "0",
				"iothread":  "1",
				"replicate": "0",
			},
		},
		{
			name: "explicit discard without ssd",
			parameters: map[string]string{
				"discard": "on",
			},
			wantCFG: map[string]string{
				"backup":    "0",
				"discard":   "on",
				"iothread":  "1",
				"replicate": "0",
			},
		},
		{
			name: "readonly attach",
			parameters: map[string]string{
				"cache": "writeback",
			},
			readonly: true,
			wantCFG: map[string]string{
				"backup":    "0",
				"cache":     "writeback",
				"iothread":  "1",
				"replicate": "0",
				"ro":        "1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build the cfg map the same way direct mode does.
			directParams, err := csipkg.ExtractParameters(tt.parameters)
			require.NoError(t, err)

			if tt.readonly {
				directParams.ReadOnly = ptrpkg.Ptr(true)
			}

			directCFG := directParams.ToCFG()
			assert.Equal(t, tt.wantCFG, directCFG, "wantCFG should match ExtractParameters().ToCFG()")

			// Now run the full reconciler and capture what it passes to AttachDisk.
			w := &fakeWriter{}
			vol := readyVolume()
			vol.Spec.Parameters = tt.parameters
			att := attachmentRequest(411)
			att.Spec.Readonly = tt.readonly

			r, _ := newReconciler(t, w, false,
				enforceTenant(), vol, att)

			reconcileAttachment(t, r)

			require.Len(t, w.attaches, 1)
			assert.Equal(t, directCFG, w.attaches[0].options,
				"operator attach options must match direct mode's ExtractParameters().ToCFG()")
		})
	}
}

// TestAttachDiskOptionsWithMutableParametersMerge verifies that
// vol.Spec.MutableParameters (from a VolumeAttributesClass) are merged over the
// base StorageClass options, matching direct mode's paramsVAC.MergeMap(cfg).
func TestAttachDiskOptionsWithMutableParametersMerge(t *testing.T) {
	w := &fakeWriter{}
	vol := readyVolume()
	vol.Spec.Parameters = map[string]string{
		"cache":    "none",
		"aio":      "native",
		"diskMBps": "150",
		"diskIOPS": "3000",
	}
	vol.Spec.MutableParameters = map[string]string{
		"diskMBps": "200",
		"diskIOPS": "5000",
	}

	att := attachmentRequest(411)

	r, _ := newReconciler(t, w, false,
		enforceTenant(), vol, att)

	reconcileAttachment(t, r)

	require.Len(t, w.attaches, 1)

	opts := w.attaches[0].options

	// The mutable parameters should override the base ones.
	assert.Equal(t, "200", opts["mbps_rd"], "diskMBps should be overridden by mutable params")
	assert.Equal(t, "200", opts["mbps_wr"])
	assert.Equal(t, "5000", opts["iops_rd"], "diskIOPS should be overridden by mutable params")
	assert.Equal(t, "5000", opts["iops_wr"])

	// The base parameters that are NOT in mutable should survive.
	assert.Equal(t, "none", opts["cache"])
	assert.Equal(t, "native", opts["aio"])
}

// TestAttachRefusedInvalidParameters verifies that invalid parameters in the
// volume spec produce a condition and no attach, rather than a crash.
func TestAttachRefusedInvalidParameters(t *testing.T) {
	w := &fakeWriter{}
	vol := readyVolume()
	vol.Spec.Parameters = map[string]string{
		"diskIOPS": "not-a-number",
	}

	att := attachmentRequest(411)

	r, c := newReconciler(t, w, false,
		enforceTenant(), vol, att)

	reconcileAttachment(t, r)

	obj := getAttachment(t, c)
	cond := apimeta.FindStatusCondition(obj.Status.Conditions, v1alpha1.ConditionReady)
	require.NotNil(t, cond)
	assert.Equal(t, v1alpha1.ReasonParameterRejected, cond.Reason)
	assert.False(t, obj.Status.Attached)
	assert.Empty(t, w.attaches, "no attach when parameters are invalid")
}
