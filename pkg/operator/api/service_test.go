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

package api_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	volumev1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/volume/v1"
	volumeapi "github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/api"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func serviceWithTenant(t *testing.T, objects ...client.Object) (*volumeapi.Service, client.Client) {
	t.Helper()

	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&v1alpha1.ProxmoxVolume{}, &v1alpha1.ProxmoxVolumeAttachment{}, &v1alpha1.ProxmoxVolumeSnapshot{}).
		WithIndex(&v1alpha1.ProxmoxVolume{}, volumeapi.VolumeIDIndex, volumeapi.IndexVolumeID).
		WithIndex(&v1alpha1.ProxmoxVolumeSnapshot{}, volumeapi.SnapshotIDIndex, volumeapi.IndexSnapshotID).
		WithObjects(objects...).
		Build()

	return volumeapi.NewService(c, nil), c
}

func tenantCtx() context.Context {
	tenant := &v1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-tenant"},
		Spec: v1alpha1.TenantClusterSpec{
			Namespace:       "tenant-ns",
			Region:          "bne",
			Subject:         "pvx:test-client",
			PlaceholderVMID: 9997,
			AllowedStorages: []string{"local"},
			Mode:            v1alpha1.TenantModeEnforce,
		},
	}

	return volumeapi.ContextWithTenantForTest(context.Background(), tenant)
}

func createReq() *volumev1.CreateVolumeRequest {
	return &volumev1.CreateVolumeRequest{
		Name:          "pvc-abc",
		PvName:        "pvc-abc",
		Region:        "bne",
		Zone:          "pve-1",
		Storage:       "local",
		CapacityBytes: 1073741824,
		ClaimRef: &volumev1.ClaimRef{
			Name:      "my-pvc",
			Namespace: "default",
		},
	}
}

func TestCreateVolumeCreatesCR(t *testing.T) {
	svc, c := serviceWithTenant(t)
	ctx := tenantCtx()

	// First call: CR does not exist. Should return Unavailable (provisioning).
	_, err := svc.CreateVolume(ctx, createReq())
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))

	// Verify the CR was created in the tenant's namespace.
	vol := &v1alpha1.ProxmoxVolume{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "tenant-ns", Name: "pvc-abc"}, vol))

	assert.Equal(t, "bne", vol.Spec.Region)
	assert.Equal(t, "pve-1", vol.Spec.Zone)
	assert.Equal(t, "local", vol.Spec.Storage)
	assert.Equal(t, int64(1073741824), vol.Spec.CapacityBytes)
	assert.Equal(t, "my-pvc", vol.Spec.ClaimRef.Name)
	assert.Equal(t, "default", vol.Spec.ClaimRef.Namespace)
	assert.Equal(t, "pvc-abc", vol.Spec.ClaimRef.PVName)

	// Check tenant label.
	assert.Equal(t, "test-tenant", vol.Labels[v1alpha1.LabelTenant])

	// Check request-id annotation.
	_, hasRequestID := vol.Annotations[v1alpha1.GroupName+"/request-id"]
	assert.True(t, hasRequestID, "request-id annotation must be set")
}

func TestCreateVolumeReturnsReadyVolume(t *testing.T) {
	existing := &v1alpha1.ProxmoxVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-ready",
			Namespace: "tenant-ns",
		},
		Spec: v1alpha1.ProxmoxVolumeSpec{
			Region:        "bne",
			Zone:          "pve-1",
			Storage:       "local",
			CapacityBytes: 1073741824,
		},
		Status: v1alpha1.ProxmoxVolumeStatus{
			VolumeID:           "bne/pve-1/local/vm-9997-pvc-ready",
			CapacityBytes:      1073741824,
			AccessibleTopology: []string{"pve-1"},
			Phase:              v1alpha1.VolumePhaseReady,
		},
	}

	svc, _ := serviceWithTenant(t, existing)
	ctx := tenantCtx()

	req := createReq()
	req.Name = "pvc-ready"

	resp, err := svc.CreateVolume(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "bne/pve-1/local/vm-9997-pvc-ready", resp.VolumeId)
	assert.Equal(t, int64(1073741824), resp.CapacityBytes)
	assert.Equal(t, []string{"pve-1"}, resp.AccessibleTopology)
	// The context direct mode's CreateVolume would return, `storage` included
	// even though the request carried it as its own field.
	assert.Equal(t, "local", resp.VolumeContext["storage"])
	assert.Equal(t, "1", resp.VolumeContext["iothread"])
}

func TestCreateVolumeAlreadyExistsDifferentSpec(t *testing.T) {
	existing := &v1alpha1.ProxmoxVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-diff",
			Namespace: "tenant-ns",
		},
		Spec: v1alpha1.ProxmoxVolumeSpec{
			Region:        "bne",
			Zone:          "pve-1",
			Storage:       "local",
			CapacityBytes: 2147483648, // Different from request
		},
	}

	svc, _ := serviceWithTenant(t, existing)
	ctx := tenantCtx()

	req := createReq()
	req.Name = "pvc-diff"

	_, err := svc.CreateVolume(ctx, req)
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
}

func TestCreateVolumeRejectedReasonMapping(t *testing.T) {
	tests := []struct {
		reason string
		code   codes.Code
	}{
		{v1alpha1.ReasonQuotaExceeded, codes.ResourceExhausted},
		{v1alpha1.ReasonNamespaceQuota, codes.ResourceExhausted},
		{v1alpha1.ReasonTenantSuspended, codes.PermissionDenied},
		{v1alpha1.ReasonTenantObserveOnly, codes.PermissionDenied},
		{v1alpha1.ReasonStorageNotAllowed, codes.PermissionDenied},
		{v1alpha1.ReasonVMIDNotOwned, codes.PermissionDenied},
		{v1alpha1.ReasonInvalidSpec, codes.InvalidArgument},
		{v1alpha1.ReasonParameterRejected, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.reason, func(t *testing.T) {
			existing := &v1alpha1.ProxmoxVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-rejected",
					Namespace: "tenant-ns",
				},
				Spec: v1alpha1.ProxmoxVolumeSpec{
					Region:        "bne",
					Zone:          "pve-1",
					Storage:       "local",
					CapacityBytes: 1073741824,
				},
				Status: v1alpha1.ProxmoxVolumeStatus{
					Phase: v1alpha1.VolumePhaseRejected,
					Conditions: []metav1.Condition{{
						Type:               v1alpha1.ConditionAdmitted,
						Status:             metav1.ConditionFalse,
						Reason:             tt.reason,
						Message:            "test rejection: " + tt.reason,
						LastTransitionTime: metav1.Now(),
					}},
				},
			}

			svc, _ := serviceWithTenant(t, existing)
			ctx := tenantCtx()

			req := createReq()
			req.Name = "pvc-rejected"

			_, err := svc.CreateVolume(ctx, req)
			require.Error(t, err)
			assert.Equal(t, tt.code, status.Code(err),
				"reason %s should map to %s", tt.reason, tt.code)
		})
	}
}

func TestDeleteVolumeIdempotent(t *testing.T) {
	svc, _ := serviceWithTenant(t)
	ctx := tenantCtx()

	// Volume doesn't exist: should succeed (idempotent).
	resp, err := svc.DeleteVolume(ctx, &volumev1.DeleteVolumeRequest{
		VolumeId: "bne/pve-1/local/vm-9997-pvc-gone",
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestDeleteVolumeUnavailableWhilePresent(t *testing.T) {
	existing := &v1alpha1.ProxmoxVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-del",
			Namespace:  "tenant-ns",
			Finalizers: []string{v1alpha1.VolumeFinalizer},
		},
		Spec: v1alpha1.ProxmoxVolumeSpec{
			Region:        "bne",
			Zone:          "pve-1",
			Storage:       "local",
			CapacityBytes: 1073741824,
		},
		Status: v1alpha1.ProxmoxVolumeStatus{
			VolumeID: "bne/pve-1/local/vm-9997-pvc-del",
			Phase:    v1alpha1.VolumePhaseReady,
		},
	}

	svc, _ := serviceWithTenant(t, existing)
	ctx := tenantCtx()

	// Delete: the object has a finalizer, so it won't be removed.
	_, err := svc.DeleteVolume(ctx, &volumev1.DeleteVolumeRequest{
		VolumeId: "bne/pve-1/local/vm-9997-pvc-del",
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestWatchCapacityFiltersToAllowedStorages(t *testing.T) {
	storages := []client.Object{
		&v1alpha1.ProxmoxStorage{
			ObjectMeta: metav1.ObjectMeta{Name: "bne-pve1-local"},
			Spec:       v1alpha1.ProxmoxStorageSpec{Region: "bne", Zone: "pve-1", Storage: "local"},
			Status:     v1alpha1.ProxmoxStorageStatus{Active: true, AvailableBytes: 100, TotalBytes: 200},
		},
		&v1alpha1.ProxmoxStorage{
			ObjectMeta: metav1.ObjectMeta{Name: "bne-pve1-ceph"},
			Spec:       v1alpha1.ProxmoxStorageSpec{Region: "bne", Zone: "pve-1", Storage: "ceph"},
			Status:     v1alpha1.ProxmoxStorageStatus{Active: true, AvailableBytes: 500, TotalBytes: 1000},
		},
	}

	svc, _ := serviceWithTenant(t, storages...)

	// Build a fake stream to capture sends.
	sent := &captureStream{ctx: tenantCtx()}

	// WatchCapacity blocks in a loop, so run it in a goroutine with a canceling context.
	ctx, cancel := context.WithCancel(sent.ctx)
	defer cancel()

	sent.ctx = ctx

	// Directly test sendCapacity instead of the blocking WatchCapacity.
	req := &volumev1.WatchCapacityRequest{}
	err := volumeapi.SendCapacityForTest(ctx, svc, req, sent)
	require.NoError(t, err)

	// The tenant's allowedStorages is ["local"], so ceph should be filtered out.
	assert.Len(t, sent.responses, 1)
	assert.Equal(t, "local", sent.responses[0].Storage)
	assert.Equal(t, int64(100), sent.responses[0].AvailableBytes)
}

// captureStream captures WatchCapacity sends.
type captureStream struct {
	ctx       context.Context //nolint:containedctx // Required to implement grpc.ServerStreamingServer.
	responses []*volumev1.WatchCapacityResponse
}

func (c *captureStream) Send(resp *volumev1.WatchCapacityResponse) error {
	c.responses = append(c.responses, resp)

	return nil
}

func (c *captureStream) Context() context.Context     { return c.ctx }
func (c *captureStream) SetHeader(metadata.MD) error  { return nil }
func (c *captureStream) SendHeader(metadata.MD) error { return nil }
func (c *captureStream) SetTrailer(metadata.MD)       {}
func (c *captureStream) SendMsg(interface{}) error    { return nil }
func (c *captureStream) RecvMsg(interface{}) error    { return nil }

func TestCreateVolumeFailedDeletesAndReturnsFinal(t *testing.T) {
	existing := &v1alpha1.ProxmoxVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-failed",
			Namespace: "tenant-ns",
		},
		Spec: v1alpha1.ProxmoxVolumeSpec{
			Region:        "bne",
			Zone:          "pve-1",
			Storage:       "local",
			CapacityBytes: 1073741824,
		},
		Status: v1alpha1.ProxmoxVolumeStatus{
			Phase: v1alpha1.VolumePhaseFailed,
			Conditions: []metav1.Condition{{
				Type:               v1alpha1.ConditionReady,
				Status:             metav1.ConditionFalse,
				Reason:             v1alpha1.ReasonProxmoxError,
				Message:            "disk creation failed: timeout",
				LastTransitionTime: metav1.Now(),
			}},
		},
	}

	svc, _ := serviceWithTenant(t, existing)
	ctx := tenantCtx()

	req := createReq()
	req.Name = "pvc-failed"

	_, err := svc.CreateVolume(ctx, req)
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err),
		"failed volume must return a final error code so external-provisioner stops retrying")
	assert.Contains(t, err.Error(), "disk creation failed: timeout")
}

func TestCreateVolumeUnavailableWhilePending(t *testing.T) {
	existing := &v1alpha1.ProxmoxVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-pending",
			Namespace: "tenant-ns",
		},
		Spec: v1alpha1.ProxmoxVolumeSpec{
			Region:        "bne",
			Zone:          "pve-1",
			Storage:       "local",
			CapacityBytes: 1073741824,
		},
		Status: v1alpha1.ProxmoxVolumeStatus{
			Phase: v1alpha1.VolumePhasePending,
		},
	}

	svc, _ := serviceWithTenant(t, existing)
	ctx := tenantCtx()

	req := createReq()
	req.Name = "pvc-pending"

	_, err := svc.CreateVolume(ctx, req)
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestRateLimiterReturnsResourceExhausted(t *testing.T) {
	// Create a rate limiter with burst=1 so the second call exceeds it.
	rl := volumeapi.NewRateLimiter(0.001, 1) // Nearly zero rate with burst=1.

	tenant := &v1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-tenant"},
		Spec: v1alpha1.TenantClusterSpec{
			Namespace:       "tenant-ns",
			Region:          "bne",
			Subject:         "pvx:test-client",
			PlaceholderVMID: 9997,
			AllowedStorages: []string{"local"},
			Mode:            v1alpha1.TenantModeEnforce,
		},
	}

	ctx := volumeapi.ContextWithTenantForTest(context.Background(), tenant)

	interceptor := rl.UnaryInterceptor()

	handler := func(_ context.Context, _ interface{}) (interface{}, error) {
		return "ok", nil
	}

	// First call succeeds (within burst).
	resp, err := interceptor(ctx, nil, nil, handler)
	require.NoError(t, err)
	assert.Equal(t, "ok", resp)

	// Subsequent calls exceed the rate limit.
	var gotExhausted bool

	for range 10 {
		_, err := interceptor(ctx, nil, nil, handler)
		if err != nil && status.Code(err) == codes.ResourceExhausted {
			gotExhausted = true

			break
		}
	}

	assert.True(t, gotExhausted, "rate limiter must return ResourceExhausted past burst")
}

func tenantCtxWithVMIDs() context.Context {
	tenant := &v1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-tenant"},
		Spec: v1alpha1.TenantClusterSpec{
			Namespace:       "tenant-ns",
			Region:          "bne",
			Subject:         "pvx:test-client",
			PlaceholderVMID: 9997,
			AllowedStorages: []string{"local"},
			Mode:            v1alpha1.TenantModeEnforce,
		},
		Status: v1alpha1.TenantClusterStatus{
			ResolvedVMIDs: []int32{411, 421, 422, 423},
		},
	}

	return volumeapi.ContextWithTenantForTest(context.Background(), tenant)
}

func readyVolumeForAttach() *v1alpha1.ProxmoxVolume {
	return &v1alpha1.ProxmoxVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-abc",
			Namespace: "tenant-ns",
		},
		Spec: v1alpha1.ProxmoxVolumeSpec{
			Region:        "bne",
			Zone:          "pve-1",
			Storage:       "local",
			CapacityBytes: 1073741824,
		},
		Status: v1alpha1.ProxmoxVolumeStatus{
			VolumeID:      "bne/pve-1/local/vm-9997-pvc-abc",
			CapacityBytes: 1073741824,
			Phase:         v1alpha1.VolumePhaseReady,
		},
	}
}

func TestAttachVolumeCreatesAttachment(t *testing.T) {
	svc, c := serviceWithTenant(t, readyVolumeForAttach())
	ctx := tenantCtxWithVMIDs()

	// First call creates the attachment and returns Unavailable (not yet attached).
	_, err := svc.AttachVolume(ctx, &volumev1.AttachVolumeRequest{
		VolumeId: "bne/pve-1/local/vm-9997-pvc-abc",
		NodeId:   "syd1-pub1-n1/411",
		Vmid:     411,
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))

	// Verify the attachment CR was created.
	attName := volumeapi.AttachmentNameForTest("bne/pve-1/local/vm-9997-pvc-abc", "syd1-pub1-n1/411")
	att := &v1alpha1.ProxmoxVolumeAttachment{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "tenant-ns", Name: attName}, att))

	assert.Equal(t, "pvc-abc", att.Spec.VolumeName)
	assert.Equal(t, "bne/pve-1/local/vm-9997-pvc-abc", att.Spec.VolumeID)
	assert.Equal(t, "syd1-pub1-n1/411", att.Spec.NodeID)
	assert.Equal(t, int32(411), att.Spec.VMID)
	assert.Equal(t, "test-tenant", att.Labels[v1alpha1.LabelTenant])
}

func TestAttachVolumeReturnsWhenAttached(t *testing.T) {
	vol := readyVolumeForAttach()
	attName := volumeapi.AttachmentNameForTest("bne/pve-1/local/vm-9997-pvc-abc", "syd1-pub1-n1/411")

	// Pre-create an attached attachment.
	att := &v1alpha1.ProxmoxVolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      attName,
			Namespace: "tenant-ns",
		},
		Spec: v1alpha1.ProxmoxVolumeAttachmentSpec{
			VolumeName: "pvc-abc",
			VolumeID:   "bne/pve-1/local/vm-9997-pvc-abc",
			NodeID:     "syd1-pub1-n1/411",
			VMID:       411,
		},
		Status: v1alpha1.ProxmoxVolumeAttachmentStatus{
			Attached:   true,
			DevicePath: "/dev/disk/by-id/wwn-0xabc123",
			LUN:        "3",
		},
	}

	svc, _ := serviceWithTenant(t, vol, att)
	ctx := tenantCtxWithVMIDs()

	resp, err := svc.AttachVolume(ctx, &volumev1.AttachVolumeRequest{
		VolumeId: "bne/pve-1/local/vm-9997-pvc-abc",
		NodeId:   "syd1-pub1-n1/411",
		Vmid:     411,
	})
	require.NoError(t, err)
	assert.Equal(t, "/dev/disk/by-id/wwn-0xabc123", resp.DevicePath)
	assert.Equal(t, int32(3), resp.Lun)
}

func TestAttachVolumeRefusedVMIDNotInResolvedVmids(t *testing.T) {
	svc, _ := serviceWithTenant(t, readyVolumeForAttach())
	ctx := tenantCtxWithVMIDs()

	_, err := svc.AttachVolume(ctx, &volumev1.AttachVolumeRequest{
		VolumeId: "bne/pve-1/local/vm-9997-pvc-abc",
		NodeId:   "syd1-pub1-n1/999",
		Vmid:     999,
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestAttachVolumeNotFound(t *testing.T) {
	svc, _ := serviceWithTenant(t)
	ctx := tenantCtxWithVMIDs()

	_, err := svc.AttachVolume(ctx, &volumev1.AttachVolumeRequest{
		VolumeId: "bne/pve-1/local/vm-9997-pvc-missing",
		NodeId:   "syd1-pub1-n1/411",
		Vmid:     411,
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestAttachVolumeVMIDFromNodeID(t *testing.T) {
	svc, c := serviceWithTenant(t, readyVolumeForAttach())
	ctx := tenantCtxWithVMIDs()

	// Pass vmid=0, let it resolve from node ID "syd1-pub1-n1/421".
	_, err := svc.AttachVolume(ctx, &volumev1.AttachVolumeRequest{
		VolumeId: "bne/pve-1/local/vm-9997-pvc-abc",
		NodeId:   "syd1-pub1-n1/421",
		Vmid:     0,
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))

	// Verify the attachment was created with VMID 421.
	attName := volumeapi.AttachmentNameForTest("bne/pve-1/local/vm-9997-pvc-abc", "syd1-pub1-n1/421")
	att := &v1alpha1.ProxmoxVolumeAttachment{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "tenant-ns", Name: attName}, att))
	assert.Equal(t, int32(421), att.Spec.VMID)
}

func TestDetachVolumeIdempotent(t *testing.T) {
	svc, _ := serviceWithTenant(t)
	ctx := tenantCtxWithVMIDs()

	// Attachment doesn't exist: should succeed.
	resp, err := svc.DetachVolume(ctx, &volumev1.DetachVolumeRequest{
		VolumeId: "bne/pve-1/local/vm-9997-pvc-abc",
		NodeId:   "syd1-pub1-n1/411",
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestDetachVolumeUnavailableWhilePresent(t *testing.T) {
	attName := volumeapi.AttachmentNameForTest("bne/pve-1/local/vm-9997-pvc-abc", "syd1-pub1-n1/411")

	att := &v1alpha1.ProxmoxVolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       attName,
			Namespace:  "tenant-ns",
			Finalizers: []string{v1alpha1.AttachmentFinalizer},
		},
		Spec: v1alpha1.ProxmoxVolumeAttachmentSpec{
			VolumeName: "pvc-abc",
			VolumeID:   "bne/pve-1/local/vm-9997-pvc-abc",
			NodeID:     "syd1-pub1-n1/411",
			VMID:       411,
		},
	}

	svc, _ := serviceWithTenant(t, att)
	ctx := tenantCtxWithVMIDs()

	_, err := svc.DetachVolume(ctx, &volumev1.DetachVolumeRequest{
		VolumeId: "bne/pve-1/local/vm-9997-pvc-abc",
		NodeId:   "syd1-pub1-n1/411",
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestCreateSnapshotFailedReturnsFinalError(t *testing.T) {
	existing := &v1alpha1.ProxmoxVolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "snap-failed",
			Namespace: "tenant-ns",
		},
		Spec: v1alpha1.ProxmoxVolumeSnapshotSpec{
			SourceVolumeName: "pvc-src",
		},
		Status: v1alpha1.ProxmoxVolumeSnapshotStatus{
			SnapshotID: "bne/pve-1/local/vm-9997-snap-failed",
			ReadyToUse: false,
			Conditions: []metav1.Condition{{
				Type:               v1alpha1.ConditionReady,
				Status:             metav1.ConditionFalse,
				Reason:             v1alpha1.ReasonProxmoxError,
				Message:            "copying disk: SSH connection refused",
				LastTransitionTime: metav1.Now(),
			}},
		},
	}

	srcVol := &v1alpha1.ProxmoxVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-src",
			Namespace: "tenant-ns",
		},
		Spec: v1alpha1.ProxmoxVolumeSpec{
			Region:        "bne",
			Zone:          "pve-1",
			Storage:       "local",
			CapacityBytes: 1073741824,
		},
		Status: v1alpha1.ProxmoxVolumeStatus{
			Phase: v1alpha1.VolumePhaseReady,
		},
	}

	svc, _ := serviceWithTenant(t, existing, srcVol)
	ctx := tenantCtx()

	_, err := svc.CreateSnapshot(ctx, &volumev1.CreateSnapshotRequest{
		Name:             "snap-failed",
		SourceVolumeName: "pvc-src",
	})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err),
		"failed snapshot must return a final error so external-snapshotter stops retrying")
	assert.Contains(t, err.Error(), "SSH connection refused")
}

func TestCreateSnapshotPendingReturnsUnavailable(t *testing.T) {
	// A snapshot that is still being created (no conditions yet).
	existing := &v1alpha1.ProxmoxVolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "snap-pending",
			Namespace: "tenant-ns",
		},
		Spec: v1alpha1.ProxmoxVolumeSnapshotSpec{
			SourceVolumeName: "pvc-src",
		},
	}

	srcVol := &v1alpha1.ProxmoxVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-src",
			Namespace: "tenant-ns",
		},
		Spec: v1alpha1.ProxmoxVolumeSpec{
			Region:        "bne",
			Zone:          "pve-1",
			Storage:       "local",
			CapacityBytes: 1073741824,
		},
		Status: v1alpha1.ProxmoxVolumeStatus{
			Phase: v1alpha1.VolumePhaseReady,
		},
	}

	svc, _ := serviceWithTenant(t, existing, srcVol)
	ctx := tenantCtx()

	_, err := svc.CreateSnapshot(ctx, &volumev1.CreateSnapshotRequest{
		Name:             "snap-pending",
		SourceVolumeName: "pvc-src",
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err),
		"pending snapshot must return Unavailable (retryable)")
}

// Suppress unused import.
var _ = apimeta.FindStatusCondition
