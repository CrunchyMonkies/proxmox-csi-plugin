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

package remote_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	volumev1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/volume/v1"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi/remote"

	corev1 "k8s.io/api/core/v1"
)

const bufSize = 1024 * 1024

// fakeVolumeService is an in-process fake of the operator's VolumeService.
type fakeVolumeService struct {
	volumev1.UnimplementedVolumeServiceServer

	createReq  *volumev1.CreateVolumeRequest
	createResp *volumev1.CreateVolumeResponse
	createErr  error

	deleteReq  *volumev1.DeleteVolumeRequest
	deleteResp *volumev1.DeleteVolumeResponse
	deleteErr  error

	attachReq  *volumev1.AttachVolumeRequest
	attachResp *volumev1.AttachVolumeResponse
	attachErr  error

	detachReq  *volumev1.DetachVolumeRequest
	detachResp *volumev1.DetachVolumeResponse
	detachErr  error

	expandReq  *volumev1.ExpandVolumeRequest
	expandResp *volumev1.ExpandVolumeResponse
	expandErr  error

	modifyReq  *volumev1.ModifyVolumeRequest
	modifyResp *volumev1.ModifyVolumeResponse
	modifyErr  error

	createSnapReq  *volumev1.CreateSnapshotRequest
	createSnapResp *volumev1.CreateSnapshotResponse
	createSnapErr  error

	deleteSnapReq  *volumev1.DeleteSnapshotRequest
	deleteSnapResp *volumev1.DeleteSnapshotResponse
	deleteSnapErr  error

	listSnapReq  *volumev1.ListSnapshotsRequest
	listSnapResp *volumev1.ListSnapshotsResponse
	listSnapErr  error

	watchResponses []*volumev1.WatchCapacityResponse
	watchErr       error
}

func (f *fakeVolumeService) CreateVolume(_ context.Context, req *volumev1.CreateVolumeRequest) (*volumev1.CreateVolumeResponse, error) {
	f.createReq = req

	if f.createErr != nil {
		return nil, f.createErr
	}

	return f.createResp, nil
}

func (f *fakeVolumeService) DeleteVolume(_ context.Context, req *volumev1.DeleteVolumeRequest) (*volumev1.DeleteVolumeResponse, error) {
	f.deleteReq = req

	if f.deleteErr != nil {
		return nil, f.deleteErr
	}

	return f.deleteResp, nil
}

func (f *fakeVolumeService) AttachVolume(_ context.Context, req *volumev1.AttachVolumeRequest) (*volumev1.AttachVolumeResponse, error) {
	f.attachReq = req

	if f.attachErr != nil {
		return nil, f.attachErr
	}

	return f.attachResp, nil
}

func (f *fakeVolumeService) DetachVolume(_ context.Context, req *volumev1.DetachVolumeRequest) (*volumev1.DetachVolumeResponse, error) {
	f.detachReq = req

	if f.detachErr != nil {
		return nil, f.detachErr
	}

	return f.detachResp, nil
}

func (f *fakeVolumeService) ExpandVolume(_ context.Context, req *volumev1.ExpandVolumeRequest) (*volumev1.ExpandVolumeResponse, error) {
	f.expandReq = req

	if f.expandErr != nil {
		return nil, f.expandErr
	}

	return f.expandResp, nil
}

func (f *fakeVolumeService) ModifyVolume(_ context.Context, req *volumev1.ModifyVolumeRequest) (*volumev1.ModifyVolumeResponse, error) {
	f.modifyReq = req

	if f.modifyErr != nil {
		return nil, f.modifyErr
	}

	return f.modifyResp, nil
}

func (f *fakeVolumeService) CreateSnapshot(_ context.Context, req *volumev1.CreateSnapshotRequest) (*volumev1.CreateSnapshotResponse, error) {
	f.createSnapReq = req

	if f.createSnapErr != nil {
		return nil, f.createSnapErr
	}

	return f.createSnapResp, nil
}

func (f *fakeVolumeService) DeleteSnapshot(_ context.Context, req *volumev1.DeleteSnapshotRequest) (*volumev1.DeleteSnapshotResponse, error) {
	f.deleteSnapReq = req

	if f.deleteSnapErr != nil {
		return nil, f.deleteSnapErr
	}

	return f.deleteSnapResp, nil
}

func (f *fakeVolumeService) ListSnapshots(_ context.Context, req *volumev1.ListSnapshotsRequest) (*volumev1.ListSnapshotsResponse, error) {
	f.listSnapReq = req

	if f.listSnapErr != nil {
		return nil, f.listSnapErr
	}

	return f.listSnapResp, nil
}

func (f *fakeVolumeService) WatchCapacity(_ *volumev1.WatchCapacityRequest, stream grpc.ServerStreamingServer[volumev1.WatchCapacityResponse]) error {
	if f.watchErr != nil {
		return f.watchErr
	}

	for _, r := range f.watchResponses {
		if err := stream.Send(r); err != nil {
			return err
		}
	}

	<-stream.Context().Done()

	return nil
}

func setup(t *testing.T) (*remote.ControllerServer, *fakeVolumeService) {
	t.Helper()

	listener := bufconn.Listen(bufSize)
	srv := grpc.NewServer()

	fk := &fakeVolumeService{}
	volumev1.RegisterVolumeServiceServer(srv, fk)

	go func() {
		if err := srv.Serve(listener); err != nil {
			// Server was stopped; expected during test cleanup.
			return
		}
	}()

	t.Cleanup(func() {
		srv.Stop()

		if err := listener.Close(); err != nil {
			t.Logf("closing listener: %v", err)
		}
	})

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Logf("closing conn: %v", err)
		}
	})

	return remote.NewControllerServer(conn), fk
}

func TestCreateVolumeMapping(t *testing.T) {
	cs, fk := setup(t)

	fk.createResp = &volumev1.CreateVolumeResponse{
		VolumeId:           "bne/pve-1/local-lvm/vm-9991-pvc-abc",
		VolumeContext:      map[string]string{"rootDirPermissions": "0777", "storage": "local-lvm"},
		CapacityBytes:      1073741824,
		AccessibleTopology: []string{"pve-1"},
	}

	req := &csi.CreateVolumeRequest{
		Name: "pvc-abc",
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1073741824},
		Parameters: map[string]string{
			"storage":                          "local-lvm",
			"csi.storage.k8s.io/pv/name":       "pvc-abc",
			"csi.storage.k8s.io/pvc/name":      "my-pvc",
			"csi.storage.k8s.io/pvc/namespace": "default",
			"storageFormat":                    "raw",
		},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{{
				Segments: map[string]string{
					corev1.LabelTopologyRegion: "bne",
					corev1.LabelTopologyZone:   "pve-1",
				},
			}},
		},
	}

	resp, err := cs.CreateVolume(context.Background(), req)
	require.NoError(t, err)

	assert.Equal(t, "bne/pve-1/local-lvm/vm-9991-pvc-abc", resp.Volume.VolumeId)
	assert.Equal(t, int64(1073741824), resp.Volume.CapacityBytes)
	assert.Equal(t, map[string]string{"rootDirPermissions": "0777", "storage": "local-lvm"}, resp.Volume.VolumeContext)
	require.Len(t, resp.Volume.AccessibleTopology, 1)
	assert.Equal(t, "bne", resp.Volume.AccessibleTopology[0].Segments[corev1.LabelTopologyRegion])
	assert.Equal(t, "pve-1", resp.Volume.AccessibleTopology[0].Segments[corev1.LabelTopologyZone])

	require.NotNil(t, fk.createReq)
	assert.Equal(t, "pvc-abc", fk.createReq.Name)
	assert.Equal(t, "bne", fk.createReq.Region)
	assert.Equal(t, "pve-1", fk.createReq.Zone)
	assert.Equal(t, "local-lvm", fk.createReq.Storage)
	assert.Equal(t, int64(1073741824), fk.createReq.CapacityBytes)

	for k := range fk.createReq.Parameters {
		assert.NotContains(t, k, "csi.storage.k8s.io/",
			"csi.storage.k8s.io/* key %q should be stripped", k)
	}

	_, hasStorage := fk.createReq.Parameters["storage"]
	assert.False(t, hasStorage, "storage key should be stripped from parameters")
	assert.Equal(t, "raw", fk.createReq.Parameters["storageFormat"])

	assert.Equal(t, "pvc-abc", fk.createReq.PvName)
	require.NotNil(t, fk.createReq.ClaimRef)
	assert.Equal(t, "my-pvc", fk.createReq.ClaimRef.Name)
	assert.Equal(t, "default", fk.createReq.ClaimRef.Namespace)
}

func TestCreateVolumeUnavailablePassthrough(t *testing.T) {
	cs, fk := setup(t)

	fk.createErr = status.Error(codes.Unavailable, "provisioning volume pvc-abc")

	req := &csi.CreateVolumeRequest{
		Name: "pvc-abc",
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1073741824},
		Parameters:    map[string]string{"storage": "local-lvm"},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{{
				Segments: map[string]string{
					corev1.LabelTopologyRegion: "bne",
					corev1.LabelTopologyZone:   "pve-1",
				},
			}},
		},
	}

	_, err := cs.CreateVolume(context.Background(), req)
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err),
		"Unavailable from the operator must pass through unchanged")
}

func TestCreateVolumeTopologyExtraction(t *testing.T) {
	cs, fk := setup(t)

	fk.createResp = &volumev1.CreateVolumeResponse{
		VolumeId:      "bne/pve-2/local-lvm/vm-100-pvc-topo",
		CapacityBytes: 1073741824,
	}

	req := &csi.CreateVolumeRequest{
		Name: "pvc-topo",
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1073741824},
		Parameters:    map[string]string{"storage": "local-lvm"},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{{
				Segments: map[string]string{
					corev1.LabelTopologyRegion: "bne",
					corev1.LabelTopologyZone:   "pve-2",
				},
			}},
		},
	}

	_, err := cs.CreateVolume(context.Background(), req)
	require.NoError(t, err)

	require.NotNil(t, fk.createReq)
	assert.Equal(t, "bne", fk.createReq.Region)
	assert.Equal(t, "pve-2", fk.createReq.Zone)
}

func TestDeleteVolumeIdempotent(t *testing.T) {
	cs, fk := setup(t)

	fk.deleteResp = &volumev1.DeleteVolumeResponse{}

	resp, err := cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: "bne/pve-1/local-lvm/vm-9991-pvc-abc",
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)

	require.NotNil(t, fk.deleteReq)
	assert.Equal(t, "bne/pve-1/local-lvm/vm-9991-pvc-abc", fk.deleteReq.VolumeId)
}

func TestDeleteVolumeEmptyIDReturnsInvalidArgument(t *testing.T) {
	cs, _ := setup(t)

	_, err := cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: ""})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetCapacityFromWatchCache(t *testing.T) {
	cs, fk := setup(t)

	fk.watchResponses = []*volumev1.WatchCapacityResponse{
		{Storage: "local-lvm", Zone: "pve-1", AvailableBytes: 50 * 1024 * 1024 * 1024, TotalBytes: 100 * 1024 * 1024 * 1024},
		{Storage: "rbd", Zone: "pve-1", AvailableBytes: 200 * 1024 * 1024 * 1024, TotalBytes: 500 * 1024 * 1024 * 1024},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cs.StartCapacityWatch(ctx)

	var gotCapacity bool

	for range 200 {
		resp, err := cs.GetCapacity(context.Background(), &csi.GetCapacityRequest{
			AccessibleTopology: &csi.Topology{
				Segments: map[string]string{corev1.LabelTopologyZone: "pve-1"},
			},
			Parameters: map[string]string{"storage": "local-lvm"},
		})
		if err == nil && resp.AvailableCapacity > 0 {
			gotCapacity = true

			assert.Equal(t, int64(50*1024*1024*1024), resp.AvailableCapacity)

			break
		}

		time.Sleep(5 * time.Millisecond)
	}

	cancel()

	assert.True(t, gotCapacity, "capacity cache should be populated from WatchCapacity")
}

func TestGetCapacityZoneStorageMatch(t *testing.T) {
	cs, fk := setup(t)

	fk.watchResponses = []*volumev1.WatchCapacityResponse{
		{Storage: "local-lvm", Zone: "pve-1", AvailableBytes: 50 * 1024 * 1024 * 1024},
		{Storage: "rbd", Zone: "pve-1", AvailableBytes: 200 * 1024 * 1024 * 1024},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cs.StartCapacityWatch(ctx)

	for range 200 {
		resp, err := cs.GetCapacity(context.Background(), &csi.GetCapacityRequest{
			AccessibleTopology: &csi.Topology{
				Segments: map[string]string{corev1.LabelTopologyZone: "pve-1"},
			},
			Parameters: map[string]string{"storage": "rbd"},
		})
		if err == nil && resp != nil && resp.AvailableCapacity > 0 {
			break
		}

		time.Sleep(5 * time.Millisecond)
	}

	cancel()

	resp, err := cs.GetCapacity(context.Background(), &csi.GetCapacityRequest{
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{corev1.LabelTopologyZone: "pve-1"},
		},
		Parameters: map[string]string{"storage": "rbd"},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(200*1024*1024*1024), resp.AvailableCapacity)

	resp, err = cs.GetCapacity(context.Background(), &csi.GetCapacityRequest{
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{corev1.LabelTopologyZone: "pve-99"},
		},
		Parameters: map[string]string{"storage": "local-lvm"},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), resp.AvailableCapacity)
}

func TestOutgoingMetadataCarriesAuthorization(t *testing.T) {
	listener := bufconn.Listen(bufSize)

	var capturedMD metadata.MD

	srv := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			md, ok := metadata.FromIncomingContext(ctx)
			if ok {
				capturedMD = md
			}

			return handler(ctx, req)
		}),
	)

	fk := &fakeVolumeService{
		deleteResp: &volumev1.DeleteVolumeResponse{},
	}

	volumev1.RegisterVolumeServiceServer(srv, fk)

	go func() {
		if err := srv.Serve(listener); err != nil {
			return
		}
	}()

	t.Cleanup(func() {
		srv.Stop()

		if err := listener.Close(); err != nil {
			t.Logf("closing listener: %v", err)
		}
	})

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(&staticToken{token: "test-bearer-token"}),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Logf("closing conn: %v", err)
		}
	})

	cs := remote.NewControllerServer(conn)

	_, err = cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: "bne/pve-1/local-lvm/vm-9991-pvc-abc",
	})
	require.NoError(t, err)

	authValues := capturedMD.Get("authorization")
	require.NotEmpty(t, authValues, "authorization header must be present")
	assert.Equal(t, "Bearer test-bearer-token", authValues[0])
}

func TestControllerPublishVolumeMapping(t *testing.T) {
	cs, fk := setup(t)

	fk.attachResp = &volumev1.AttachVolumeResponse{
		DevicePath:     "/dev/disk/by-id/wwn-0x5056432d494430310a",
		Lun:            3,
		ResizeRequired: true,
	}

	req := &csi.ControllerPublishVolumeRequest{
		VolumeId: "bne/pve-1/local/vm-9997-pvc-abc",
		NodeId:   "syd1-pub1-n1/411",
		Readonly: false,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
	}

	resp, err := cs.ControllerPublishVolume(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, "/dev/disk/by-id/wwn-0x5056432d494430310a", resp.PublishContext["DevicePath"])
	assert.Equal(t, "3", resp.PublishContext["lun"])
	assert.Equal(t, "true", resp.PublishContext["resizeRequired"])

	// Verify the proto request.
	require.NotNil(t, fk.attachReq)
	assert.Equal(t, "bne/pve-1/local/vm-9997-pvc-abc", fk.attachReq.VolumeId)
	assert.Equal(t, "syd1-pub1-n1/411", fk.attachReq.NodeId)
	assert.Equal(t, int32(411), fk.attachReq.Vmid, "VMID should be extracted from node ID")
}

func TestControllerPublishVolumeNoVMID(t *testing.T) {
	cs, fk := setup(t)

	fk.attachResp = &volumev1.AttachVolumeResponse{
		DevicePath: "/dev/disk/by-id/wwn-0xabc",
		Lun:        1,
	}

	req := &csi.ControllerPublishVolumeRequest{
		VolumeId: "bne/pve-1/local/vm-9997-pvc-abc",
		NodeId:   "syd1-pub1-n1", // No VMID in node ID.
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
	}

	resp, err := cs.ControllerPublishVolume(context.Background(), req)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.PublishContext["DevicePath"])

	require.NotNil(t, fk.attachReq)
	assert.Equal(t, int32(0), fk.attachReq.Vmid, "VMID should be 0 when not in node ID")
}

func TestControllerPublishVolumeNoResizeRequired(t *testing.T) {
	cs, fk := setup(t)

	fk.attachResp = &volumev1.AttachVolumeResponse{
		DevicePath:     "/dev/disk/by-id/wwn-0xabc",
		Lun:            1,
		ResizeRequired: false,
	}

	req := &csi.ControllerPublishVolumeRequest{
		VolumeId: "bne/pve-1/local/vm-9997-pvc-abc",
		NodeId:   "syd1-pub1-n1/411",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
	}

	resp, err := cs.ControllerPublishVolume(context.Background(), req)
	require.NoError(t, err)

	_, hasResize := resp.PublishContext["resizeRequired"]
	assert.False(t, hasResize, "resizeRequired should not be set when false")
}

func TestControllerUnpublishVolumeMapping(t *testing.T) {
	cs, fk := setup(t)

	fk.detachResp = &volumev1.DetachVolumeResponse{}

	req := &csi.ControllerUnpublishVolumeRequest{
		VolumeId: "bne/pve-1/local/vm-9997-pvc-abc",
		NodeId:   "syd1-pub1-n1/411",
	}

	resp, err := cs.ControllerUnpublishVolume(context.Background(), req)
	require.NoError(t, err)
	assert.NotNil(t, resp)

	require.NotNil(t, fk.detachReq)
	assert.Equal(t, "bne/pve-1/local/vm-9997-pvc-abc", fk.detachReq.VolumeId)
	assert.Equal(t, "syd1-pub1-n1/411", fk.detachReq.NodeId)
	assert.Equal(t, int32(411), fk.detachReq.Vmid)
}

func TestControllerUnpublishVolumeUnavailablePassthrough(t *testing.T) {
	cs, fk := setup(t)

	fk.detachErr = status.Error(codes.Unavailable, "detaching")

	_, err := cs.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: "bne/pve-1/local/vm-9997-pvc-abc",
		NodeId:   "syd1-pub1-n1/411",
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestControllerPublishVolumeEmptyVolumeIDReturnsInvalidArgument(t *testing.T) {
	cs, _ := setup(t)

	_, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "",
		NodeId:   "syd1-pub1-n1/411",
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestControllerPublishVolumeEmptyNodeIDReturnsInvalidArgument(t *testing.T) {
	cs, _ := setup(t)

	_, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "bne/pve-1/local/vm-9997-pvc-abc",
		NodeId:   "",
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

type staticToken struct {
	token string
}

func (s *staticToken) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Bearer " + s.token,
	}, nil
}

func (s *staticToken) RequireTransportSecurity() bool { return false }

func TestControllerExpandVolumeMapping(t *testing.T) {
	cs, fk := setup(t)

	fk.expandResp = &volumev1.ExpandVolumeResponse{
		CapacityBytes:         2 * 1024 * 1024 * 1024,
		NodeExpansionRequired: true,
	}

	resp, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      "bne/pve-1/local/vm-9997-pvc-abc",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 2 * 1024 * 1024 * 1024},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2*1024*1024*1024), resp.CapacityBytes)
	assert.True(t, resp.NodeExpansionRequired)

	require.NotNil(t, fk.expandReq)
	assert.Equal(t, "bne/pve-1/local/vm-9997-pvc-abc", fk.expandReq.VolumeId)
	assert.Equal(t, int64(2*1024*1024*1024), fk.expandReq.CapacityBytes)
}

func TestControllerExpandVolumeEmptyIDReturnsInvalidArgument(t *testing.T) {
	cs, _ := setup(t)

	_, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      "",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 2 * 1024 * 1024 * 1024},
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestControllerModifyVolumeMapping(t *testing.T) {
	cs, fk := setup(t)

	fk.modifyResp = &volumev1.ModifyVolumeResponse{}

	resp, err := cs.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId:          "bne/pve-1/local/vm-9997-pvc-abc",
		MutableParameters: map[string]string{"backup": "true", "diskIOPS": "1000"},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)

	require.NotNil(t, fk.modifyReq)
	assert.Equal(t, "bne/pve-1/local/vm-9997-pvc-abc", fk.modifyReq.VolumeId)
	assert.Equal(t, "true", fk.modifyReq.MutableParameters["backup"])
	assert.Equal(t, "1000", fk.modifyReq.MutableParameters["diskIOPS"])
}

func TestCreateSnapshotMapping(t *testing.T) {
	cs, fk := setup(t)

	fk.createSnapResp = &volumev1.CreateSnapshotResponse{
		SnapshotId:       "bne/pve-1/local/vm-9997-snap-test",
		ReadyToUse:       true,
		RestoreSizeBytes: 1024 * 1024 * 1024,
	}

	resp, err := cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "snap-test",
		SourceVolumeId: "bne/pve-1/local/vm-9997-pvc-abc",
		Parameters:     map[string]string{"zone": "pve-1"},
	})
	require.NoError(t, err)
	assert.Equal(t, "bne/pve-1/local/vm-9997-snap-test", resp.Snapshot.SnapshotId)
	assert.Equal(t, "bne/pve-1/local/vm-9997-pvc-abc", resp.Snapshot.SourceVolumeId)
	assert.True(t, resp.Snapshot.ReadyToUse)
	assert.Equal(t, int64(1024*1024*1024), resp.Snapshot.SizeBytes)

	require.NotNil(t, fk.createSnapReq)
	assert.Equal(t, "snap-test", fk.createSnapReq.Name)
	assert.Equal(t, "pvc-abc", fk.createSnapReq.SourceVolumeName)
	assert.Equal(t, "pve-1", fk.createSnapReq.Zone)
}

func TestDeleteSnapshotMapping(t *testing.T) {
	cs, fk := setup(t)

	fk.deleteSnapResp = &volumev1.DeleteSnapshotResponse{}

	resp, err := cs.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{
		SnapshotId: "bne/pve-1/local/vm-9997-snap-test",
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)

	require.NotNil(t, fk.deleteSnapReq)
	assert.Equal(t, "bne/pve-1/local/vm-9997-snap-test", fk.deleteSnapReq.SnapshotId)
}

func TestListSnapshotsMapping(t *testing.T) {
	cs, fk := setup(t)

	fk.listSnapResp = &volumev1.ListSnapshotsResponse{
		Entries: []*volumev1.ListSnapshotsResponse_Entry{
			{
				SnapshotId:       "bne/pve-1/local/vm-9997-snap-test",
				ReadyToUse:       true,
				RestoreSizeBytes: 1024 * 1024 * 1024,
			},
		},
	}

	resp, err := cs.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		SourceVolumeId: "bne/pve-1/local/vm-9997-pvc-abc",
	})
	require.NoError(t, err)
	require.Len(t, resp.Entries, 1)
	assert.Equal(t, "bne/pve-1/local/vm-9997-snap-test", resp.Entries[0].Snapshot.SnapshotId)

	require.NotNil(t, fk.listSnapReq)
	assert.Equal(t, "pvc-abc", fk.listSnapReq.SourceVolumeName)
}

func TestControllerGetCapabilitiesMatchesDirect(t *testing.T) {
	cs, _ := setup(t)

	resp, err := cs.ControllerGetCapabilities(context.Background(), &csi.ControllerGetCapabilitiesRequest{})
	require.NoError(t, err)

	// The remote controller must advertise the same capabilities as direct mode.
	// This test ensures the csi-resizer (EXPAND_VOLUME) and csi-snapshotter
	// (CREATE_DELETE_SNAPSHOT) sidecars do not crash-loop.
	capTypes := make(map[csi.ControllerServiceCapability_RPC_Type]bool)

	for _, cap := range resp.Capabilities {
		if rpc := cap.GetRpc(); rpc != nil {
			capTypes[rpc.Type] = true
		}
	}

	required := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_GET_CAPACITY,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
		csi.ControllerServiceCapability_RPC_GET_VOLUME,
		csi.ControllerServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
		csi.ControllerServiceCapability_RPC_MODIFY_VOLUME,
	}

	for _, r := range required {
		assert.True(t, capTypes[r], "capability %s must be advertised", r)
	}
}

func TestCreateVolumeFromSnapshot(t *testing.T) {
	cs, fk := setup(t)

	fk.createResp = &volumev1.CreateVolumeResponse{
		VolumeId:           "bne/pve-1/local-lvm/vm-9991-pvc-restored",
		CapacityBytes:      1073741824,
		AccessibleTopology: []string{"pve-1"},
	}

	req := &csi.CreateVolumeRequest{
		Name: "pvc-restored",
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1073741824},
		Parameters:    map[string]string{"storage": "local-lvm"},
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{
					SnapshotId: "bne/pve-1/local-lvm/vm-9991-snap-x",
				},
			},
		},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{{
				Segments: map[string]string{
					corev1.LabelTopologyRegion: "bne",
					corev1.LabelTopologyZone:   "pve-1",
				},
			}},
		},
	}

	resp, err := cs.CreateVolume(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, "bne/pve-1/local-lvm/vm-9991-pvc-restored", resp.Volume.VolumeId)

	require.NotNil(t, fk.createReq)
	require.NotNil(t, fk.createReq.Source)
	assert.Equal(t, "bne/pve-1/local-lvm/vm-9991-snap-x", fk.createReq.Source.SnapshotName)
	assert.Empty(t, fk.createReq.Source.VolumeName)
}

func TestCreateVolumeFromVolume(t *testing.T) {
	cs, fk := setup(t)

	fk.createResp = &volumev1.CreateVolumeResponse{
		VolumeId:           "bne/pve-1/local-lvm/vm-9991-pvc-cloned",
		CapacityBytes:      1073741824,
		AccessibleTopology: []string{"pve-1"},
	}

	req := &csi.CreateVolumeRequest{
		Name: "pvc-cloned",
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1073741824},
		Parameters:    map[string]string{"storage": "local-lvm"},
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Volume{
				Volume: &csi.VolumeContentSource_VolumeSource{
					VolumeId: "bne/pve-1/local-lvm/vm-9991-pvc-source",
				},
			},
		},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{{
				Segments: map[string]string{
					corev1.LabelTopologyRegion: "bne",
					corev1.LabelTopologyZone:   "pve-1",
				},
			}},
		},
	}

	resp, err := cs.CreateVolume(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, "bne/pve-1/local-lvm/vm-9991-pvc-cloned", resp.Volume.VolumeId)

	require.NotNil(t, fk.createReq)
	require.NotNil(t, fk.createReq.Source)
	assert.Equal(t, "pvc-source", fk.createReq.Source.VolumeName)
	assert.Empty(t, fk.createReq.Source.SnapshotName)
}
