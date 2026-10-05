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
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	volumev1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/volume/v1"
	volumeapi "github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/api"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// serveForTest runs the real server -- interceptor chain and service as
// NewServer wires them -- on an in-memory listener and returns a client.
func serveForTest(t *testing.T, issuer string) volumev1.VolumeServiceClient {
	t.Helper()

	storage := &v1alpha1.ProxmoxStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "bne.pve-1.local"},
		Spec:       v1alpha1.ProxmoxStorageSpec{Region: "bne", Zone: "pve-1", Storage: "local"},
		Status:     v1alpha1.ProxmoxStorageStatus{Active: true, AvailableBytes: 100, TotalBytes: 200},
	}

	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(admittedTenantWithIssuer(issuer), storage).
		WithStatusSubresource(&v1alpha1.TenantCluster{}, &v1alpha1.ProxmoxStorage{}, &v1alpha1.ProxmoxVolume{}, &v1alpha1.ProxmoxVolumeAttachment{}, &v1alpha1.ProxmoxVolumeSnapshot{}).
		WithIndex(&v1alpha1.ProxmoxVolume{}, volumeapi.VolumeIDIndex, volumeapi.IndexVolumeID).
		WithIndex(&v1alpha1.ProxmoxVolumeSnapshot{}, volumeapi.SnapshotIDIndex, volumeapi.IndexSnapshotID).
		Build()

	srv := volumeapi.NewServer(volumeapi.ServerConfig{Client: c, Rate: 100, Burst: 100})

	lis := bufconn.Listen(1 << 20)
	g := srv.GRPCServerForTest()

	go func() { _ = g.Serve(lis) }() //nolint:errcheck

	t.Cleanup(g.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	t.Cleanup(func() { _ = conn.Close() }) //nolint:errcheck

	return volumev1.NewVolumeServiceClient(conn)
}

// TestServerWatchCapacityThroughInterceptorChain is the path that crashed the
// operator on syd1: a streaming RPC through authn and audit. Both stream
// interceptors must hand the service on to the generated handler, which
// type-asserts it.
func TestServerWatchCapacityThroughInterceptorChain(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	cl := serveForTest(t, oidc.issuer())

	token := oidc.signToken(t, validClaims(oidc.issuer()))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

	stream, err := cl.WatchCapacity(ctx, &volumev1.WatchCapacityRequest{})
	require.NoError(t, err)

	resp, err := stream.Recv()
	require.NoError(t, err)
	assert.Equal(t, "local", resp.GetStorage())
	assert.Equal(t, int64(100), resp.GetAvailableBytes())
}

func TestServerRejectsUnauthenticatedStreamAndUnary(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	cl := serveForTest(t, oidc.issuer())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := cl.WatchCapacity(ctx, &volumev1.WatchCapacityRequest{})
	if err == nil {
		_, err = stream.Recv()
	}

	assert.Equal(t, codes.Unauthenticated, status.Code(err))

	_, err = cl.DeleteVolume(ctx, &volumev1.DeleteVolumeRequest{VolumeId: "bne/pve-1/local/vm-9997-pvc-x"})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestServerAttachVolumeNotFoundThroughInterceptorChain(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	cl := serveForTest(t, oidc.issuer())

	token := oidc.signToken(t, validClaims(oidc.issuer()))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

	// Volume doesn't exist, so should get NotFound.
	_, err := cl.AttachVolume(ctx, &volumev1.AttachVolumeRequest{
		VolumeId: "bne/pve-1/local/vm-9997-pvc-missing",
		NodeId:   "node1/411",
		Vmid:     411,
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestServerDetachVolumeIdempotentThroughInterceptorChain(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	cl := serveForTest(t, oidc.issuer())

	token := oidc.signToken(t, validClaims(oidc.issuer()))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

	// Attachment doesn't exist — should succeed (idempotent).
	resp, err := cl.DetachVolume(ctx, &volumev1.DetachVolumeRequest{
		VolumeId: "bne/pve-1/local/vm-9997-pvc-abc",
		NodeId:   "node1/411",
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestServerRejectsUnauthenticatedAttachDetach(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	cl := serveForTest(t, oidc.issuer())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := cl.AttachVolume(ctx, &volumev1.AttachVolumeRequest{
		VolumeId: "bne/pve-1/local/vm-9997-pvc-abc",
		NodeId:   "node1/411",
		Vmid:     411,
	})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))

	_, err = cl.DetachVolume(ctx, &volumev1.DetachVolumeRequest{
		VolumeId: "bne/pve-1/local/vm-9997-pvc-abc",
		NodeId:   "node1/411",
	})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestServerExpandVolumeNotFoundThroughInterceptorChain(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	cl := serveForTest(t, oidc.issuer())

	token := oidc.signToken(t, validClaims(oidc.issuer()))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

	// Volume doesn't exist, so should get NotFound.
	_, err := cl.ExpandVolume(ctx, &volumev1.ExpandVolumeRequest{
		VolumeId:      "bne/pve-1/local/vm-9997-pvc-missing",
		CapacityBytes: 2 * 1024 * 1024 * 1024,
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestServerModifyVolumeNotFoundThroughInterceptorChain(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	cl := serveForTest(t, oidc.issuer())

	token := oidc.signToken(t, validClaims(oidc.issuer()))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

	_, err := cl.ModifyVolume(ctx, &volumev1.ModifyVolumeRequest{
		VolumeId:          "bne/pve-1/local/vm-9997-pvc-missing",
		MutableParameters: map[string]string{"backup": "true"},
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestServerCreateSnapshotSourceNotFoundThroughInterceptorChain(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	cl := serveForTest(t, oidc.issuer())

	token := oidc.signToken(t, validClaims(oidc.issuer()))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

	// Source volume does not exist.
	_, err := cl.CreateSnapshot(ctx, &volumev1.CreateSnapshotRequest{
		Name:             "snap-test",
		SourceVolumeName: "pvc-nonexistent",
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestServerDeleteSnapshotIdempotentThroughInterceptorChain(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	cl := serveForTest(t, oidc.issuer())

	token := oidc.signToken(t, validClaims(oidc.issuer()))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

	// Snapshot doesn't exist -- should succeed (idempotent).
	resp, err := cl.DeleteSnapshot(ctx, &volumev1.DeleteSnapshotRequest{
		SnapshotId: "bne/pve-1/local/vm-9997-snap-missing",
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestServerListSnapshotsEmptyThroughInterceptorChain(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	cl := serveForTest(t, oidc.issuer())

	token := oidc.signToken(t, validClaims(oidc.issuer()))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

	resp, err := cl.ListSnapshots(ctx, &volumev1.ListSnapshotsRequest{})
	require.NoError(t, err)
	assert.Empty(t, resp.GetEntries())
}

func TestServerRejectsUnauthenticatedExpandModifySnapshot(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	cl := serveForTest(t, oidc.issuer())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := cl.ExpandVolume(ctx, &volumev1.ExpandVolumeRequest{
		VolumeId:      "bne/pve-1/local/vm-9997-pvc-abc",
		CapacityBytes: 2 * 1024 * 1024 * 1024,
	})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))

	_, err = cl.ModifyVolume(ctx, &volumev1.ModifyVolumeRequest{
		VolumeId:          "bne/pve-1/local/vm-9997-pvc-abc",
		MutableParameters: map[string]string{"backup": "true"},
	})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))

	_, err = cl.CreateSnapshot(ctx, &volumev1.CreateSnapshotRequest{
		Name:             "snap-test",
		SourceVolumeName: "pvc-test",
	})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))

	_, err = cl.DeleteSnapshot(ctx, &volumev1.DeleteSnapshotRequest{
		SnapshotId: "bne/pve-1/local/vm-9997-snap-test",
	})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))

	_, err = cl.ListSnapshots(ctx, &volumev1.ListSnapshotsRequest{})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}
