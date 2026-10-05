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

// Package remote implements a CSI ControllerServer that delegates volume
// operations to the operator's gRPC volume API.
//
// It must NOT import controller-runtime or pkg/operator -- the CSI binary's
// operator-isolation check enforces this.
package remote

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	volumev1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/volume/v1"
	utilsnode "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/node"
	volume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// remoteCaps is the set of capabilities the remote controller exposes.
// This must match direct mode's controllerCaps (pkg/csi/controller.go) so the
// CSI sidecars (csi-resizer, csi-snapshotter) do not crash-loop probing for
// capabilities the socket does not advertise.
var remoteCaps = []csi.ControllerServiceCapability_RPC_Type{
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

// ControllerServer is a CSI ControllerServer backed by the operator's volume API.
type ControllerServer struct {
	csi.UnimplementedControllerServer

	volumeClient volumev1.VolumeServiceClient

	// capacityCache stores the latest capacity data from WatchCapacity.
	mu       sync.RWMutex
	capacity map[string]*volumev1.WatchCapacityResponse
}

// NewControllerServer creates a new remote controller server.
func NewControllerServer(conn grpc.ClientConnInterface) *ControllerServer {
	return &ControllerServer{
		volumeClient: volumev1.NewVolumeServiceClient(conn),
		capacity:     make(map[string]*volumev1.WatchCapacityResponse),
	}
}

// StartCapacityWatch starts a background goroutine that watches capacity from
// the volume API and populates the local cache.
func (cs *ControllerServer) StartCapacityWatch(ctx context.Context) {
	go cs.watchCapacity(ctx)
}

// ControllerGetCapabilities returns the capabilities of the remote controller.
func (cs *ControllerServer) ControllerGetCapabilities(_ context.Context, _ *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	caps := make([]*csi.ControllerServiceCapability, 0, len(remoteCaps))

	for _, c := range remoteCaps {
		caps = append(caps, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: c,
				},
			},
		})
	}

	return &csi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}

// CreateVolume maps a CSI CreateVolume to the volume API's CreateVolume.
func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	klog.V(4).InfoS("CreateVolume: called", "name", req.GetName())

	pvc := req.GetName()
	if pvc == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeName must be provided")
	}

	if req.GetVolumeCapabilities() == nil {
		return nil, status.Error(codes.InvalidArgument, "VolumeCapabilities must be provided")
	}

	volSizeBytes := int64(0)
	if req.GetCapacityRange() != nil {
		volSizeBytes = req.GetCapacityRange().GetRequiredBytes()
	}

	params := req.GetParameters()
	if params == nil {
		params = map[string]string{}
	}

	region, zone := locationFromAccessibility(req.GetAccessibilityRequirements())
	if region == "" {
		return nil, status.Error(codes.Internal, "cannot determine region from topology")
	}

	storage := params["storage"]
	if storage == "" {
		return nil, status.Error(codes.InvalidArgument, "parameter storage must be provided")
	}

	// Build the proto request, stripping csi.storage.k8s.io/* keys.
	protoParams := make(map[string]string)

	for k, v := range params {
		if !strings.HasPrefix(k, "csi.storage.k8s.io/") && k != "storage" {
			protoParams[k] = v
		}
	}

	protoReq := &volumev1.CreateVolumeRequest{
		Name:              pvc,
		PvName:            params["csi.storage.k8s.io/pv/name"],
		Region:            region,
		Zone:              zone,
		Storage:           storage,
		CapacityBytes:     volSizeBytes,
		Parameters:        protoParams,
		MutableParameters: req.GetMutableParameters(),
	}

	// Claim ref from provisioner metadata.
	pvcName := params["csi.storage.k8s.io/pvc/name"]
	pvcNamespace := params["csi.storage.k8s.io/pvc/namespace"]

	if pvcName != "" || pvcNamespace != "" {
		protoReq.ClaimRef = &volumev1.ClaimRef{
			Name:      pvcName,
			Namespace: pvcNamespace,
		}
	}

	// Map CSI content sources to the API's VolumeSource.
	if contentSource := req.GetVolumeContentSource(); contentSource != nil {
		protoReq.Source = &volumev1.VolumeSource{}

		if snap := contentSource.GetSnapshot(); snap != nil {
			// The CSI sidecar sends the snapshot handle (snapshotID). The API
			// resolves the ProxmoxVolumeSnapshot CR by that handle.
			protoReq.Source.SnapshotName = snap.GetSnapshotId()
		}

		if vol := contentSource.GetVolume(); vol != nil {
			// Parse the volumeID to extract the PV name, which is the
			// ProxmoxVolume CR name.
			srcVol, err := volume.NewVolumeFromVolumeID(vol.GetVolumeId())
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "invalid source volume ID: %v", err)
			}

			pvName := srcVol.PV()
			if pvName == "" {
				pvName = vol.GetVolumeId()
			}

			protoReq.Source.VolumeName = pvName
		}
	}

	resp, err := cs.volumeClient.CreateVolume(ctx, protoReq)
	if err != nil {
		return nil, translateError(err)
	}

	topology := make([]*csi.Topology, 0, len(resp.AccessibleTopology))

	for _, z := range resp.AccessibleTopology {
		topology = append(topology, &csi.Topology{
			Segments: map[string]string{
				corev1.LabelTopologyRegion: region,
				corev1.LabelTopologyZone:   z,
			},
		})
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:           resp.VolumeId,
			CapacityBytes:      resp.CapacityBytes,
			VolumeContext:      resp.VolumeContext,
			AccessibleTopology: topology,
		},
	}, nil
}

// DeleteVolume maps a CSI DeleteVolume to the volume API's DeleteVolume.
//
//nolint:dupl // Structurally similar to DeleteSnapshot but different proto types and semantics.
func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	klog.V(4).InfoS("DeleteVolume: called", "volumeId", req.GetVolumeId())

	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId must be provided")
	}

	_, err := cs.volumeClient.DeleteVolume(ctx, &volumev1.DeleteVolumeRequest{
		VolumeId: req.GetVolumeId(),
	})
	if err != nil {
		return nil, translateError(err)
	}

	return &csi.DeleteVolumeResponse{}, nil
}

// GetCapacity serves capacity from the WatchCapacity cache.
func (cs *ControllerServer) GetCapacity(_ context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	klog.V(6).InfoS("GetCapacity: called")

	topology := req.GetAccessibleTopology()
	if topology == nil {
		return nil, status.Error(codes.InvalidArgument, "no topology specified")
	}

	segments := topology.GetSegments()
	zone := segments[corev1.LabelTopologyZone]
	storage := req.GetParameters()["storage"]

	if storage == "" {
		return nil, status.Error(codes.InvalidArgument, "storage parameter is required")
	}

	key := capacityKey(zone, storage)

	cs.mu.RLock()
	entry, ok := cs.capacity[key]
	cs.mu.RUnlock()

	if !ok {
		// No data yet; return zero.
		return &csi.GetCapacityResponse{AvailableCapacity: 0}, nil
	}

	return &csi.GetCapacityResponse{
		AvailableCapacity: entry.AvailableBytes,
	}, nil
}

// ControllerPublishVolume maps a CSI ControllerPublishVolume to the volume
// API's AttachVolume.
func (cs *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	klog.V(4).InfoS("ControllerPublishVolume: called", "volumeId", req.GetVolumeId(), "nodeId", req.GetNodeId())

	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId must be provided")
	}

	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "NodeId must be provided")
	}

	// Extract VMID from the node ID if present; otherwise pass 0 and let the
	// operator resolve it by VM name.
	var vmid int32

	if n, err := utilsnode.ParseNodeID(req.GetNodeId()); err == nil {
		if id, err := n.GetVMID(); err == nil && id != 0 {
			vmid = int32(id)
		}
	}

	protoReq := &volumev1.AttachVolumeRequest{
		VolumeId: req.GetVolumeId(),
		NodeId:   req.GetNodeId(),
		Vmid:     vmid,
		Readonly: req.GetReadonly(),
		// MutableParameters left empty: the operator's own record of the
		// volume carries the StorageClass options (cache, aio, ssd, backup,
		// throttles), and VolumeAttributesClass changes go through
		// ModifyVolume (Phase 3), not through the attach path.
	}

	resp, err := cs.volumeClient.AttachVolume(ctx, protoReq)
	if err != nil {
		return nil, translateError(err)
	}

	publishContext := map[string]string{
		"DevicePath": resp.DevicePath,
		"lun":        strconv.Itoa(int(resp.Lun)),
	}

	if resp.ResizeRequired {
		publishContext["resizeRequired"] = "true"
	}

	return &csi.ControllerPublishVolumeResponse{PublishContext: publishContext}, nil
}

// ControllerUnpublishVolume maps a CSI ControllerUnpublishVolume to the volume
// API's DetachVolume.
func (cs *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	klog.V(4).InfoS("ControllerUnpublishVolume: called", "volumeId", req.GetVolumeId(), "nodeId", req.GetNodeId())

	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId must be provided")
	}

	// Extract VMID from node ID if present.
	var vmid int32

	if req.GetNodeId() != "" {
		if n, err := utilsnode.ParseNodeID(req.GetNodeId()); err == nil {
			if id, err := n.GetVMID(); err == nil && id != 0 {
				vmid = int32(id)
			}
		}
	}

	_, err := cs.volumeClient.DetachVolume(ctx, &volumev1.DetachVolumeRequest{
		VolumeId: req.GetVolumeId(),
		NodeId:   req.GetNodeId(),
		Vmid:     vmid,
	})
	if err != nil {
		return nil, translateError(err)
	}

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// ControllerExpandVolume maps a CSI ControllerExpandVolume to the volume API's
// ExpandVolume.
func (cs *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	klog.V(4).InfoS("ControllerExpandVolume: called", "volumeId", req.GetVolumeId())

	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId must be provided")
	}

	capacityRange := req.GetCapacityRange()
	if capacityRange == nil {
		return nil, status.Error(codes.InvalidArgument, "CapacityRange must be provided")
	}

	resp, err := cs.volumeClient.ExpandVolume(ctx, &volumev1.ExpandVolumeRequest{
		VolumeId:      req.GetVolumeId(),
		CapacityBytes: capacityRange.GetRequiredBytes(),
	})
	if err != nil {
		return nil, translateError(err)
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         resp.CapacityBytes,
		NodeExpansionRequired: resp.NodeExpansionRequired,
	}, nil
}

// ControllerModifyVolume maps a CSI ControllerModifyVolume to the volume API's
// ModifyVolume.
func (cs *ControllerServer) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	klog.V(4).InfoS("ControllerModifyVolume: called", "volumeId", req.GetVolumeId())

	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId must be provided")
	}

	_, err := cs.volumeClient.ModifyVolume(ctx, &volumev1.ModifyVolumeRequest{
		VolumeId:          req.GetVolumeId(),
		MutableParameters: req.GetMutableParameters(),
	})
	if err != nil {
		return nil, translateError(err)
	}

	return &csi.ControllerModifyVolumeResponse{}, nil
}

// CreateSnapshot maps a CSI CreateSnapshot to the volume API's CreateSnapshot.
func (cs *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	klog.V(4).InfoS("CreateSnapshot: called", "name", req.GetName(), "sourceVolumeId", req.GetSourceVolumeId())

	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "Name must be provided")
	}

	sourceVolumeID := req.GetSourceVolumeId()
	if sourceVolumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "SourceVolumeId must be provided")
	}

	// Parse the volumeID to extract the PV name, which is the ProxmoxVolume
	// CR name on the operator side.
	srcVol, err := volume.NewVolumeFromVolumeID(sourceVolumeID)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid source volume ID: %v", err)
	}

	sourceVolumeName := srcVol.PV()
	if sourceVolumeName == "" {
		sourceVolumeName = sourceVolumeID
	}

	zone := ""
	if req.GetParameters() != nil {
		zone = req.GetParameters()["zone"]
	}

	resp, err := cs.volumeClient.CreateSnapshot(ctx, &volumev1.CreateSnapshotRequest{
		Name:             name,
		SourceVolumeName: sourceVolumeName,
		Zone:             zone,
	})
	if err != nil {
		return nil, translateError(err)
	}

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     resp.SnapshotId,
			SourceVolumeId: sourceVolumeID,
			CreationTime:   resp.CreationTime,
			SizeBytes:      resp.RestoreSizeBytes,
			ReadyToUse:     resp.ReadyToUse,
		},
	}, nil
}

// DeleteSnapshot maps a CSI DeleteSnapshot to the volume API's DeleteSnapshot.
//
//nolint:dupl // Structurally similar to DeleteVolume but different proto types and semantics.
func (cs *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	klog.V(4).InfoS("DeleteSnapshot: called", "snapshotId", req.GetSnapshotId())

	if req.GetSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "SnapshotId must be provided")
	}

	_, err := cs.volumeClient.DeleteSnapshot(ctx, &volumev1.DeleteSnapshotRequest{
		SnapshotId: req.GetSnapshotId(),
	})
	if err != nil {
		return nil, translateError(err)
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

// ListSnapshots maps a CSI ListSnapshots to the volume API's ListSnapshots.
func (cs *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	klog.V(4).InfoS("ListSnapshots: called")

	protoReq := &volumev1.ListSnapshotsRequest{
		SnapshotId:    req.GetSnapshotId(),
		MaxEntries:    req.GetMaxEntries(),
		StartingToken: req.GetStartingToken(),
	}

	// CSI sends source_volume_id; the API wants source_volume_name.
	if req.GetSourceVolumeId() != "" {
		srcVol, err := volume.NewVolumeFromVolumeID(req.GetSourceVolumeId())
		if err == nil && srcVol.PV() != "" {
			protoReq.SourceVolumeName = srcVol.PV()
		}
	}

	resp, err := cs.volumeClient.ListSnapshots(ctx, protoReq)
	if err != nil {
		return nil, translateError(err)
	}

	entries := make([]*csi.ListSnapshotsResponse_Entry, 0, len(resp.Entries))

	for _, e := range resp.Entries {
		entries = append(entries, &csi.ListSnapshotsResponse_Entry{
			Snapshot: &csi.Snapshot{
				SnapshotId:     e.SnapshotId,
				SourceVolumeId: e.SourceVolumeId,
				CreationTime:   e.CreationTime,
				SizeBytes:      e.RestoreSizeBytes,
				ReadyToUse:     e.ReadyToUse,
			},
		})
	}

	return &csi.ListSnapshotsResponse{
		Entries:   entries,
		NextToken: resp.NextToken,
	}, nil
}

// Reconnect delays for the capacity stream: doubled after each stream that
// ends without delivering anything, back to the minimum once one does.
const (
	watchBackoffFloor   = time.Second
	watchBackoffCeiling = time.Minute
)

func (cs *ControllerServer) watchCapacity(ctx context.Context) {
	backoff := watchBackoffFloor

	for {
		if cs.doWatch(ctx) {
			backoff = watchBackoffFloor
		} else {
			backoff = min(backoff*2, watchBackoffCeiling)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

// doWatch runs one capacity stream until it ends, and reports whether it
// delivered at least one update.
func (cs *ControllerServer) doWatch(ctx context.Context) bool {
	stream, err := cs.volumeClient.WatchCapacity(ctx, &volumev1.WatchCapacityRequest{})
	if err != nil {
		klog.ErrorS(err, "WatchCapacity: failed to start stream")

		return false
	}

	received := false

	for {
		resp, err := stream.Recv()
		if err != nil {
			if ctx.Err() == nil {
				klog.V(2).InfoS("WatchCapacity: stream ended", "err", err)
			}

			return received
		}

		received = true

		key := capacityKey(resp.Zone, resp.Storage)

		cs.mu.Lock()
		cs.capacity[key] = resp
		cs.mu.Unlock()
	}
}

func capacityKey(zone, storage string) string {
	return zone + "/" + storage
}

// locationFromAccessibility extracts region and zone from CSI topology requirements.
func locationFromAccessibility(tr *csi.TopologyRequirement) (region, zone string) {
	if tr == nil {
		return "", ""
	}

	for _, top := range tr.GetPreferred() {
		seg := top.GetSegments()

		r := seg[corev1.LabelTopologyRegion]
		z := seg[corev1.LabelTopologyZone]

		if r != "" && z != "" {
			return r, z
		}

		if r != "" && region == "" {
			region = r
		}
	}

	for _, top := range tr.GetRequisite() {
		seg := top.GetSegments()

		r := seg[corev1.LabelTopologyRegion]
		z := seg[corev1.LabelTopologyZone]

		if r != "" && z != "" {
			return r, z
		}

		if r != "" && region == "" {
			region = r
		}
	}

	return region, ""
}

// translateError passes through gRPC status codes 1:1 from the operator to the
// CSI sidecar, so Unavailable stays Unavailable and triggers the retry the
// sidecar knows how to do.
func translateError(err error) error {
	if err == nil {
		return nil
	}

	st, ok := status.FromError(err)
	if !ok {
		return status.Errorf(codes.Internal, "%v", err)
	}

	return status.Error(st.Code(), fmt.Sprintf("remote: %s", st.Message()))
}
