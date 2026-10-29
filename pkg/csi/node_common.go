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

package csi

import (
	"context"
	"fmt"
	"runtime"
	"strconv"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	toolsproxmox "github.com/sergelogvinov/proxmox-csi-plugin/pkg/tools/proxmox"
	utilsnode "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/node"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

var nodeCaps = []csi.NodeServiceCapability_RPC_Type{
	csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
	csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
	csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
}

var volumeCaps = []csi.VolumeCapability_AccessMode_Mode{
	csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
	csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
	csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
	csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER,
}

const (
	// VolumesPerNodeHardLimit is the technical limitation of the number of volumes that can be attached to a single node.
	// PVE allows 31 SCSI slots (scsi0-scsi30) and 16 virtio-blk slots (virtio0-virtio15) per VM, see `man qm`.
	// The driver uses scsi1-scsi30 and then every free virtio slot; the boot disk takes one of them.
	VolumesPerNodeHardLimit = toolsproxmox.MaxLun - 1

	// WindowsVolumesPerNodeHardLimit keeps a Windows node on the SCSI slots: it finds
	// its disks by SCSI WWN only.
	WindowsVolumesPerNodeHardLimit = toolsproxmox.MaxSCSIDisks - 1
)

// NodeGetCapabilities get the node capabilities
func (n *NodeService) NodeGetCapabilities(_ context.Context, _ *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	klog.V(4).InfoS("NodeGetCapabilities: called")

	caps := make([]*csi.NodeServiceCapability, 0, len(nodeCaps))

	for _, cap := range nodeCaps {
		c := &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: cap,
				},
			},
		}
		caps = append(caps, c)
	}

	return &csi.NodeGetCapabilitiesResponse{Capabilities: caps}, nil
}

// NodeGetInfo get the node info
func (n *NodeService) NodeGetInfo(ctx context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	klog.V(4).InfoS("NodeGetInfo: called")

	nodeID, err := utilsnode.ParseNodeID(n.nodeID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("failed to parse nodeID %s: %v", n.nodeID, err))
	}

	if _, err = nodeID.GetVMID(); err != nil {
		nodeIDSMBIOS, err := utilsnode.GetNodeID(nodeID.GetNodeName())
		if err != nil {
			klog.ErrorS(err, "NodeGetInfo: failed to get nodeID from SMBIOS, continuing with original nodeID", "nodeName", nodeID.GetNodeName())
		}

		nodeID = nodeIDSMBIOS
	}

	node, err := n.kclient.CoreV1().Nodes().Get(ctx, nodeID.GetNodeName(), metav1.GetOptions{})
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get node %s: %v", n.nodeID, err))
	}

	region, zone := GetNodeTopology(node.Labels)
	if region == "" || zone == "" {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get region or zone for node %s", n.nodeID))
	}

	return &csi.NodeGetInfoResponse{
		NodeId:            nodeID.String(),
		MaxVolumesPerNode: maxVolumes(node),
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{
				corev1.LabelTopologyRegion: region,
				corev1.LabelTopologyZone:   zone,
			},
		},
	}, nil
}

func isValidVolumeCapabilities(volCaps []*csi.VolumeCapability) bool {
	hasSupport := func(reqcap *csi.VolumeCapability) bool {
		for _, c := range volumeCaps {
			if c == reqcap.GetAccessMode().GetMode() {
				return true
			}
		}

		return false
	}

	foundAll := true

	for _, c := range volCaps {
		if !hasSupport(c) {
			foundAll = false
		}
	}

	return foundAll
}

func maxVolumes(node *corev1.Node) int64 {
	volumes, err := strconv.ParseInt(node.Labels[NodeLabelMaxVolumeAttachments], 10, 64)
	if err != nil {
		volumes = DefaultMaxVolumesPerNode
	}

	hardLimit := int64(VolumesPerNodeHardLimit)
	if runtime.GOOS == "windows" {
		hardLimit = WindowsVolumesPerNodeHardLimit
	}

	if volumes < 0 || volumes > hardLimit {
		klog.V(3).InfoS("Node has out of range value for max volume attachments, using default",
			"node", node.Name,
			"label", NodeLabelMaxVolumeAttachments,
			"value", volumes,
			"hardLimit", hardLimit,
			"default", DefaultMaxVolumesPerNode)

		return DefaultMaxVolumesPerNode
	}

	return volumes
}
