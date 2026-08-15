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

package csi_test

import (
	"context"
	"fmt"
	"maps"
	"testing"

	proto "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/helpers/ptr"
	testcluster "github.com/sergelogvinov/proxmox-csi-plugin/test/cluster"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientkubernetes "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
)

var _ proto.ControllerServer = (*csi.ControllerService)(nil)

type baseCSITestSuite struct {
	suite.Suite

	s *csi.ControllerService
	// kclient is the same fake the service holds, kept so a test can make an API
	// call fail on demand.
	kclient *fake.Clientset
}

type configTestCase struct {
	name   string
	config string
	// reassign mirrors features.reassignVolumeOnAttach in config, for the few tests
	// whose expected result depends on it.
	reassign bool
}

func getTestConfigs() []configTestCase {
	return []configTestCase{
		{
			name:   "CapMoxProvider",
			config: "../../test/config/cluster-config-2.yaml",
		},
		{
			name:   "DefaultProvider",
			config: "../../test/config/cluster-config-1.yaml",
		},
		{
			// Same cluster as DefaultProvider with reassignVolumeOnAttach on, so the
			// whole suite also runs against the suffix-resolution paths that flag turns on.
			name:     "ReassignVolumeOnAttach",
			config:   "../../test/config/cluster-config-3.yaml",
			reassign: true,
		},
	}
}

func (ts *baseCSITestSuite) setupTestSuite(config string) error {
	nodes := &corev1.NodeList{
		Items: []corev1.Node{
			{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Node",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "cluster-1-node-1",
				},
				Spec: corev1.NodeSpec{
					ProviderID: "proxmox://cluster-1/100",
				},
				Status: corev1.NodeStatus{
					NodeInfo: corev1.NodeSystemInfo{
						SystemUUID: "11833f4c-341f-4bd3-aad7-f7abed000000",
					},
				},
			},
			{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Node",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "cluster-1-node-2",
				},
				Spec: corev1.NodeSpec{
					ProviderID: "proxmox://cluster-1/101",
				},
				Status: corev1.NodeStatus{
					NodeInfo: corev1.NodeSystemInfo{
						SystemUUID: "11833f4c-341f-4bd3-aad7-f7abed000001",
					},
				},
			},
		},
	}

	pv := &corev1.PersistentVolumeList{
		Items: []corev1.PersistentVolume{
			{
				TypeMeta: metav1.TypeMeta{
					Kind:       "PersistentVolume",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "pvc-123",
				},
			},
			{
				TypeMeta: metav1.TypeMeta{
					Kind:       "PersistentVolume",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "pvc-error",
				},
			},
			{
				TypeMeta: metav1.TypeMeta{
					Kind:       "PersistentVolume",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:        "pvc-non-exist",
					Annotations: map[string]string{},
				},
			},
			{
				// A volume whose claim was given a VolumeAttributesClass long after the
				// volume was created, which is the only way the class can reach Proxmox:
				// the volume context the attach is configured from is frozen at
				// CreateVolume time and carries no trace of it.
				TypeMeta: metav1.TypeMeta{
					Kind:       "PersistentVolume",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "pvc-unpublished",
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							Driver:       csi.DriverName,
							VolumeHandle: "cluster-1/pve-1/local-lvm/vm-9999-pvc-unpublished",
						},
					},
					ClaimRef: &corev1.ObjectReference{
						Namespace: "default",
						Name:      "unpublished-claim",
					},
				},
			},
			{
				// The volume the unpublish tests detach from VM 101. Bound to a claim
				// so a failed rename back has somewhere to report itself; it names no
				// VolumeAttributesClass, so nothing else about it changes.
				TypeMeta: metav1.TypeMeta{
					Kind:       "PersistentVolume",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "pvc-reassigned",
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							Driver:       csi.DriverName,
							VolumeHandle: "cluster-1/pve-2/local-lvm/vm-9999-pvc-reassigned",
						},
					},
					ClaimRef: &corev1.ObjectReference{
						Namespace: "default",
						Name:      "reassigned-claim",
					},
				},
			},
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PersistentVolumeClaim",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "unpublished-claim",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:                "pvc-unpublished",
			VolumeAttributesClassName: ptr.Ptr("proxmox-throttle"),
		},
	}

	pvcReassigned := &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PersistentVolumeClaim",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "reassigned-claim",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pvc-reassigned",
		},
	}

	vac := &storagev1.VolumeAttributesClass{
		TypeMeta: metav1.TypeMeta{
			Kind:       "VolumeAttributesClass",
			APIVersion: "storage.k8s.io/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "proxmox-throttle",
		},
		DriverName: csi.DriverName,
		Parameters: map[string]string{
			csi.StorageDiskIOPSKey: "3000",
			csi.StorageDiskMBpsKey: "150",
		},
	}

	kclient := fake.NewClientset(nodes, pv, pvc, pvcReassigned, vac)

	px, err := csi.NewControllerService(kclient, config, "default")
	if err != nil {
		return fmt.Errorf("failed to create controller service: %v", err)
	}

	ts.s = px
	ts.kclient = kclient
	ts.s.Init()

	return nil
}

// TestSuiteCSI runs all test configurations
func TestSuiteCSI(t *testing.T) {
	configs := getTestConfigs()
	for _, cfg := range configs {
		// Create a new test suite for each configuration
		ts := &baseCSITestSuite{}

		// Run the suite with the current configuration
		suite.Run(t, &configuredTestSuite{
			baseCSITestSuite: ts,
			configCase:       cfg,
		})
	}
}

// configuredTestSuite wraps the base suite with a specific configuration
type configuredTestSuite struct {
	*baseCSITestSuite

	configCase configTestCase
}

func (ts *configuredTestSuite) SetupTest() {
	testcluster.SetupMockResponders()

	err := ts.setupTestSuite(ts.configCase.config)
	if err != nil {
		ts.T().Fatalf("Failed to setup test suite: %v", err)
	}
}

func TestNewControllerService(t *testing.T) {
	service, err := csi.NewControllerService(&clientkubernetes.Clientset{}, "fake-file", "default")
	assert.NotNil(t, err)
	assert.Nil(t, service)
	assert.Equal(t, "failed to read config: error reading fake-file: open fake-file: no such file or directory", err.Error())

	service, err = csi.NewControllerService(&clientkubernetes.Clientset{}, "../../hack/testdata/cloud-config.yaml", "default")
	assert.Nil(t, err)
	assert.NotNil(t, service)
}

//nolint:dupl
func (ts *configuredTestSuite) TestCreateVolume() {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	volcap := &proto.VolumeCapability{
		AccessMode: &proto.VolumeCapability_AccessMode{
			Mode: proto.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
		AccessType: &proto.VolumeCapability_Mount{
			Mount: &proto.VolumeCapability_MountVolume{
				FsType: "ext4",
			},
		},
	}
	volParam := map[string]string{
		"storage": "local-lvm",
	}
	volParamDefaults := map[string]string{
		"backup":    "0",
		"iothread":  "1",
		"storage":   "local-lvm",
		"replicate": "0",
	}
	volsize := &proto.CapacityRange{
		RequiredBytes: 1,
		LimitBytes:    100 * 1024 * 1024 * 1024,
	}
	topology := &proto.TopologyRequirement{
		Preferred: []*proto.Topology{
			{
				Segments: map[string]string{
					corev1.LabelTopologyRegion: "region",
					corev1.LabelTopologyZone:   "zone",
				},
			},
		},
	}

	tests := []struct {
		msg           string
		request       *proto.CreateVolumeRequest
		expected      *proto.CreateVolumeResponse
		expectedError error
	}{
		{
			msg: "EmptyVolumeName",
			request: &proto.CreateVolumeRequest{
				Name:                      "",
				VolumeCapabilities:        []*proto.VolumeCapability{volcap},
				Parameters:                volParam,
				CapacityRange:             volsize,
				AccessibilityRequirements: topology,
			},
			expectedError: status.Error(codes.InvalidArgument, "VolumeName must be provided"),
		},
		{
			msg: "VolumeCapabilities",
			request: &proto.CreateVolumeRequest{
				Name:                      "volume-id",
				Parameters:                volParam,
				CapacityRange:             volsize,
				AccessibilityRequirements: topology,
			},
			expectedError: status.Error(codes.InvalidArgument, "VolumeCapabilities must be provided"),
		},
		{
			msg: "VolumeParametersStorage",
			request: &proto.CreateVolumeRequest{
				Name:                      "volume-id",
				Parameters:                map[string]string{},
				VolumeCapabilities:        []*proto.VolumeCapability{volcap},
				CapacityRange:             volsize,
				AccessibilityRequirements: topology,
			},
			expectedError: status.Error(codes.InvalidArgument, "parameter storage must be provided"),
		},
		{
			msg: "VolumeParametersBlockSize",
			request: &proto.CreateVolumeRequest{
				Name: "volume-id",
				Parameters: map[string]string{
					"storage":   "local-lvm",
					"blockSize": "abc",
				},
				VolumeCapabilities:        []*proto.VolumeCapability{volcap},
				CapacityRange:             volsize,
				AccessibilityRequirements: topology,
			},
			expectedError: status.Error(codes.InvalidArgument, "parameters blockSize must be a number"),
		},
		{
			msg: "VolumeParametersInodeSize",
			request: &proto.CreateVolumeRequest{
				Name: "volume-id",
				Parameters: map[string]string{
					"storage":   "local-lvm",
					"inodeSize": "abc",
				},
				VolumeCapabilities:        []*proto.VolumeCapability{volcap},
				CapacityRange:             volsize,
				AccessibilityRequirements: topology,
			},
			expectedError: status.Error(codes.InvalidArgument, "parameters inodeSize must be a number"),
		},
		{
			msg: "RegionZone",
			request: &proto.CreateVolumeRequest{
				Name:               "volume-id",
				Parameters:         volParam,
				VolumeCapabilities: []*proto.VolumeCapability{volcap},
				CapacityRange:      volsize,
			},
			expectedError: status.Error(codes.Internal, "cannot find best region"),
		},
		{
			msg: "EmptyZone",
			request: &proto.CreateVolumeRequest{
				Name: "volume-id",
				Parameters: map[string]string{
					"storage": "fake-storage",
				},
				VolumeCapabilities: []*proto.VolumeCapability{volcap},
				CapacityRange:      volsize,
				AccessibilityRequirements: &proto.TopologyRequirement{
					Preferred: []*proto.Topology{
						{
							Segments: map[string]string{
								corev1.LabelTopologyRegion: "cluster-1",
							},
						},
					},
				},
			},
			expectedError: status.Error(codes.Internal, "failed to get zones with storage fake-storage: not found"),
		},
		{
			msg: "EmptyRegion",
			request: &proto.CreateVolumeRequest{
				Name:               "volume-id",
				Parameters:         volParam,
				VolumeCapabilities: []*proto.VolumeCapability{volcap},
				CapacityRange:      volsize,
				AccessibilityRequirements: &proto.TopologyRequirement{
					Preferred: []*proto.Topology{
						{
							Segments: map[string]string{
								corev1.LabelTopologyZone: "zone",
							},
						},
					},
				},
			},
			expectedError: status.Error(codes.Internal, "cannot find best region"),
		},
		{
			msg: "UnknownRegion",
			request: &proto.CreateVolumeRequest{
				Name:               "volume-id",
				Parameters:         volParam,
				VolumeCapabilities: []*proto.VolumeCapability{volcap},
				CapacityRange:      volsize,
				AccessibilityRequirements: &proto.TopologyRequirement{
					Preferred: []*proto.Topology{
						{
							Segments: map[string]string{
								corev1.LabelTopologyRegion: "unknown-region",
							},
						},
					},
				},
			},
			expectedError: status.Error(codes.Internal, "region not found"),
		},
		{
			msg: "NonSupportZonalSMB",
			request: &proto.CreateVolumeRequest{
				Name: "volume-smb",
				Parameters: map[string]string{
					"storage": "smb",
				},
				VolumeCapabilities: []*proto.VolumeCapability{volcap},
				CapacityRange:      volsize,
				AccessibilityRequirements: &proto.TopologyRequirement{
					Preferred: []*proto.Topology{
						{
							Segments: map[string]string{
								corev1.LabelTopologyRegion: "cluster-1",
								corev1.LabelTopologyZone:   "pve-1",
							},
						},
					},
				},
			},
			expectedError: status.Error(codes.Internal, "error: shared storage type cifs, pbs are not supported"),
		},
		{
			msg: "SupportZonalRBD",
			request: &proto.CreateVolumeRequest{
				Name: "volume-rbd",
				Parameters: map[string]string{
					"storage": "rbd",
				},
				VolumeCapabilities: []*proto.VolumeCapability{volcap},
				CapacityRange:      volsize,
				AccessibilityRequirements: &proto.TopologyRequirement{
					Preferred: []*proto.Topology{
						{
							Segments: map[string]string{
								corev1.LabelTopologyRegion: "cluster-1",
							},
						},
					},
				},
			},
			expected: &proto.CreateVolumeResponse{
				Volume: &proto.Volume{
					VolumeId: "cluster-1//rbd/9999/vm-9999-volume-rbd.raw",
					VolumeContext: func() map[string]string {
						vc := maps.Clone(volParamDefaults)
						vc["storage"] = "rbd"

						return vc
					}(),
					CapacityBytes: csi.MinChunkSizeBytes,
					AccessibleTopology: []*proto.Topology{
						{
							Segments: map[string]string{
								corev1.LabelTopologyRegion: "cluster-1",
							},
						},
					},
				},
			},
		},
		{
			msg: "PVCAlreadyExistSameSize",
			request: &proto.CreateVolumeRequest{
				Name:               "pvc-exist-same-size",
				Parameters:         volParam,
				VolumeCapabilities: []*proto.VolumeCapability{volcap},
				CapacityRange:      volsize,
				AccessibilityRequirements: &proto.TopologyRequirement{
					Preferred: []*proto.Topology{
						{
							Segments: map[string]string{
								corev1.LabelTopologyRegion: "cluster-1",
								corev1.LabelTopologyZone:   "pve-1",
							},
						},
					},
				},
			},
			expected: &proto.CreateVolumeResponse{
				Volume: &proto.Volume{
					VolumeId:      "cluster-1/pve-1/local-lvm/vm-9999-pvc-exist-same-size",
					VolumeContext: volParamDefaults,
					CapacityBytes: csi.MinChunkSizeBytes,
					AccessibleTopology: []*proto.Topology{
						{
							Segments: map[string]string{
								corev1.LabelTopologyRegion: "cluster-1",
								corev1.LabelTopologyZone:   "pve-1",
							},
						},
					},
				},
			},
		},
		{
			msg: "CreateVolume",
			request: &proto.CreateVolumeRequest{
				Name:               "pvc-123",
				Parameters:         volParam,
				VolumeCapabilities: []*proto.VolumeCapability{volcap},
				CapacityRange:      volsize,
				AccessibilityRequirements: &proto.TopologyRequirement{
					Preferred: []*proto.Topology{
						{
							Segments: map[string]string{
								corev1.LabelTopologyRegion: "cluster-1",
								corev1.LabelTopologyZone:   "pve-1",
							},
						},
					},
				},
			},
			expected: &proto.CreateVolumeResponse{
				Volume: &proto.Volume{
					VolumeId:      "cluster-1/pve-1/local-lvm/vm-9999-pvc-123",
					VolumeContext: volParamDefaults,
					CapacityBytes: csi.MinChunkSizeBytes,
					AccessibleTopology: []*proto.Topology{
						{
							Segments: map[string]string{
								corev1.LabelTopologyRegion: "cluster-1",
								corev1.LabelTopologyZone:   "pve-1",
							},
						},
					},
				},
			},
		},
	}

	for _, testCase := range tests {
		ts.Run(fmt.Sprint(testCase.msg), func() {
			resp, err := ts.s.CreateVolume(context.Background(), testCase.request)
			if testCase.expectedError == nil {
				ts.Require().NoError(err)
				ts.Require().Equal(resp, testCase.expected)
			} else {
				ts.Require().Error(err)
				ts.Require().Equal(err, testCase.expectedError)
			}
		})
	}
}

//nolint:dupl
func (ts *configuredTestSuite) TestDeleteVolume() {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	tests := []struct {
		msg           string
		request       *proto.DeleteVolumeRequest
		expected      *proto.DeleteVolumeResponse
		expectedError error
	}{
		{
			msg: "VolumeID",
			request: &proto.DeleteVolumeRequest{
				VolumeId: "volume-id",
			},
			expectedError: status.Error(codes.InvalidArgument, "VolumeID must be in the format of region/zone/storageName/diskName"),
		},
		{
			msg: "WrongCluster",
			request: &proto.DeleteVolumeRequest{
				VolumeId: "fake-region/node/data/volume-id",
			},
			expectedError: status.Error(codes.Internal, "region not found"),
		},
		{
			msg: "WrongPVZone",
			request: &proto.DeleteVolumeRequest{
				VolumeId: "cluster-1/pve-removed/local-lvm/vm-9999-pvc-non-exist",
			},
			expected: &proto.DeleteVolumeResponse{},
		},
		{
			msg: "VolumeIDNonExist",
			request: &proto.DeleteVolumeRequest{
				VolumeId: "cluster-1/pve-1/wrong-volume/vm-9999-pvc-non-exist",
			},
			expected: &proto.DeleteVolumeResponse{},
		},
		{
			msg: "PVCNonExist",
			request: &proto.DeleteVolumeRequest{
				VolumeId: "cluster-1/pve-1/local-lvm/vm-9999-pvc-non-exist",
			},
			expected: &proto.DeleteVolumeResponse{},
		},
		{
			msg: "DeleteVolume",
			request: &proto.DeleteVolumeRequest{
				VolumeId: "cluster-1/pve-1/local-lvm/vm-9999-pvc-123",
			},
			expected: &proto.DeleteVolumeResponse{},
		},
		{
			msg: "DeleteVolumeError",
			request: &proto.DeleteVolumeRequest{
				VolumeId: "cluster-1/pve-1/local-lvm/vm-9999-pvc-error",
			},
			expectedError: status.Error(codes.Internal, "failed to delete volume: cluster-1/pve-1/local-lvm/vm-9999-pvc-error, unable to delete virtual machine disk: ERROR"),
		},
	}

	for _, testCase := range tests {
		ts.Run(fmt.Sprint(testCase.msg), func() {
			resp, err := ts.s.DeleteVolume(context.Background(), testCase.request)
			if testCase.expectedError == nil {
				ts.Require().NoError(err)
				ts.Require().Equal(resp, testCase.expected)
			} else {
				ts.Require().Error(err)
				ts.Require().Equal(testCase.expectedError, err)
			}
		})
	}
}

func (ts *configuredTestSuite) TestControllerServiceControllerGetCapabilities() {
	resp, err := ts.s.ControllerGetCapabilities(context.Background(), &proto.ControllerGetCapabilitiesRequest{})
	ts.Require().NoError(err)
	ts.Require().NotNil(resp)

	if len(resp.GetCapabilities()) != 9 {
		ts.T().Fatalf("unexpected number of capabilities: %d", len(resp.GetCapabilities()))
	}
}

//nolint:dupl
func (ts *configuredTestSuite) TestControllerPublishVolumeError() {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	volCap := &proto.VolumeCapability{
		AccessMode: &proto.VolumeCapability_AccessMode{
			Mode: proto.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
		AccessType: &proto.VolumeCapability_Mount{
			Mount: &proto.VolumeCapability_MountVolume{
				FsType: "ext4",
			},
		},
	}
	volCtx := map[string]string{
		csi.StorageIDKey: "local-lvm",
	}

	tests := []struct {
		msg           string
		request       *proto.ControllerPublishVolumeRequest
		expected      *proto.ControllerPublishVolumeResponse
		expectedError error
	}{
		{
			msg: "NodeID",
			request: &proto.ControllerPublishVolumeRequest{
				VolumeId:         "volume-id",
				VolumeCapability: volCap,
				VolumeContext:    volCtx,
			},
			expectedError: status.Error(codes.InvalidArgument, "NodeID must be in format <nodeName>/<vmID> or <nodeName>"),
		},
		{
			msg: "VolumeCapability",
			request: &proto.ControllerPublishVolumeRequest{
				NodeId:        "node-id",
				VolumeId:      "volume-id",
				VolumeContext: volCtx,
			},
			expectedError: status.Error(codes.InvalidArgument, "VolumeCapability must be provided"),
		},
		{
			msg: "WrongVolumeID",
			request: &proto.ControllerPublishVolumeRequest{
				NodeId:           "node-id",
				VolumeId:         "volume-id",
				VolumeCapability: volCap,
				VolumeContext:    volCtx,
			},
			expectedError: status.Error(codes.InvalidArgument, "VolumeID must be in the format of region/zone/storageName/diskName"),
		},
		{
			msg: "WrongCluster",
			request: &proto.ControllerPublishVolumeRequest{
				NodeId:           "node-id",
				VolumeId:         "fake-region/node-id/data/volume-id",
				VolumeCapability: volCap,
				VolumeContext:    volCtx,
			},
			expectedError: status.Error(codes.Internal, "region not found"),
		},
		// {
		// 	msg: "WrongNode",
		// 	request: &proto.ControllerPublishVolumeRequest{
		// 		NodeId:           "cluster-1-node-2",
		// 		VolumeId:         "cluster-1/pve-1/local-lvm/vm-9999-pvc-123",
		// 		VolumeCapability: volCap,
		// 		VolumeContext:    volCtx,
		// 		Readonly:         true,
		// 	},
		// 	expectedError: status.Error(codes.InvalidArgument, "volume cluster-1/pve-1/local-lvm/vm-9999-pvc-123 does not exist on the node cluster-1-node-2"),
		// },
		{
			msg: "VolumeNotExist",
			request: &proto.ControllerPublishVolumeRequest{
				NodeId:           "cluster-1-node-1",
				VolumeId:         "cluster-1/pve-1/local-lvm/vm-9999-pvc-123-not-exist",
				VolumeCapability: volCap,
				VolumeContext:    volCtx,
			},
			expectedError: status.Error(codes.NotFound, "volume cluster-1/pve-1/local-lvm/vm-9999-pvc-123-not-exist not found"),
		},
		{
			msg: "VolumeAlreadyAttached",
			request: &proto.ControllerPublishVolumeRequest{
				NodeId:           "cluster-1-node-1",
				VolumeId:         "cluster-1/pve-1/local-lvm/vm-9999-pvc-123",
				VolumeCapability: volCap,
				VolumeContext:    volCtx,
			},
			expected: &proto.ControllerPublishVolumeResponse{
				PublishContext: map[string]string{
					"DevicePath": "/dev/disk/by-id/wwn-0x5056432d49443031",
					"lun":        "1",
				},
			},
		},
	}

	for _, testCase := range tests {
		ts.Run(fmt.Sprint(testCase.msg), func() {
			resp, err := ts.s.ControllerPublishVolume(context.Background(), testCase.request)
			if testCase.expectedError == nil {
				ts.Require().NoError(err)
				ts.Require().Equal(resp, testCase.expected)
			} else {
				ts.Require().Error(err)
				ts.Require().Equal(testCase.expectedError, err)
			}
		})
	}
}

// TestControllerPublishVolumeAppliesVolumeAttributesClass covers the case the
// modify RPC cannot: a class assigned to a claim whose volume already existed.
// Nothing about it is in the volume context, so if the attach did not resolve
// the class itself the throttle would never be written at all.
func (ts *configuredTestSuite) TestControllerPublishVolumeAppliesVolumeAttributesClass() {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	resp, err := ts.s.ControllerPublishVolume(context.Background(), &proto.ControllerPublishVolumeRequest{
		NodeId:   "cluster-1-node-1",
		VolumeId: "cluster-1/pve-1/local-lvm/vm-9999-pvc-unpublished",
		VolumeCapability: &proto.VolumeCapability{
			AccessMode: &proto.VolumeCapability_AccessMode{
				Mode: proto.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
			AccessType: &proto.VolumeCapability_Mount{
				Mount: &proto.VolumeCapability_MountVolume{FsType: "ext4"},
			},
		},
		VolumeContext: map[string]string{csi.StorageIDKey: "local-lvm"},
	})
	ts.Require().NoError(err)
	ts.Require().NotNil(resp)

	attached := testcluster.AttachRequests()
	ts.Require().Len(attached, 1)

	options := attached[0].Options()
	ts.Require().Equal("3000", options["iops_rd"])
	ts.Require().Equal("3000", options["iops_wr"])
	ts.Require().Equal("150", options["mbps_rd"])
	ts.Require().Equal("150", options["mbps_wr"])

	// The class does not set these, so the volume context still decides them.
	ts.Require().Equal("1", options["iothread"])
	ts.Require().Equal("0", options["backup"])
}

// TestControllerPublishVolumeSurvivesVolumeAttributesClassLookupFailure: the
// class is only ever an enrichment of the attach, never a precondition for it.
func (ts *configuredTestSuite) TestControllerPublishVolumeSurvivesVolumeAttributesClassLookupFailure() {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	ts.kclient.PrependReactor("get", "volumeattributesclasses",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, fmt.Errorf("apiserver is unavailable")
		})

	resp, err := ts.s.ControllerPublishVolume(context.Background(), &proto.ControllerPublishVolumeRequest{
		NodeId:   "cluster-1-node-1",
		VolumeId: "cluster-1/pve-1/local-lvm/vm-9999-pvc-unpublished",
		VolumeCapability: &proto.VolumeCapability{
			AccessMode: &proto.VolumeCapability_AccessMode{
				Mode: proto.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
			AccessType: &proto.VolumeCapability_Mount{
				Mount: &proto.VolumeCapability_MountVolume{FsType: "ext4"},
			},
		},
		VolumeContext: map[string]string{csi.StorageIDKey: "local-lvm"},
	})
	ts.Require().NoError(err)
	ts.Require().NotNil(resp)

	attached := testcluster.AttachRequests()
	ts.Require().Len(attached, 1)

	options := attached[0].Options()
	ts.Require().NotContains(options, "iops_rd")
	ts.Require().NotContains(options, "mbps_rd")
}

// TestControllerPublishVolumeEventsFailedReassignment: a rename that fails is
// deliberately non-fatal, so the attach it happens under reports success and the
// volume works — which is exactly why it needs to say so somewhere an operator
// looks. A live fleet ran for a week with this rename failing on three nodes out
// of four and the only trace was one controller log line.
func (ts *configuredTestSuite) TestControllerPublishVolumeEventsFailedReassignment() {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.FailRenames("source volume 'local-lvm:9999/vm-9999-pvc-unpublished' not found")

	events := ts.recordEvents()

	resp, err := ts.publishVolume()
	ts.Require().NoError(err)
	ts.Require().NotNil(resp)
	ts.Require().Len(testcluster.AttachRequests(), 1)

	if !ts.configCase.reassign {
		// Nothing was renamed, so there is nothing to report.
		ts.Require().Empty(testcluster.RenameRequests())
		ts.Require().Empty(events())

		return
	}

	ts.Require().Len(testcluster.RenameRequests(), 1)

	reasons := events()
	ts.Require().Len(reasons, 1)
	ts.Require().Contains(reasons[0], corev1.EventTypeWarning)
	ts.Require().Contains(reasons[0], "ReassignVolumeFailed")
	ts.Require().Contains(reasons[0], "not found")
}

// TestControllerPublishVolumeSilentOnSuccessfulReassignment: the event is a
// failure signal, so a working cluster must not accumulate one per attach.
func (ts *configuredTestSuite) TestControllerPublishVolumeSilentOnSuccessfulReassignment() {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	events := ts.recordEvents()

	resp, err := ts.publishVolume()
	ts.Require().NoError(err)
	ts.Require().NotNil(resp)

	if ts.configCase.reassign {
		ts.Require().Len(testcluster.RenameRequests(), 1)
	}

	ts.Require().Empty(events())
}

// TestControllerPublishVolumeSurvivesFlakyAPI: a Proxmox API that answers a
// couple of reads badly must not fail an attach.
//
// This is the live failure the retrying transport was written for. A burst of
// controller calls provoked 33 "unexpected end of JSON input" errors in a
// fourteen-second window — an empty-bodied 596 from pveproxy, which the client
// library reported as a JSON parse error with the status code thrown away. One
// such read anywhere in the per-VM scan aborted the whole lookup, because
// GetVMByFilter returns on the first callback error.
func (ts *configuredTestSuite) TestControllerPublishVolumeSurvivesFlakyAPI() {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.FailNextReads(2)

	resp, err := ts.publishVolume()
	ts.Require().NoError(err)
	ts.Require().NotNil(resp)
	ts.Require().Len(testcluster.AttachRequests(), 1)
}

// TestControllerUnpublishVolumeEventsFailedRenameBack: a volume left on the
// target VM's name is still resolved by suffix and adopted on its next attach,
// so the detach succeeds — but until then the PV's volumeHandle names something
// that is not there, which is worth surfacing under its own reason.
func (ts *configuredTestSuite) TestControllerUnpublishVolumeEventsFailedRenameBack() {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.FailRenames("source volume 'local-lvm:101/vm-101-pvc-reassigned' not found")

	events := ts.recordEvents()

	_, err := ts.s.ControllerUnpublishVolume(context.Background(), &proto.ControllerUnpublishVolumeRequest{
		NodeId:   "cluster-1-node-2",
		VolumeId: "cluster-1/pve-2/local-lvm/vm-9999-pvc-reassigned",
	})
	ts.Require().NoError(err)

	if !ts.configCase.reassign {
		ts.Require().Empty(testcluster.RenameRequests())
		ts.Require().Empty(events())

		return
	}

	ts.Require().Len(testcluster.RenameRequests(), 1)

	reasons := events()
	ts.Require().Len(reasons, 1)
	ts.Require().Contains(reasons[0], corev1.EventTypeWarning)
	ts.Require().Contains(reasons[0], "ReassignVolumeBackFailed")
}

// TestControllerPublishVolumeSurvivesUnreportableFailure: emitting the event
// must be as incapable of failing the attach as the rename it reports. A volume
// whose claim cannot be read has nothing to attach the event to.
func (ts *configuredTestSuite) TestControllerPublishVolumeSurvivesUnreportableFailure() {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.FailRenames("source volume not found")

	ts.kclient.PrependReactor("get", "persistentvolumeclaims",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, fmt.Errorf("apiserver is unavailable")
		})

	events := ts.recordEvents()

	resp, err := ts.publishVolume()
	ts.Require().NoError(err)
	ts.Require().NotNil(resp)
	ts.Require().Len(testcluster.AttachRequests(), 1)
	ts.Require().Empty(events())
}

// TestControllerModifyVolumeUnpublished: a detached volume has no VM config
// entry to carry a throttle, so there is nothing to write and nothing to wait
// for — ControllerPublishVolume applies the class when the volume next attaches.
// Reporting NotFound here instead left external-resizer retrying idle volumes
// for as long as they stayed idle.
func (ts *configuredTestSuite) TestControllerModifyVolumeUnpublished() {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	resp, err := ts.s.ControllerModifyVolume(context.Background(), &proto.ControllerModifyVolumeRequest{
		VolumeId: "cluster-1/pve-1/local-lvm/vm-9999-pvc-unpublished",
		MutableParameters: map[string]string{
			csi.StorageDiskIOPSKey: "3000",
			csi.StorageDiskMBpsKey: "150",
		},
	})
	ts.Require().NoError(err)
	ts.Require().NotNil(resp)
	ts.Require().Empty(testcluster.AttachRequests())
}

func (ts *configuredTestSuite) TestControllerModifyVolumeError() {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	tests := []struct {
		msg           string
		request       *proto.ControllerModifyVolumeRequest
		expectedError error
	}{
		{
			msg:           "VolumeID",
			request:       &proto.ControllerModifyVolumeRequest{},
			expectedError: status.Error(codes.InvalidArgument, "VolumeID must be provided"),
		},
		{
			msg: "BadParameters",
			request: &proto.ControllerModifyVolumeRequest{
				VolumeId:          "cluster-1/pve-1/local-lvm/vm-9999-pvc-123",
				MutableParameters: map[string]string{csi.StorageDiskIOPSKey: "many"},
			},
			expectedError: status.Error(codes.InvalidArgument, "parameters diskIOPS must be a number"),
		},
		{
			msg: "WrongVolumeID",
			request: &proto.ControllerModifyVolumeRequest{
				VolumeId: "volume-id",
			},
			expectedError: status.Error(codes.InvalidArgument, "VolumeID must be in the format of region/zone/storageName/diskName"),
		},
		{
			msg: "WrongCluster",
			request: &proto.ControllerModifyVolumeRequest{
				VolumeId: "fake-region/pve-1/local-lvm/vm-9999-pvc-123",
			},
			expectedError: status.Error(codes.Internal, "region not found"),
		},
		{
			msg: "VolumeNotExist",
			request: &proto.ControllerModifyVolumeRequest{
				VolumeId: "cluster-1/pve-1/local-lvm/vm-9999-pvc-none",
			},
			expectedError: status.Error(codes.NotFound, "volume cluster-1/pve-1/local-lvm/vm-9999-pvc-none not found"),
		},
	}

	for _, testCase := range tests {
		ts.Run(fmt.Sprint(testCase.msg), func() {
			_, err := ts.s.ControllerModifyVolume(context.Background(), testCase.request)
			ts.Require().Error(err)
			ts.Require().Equal(testCase.expectedError, err)
		})
	}
}

//nolint:dupl
func (ts *configuredTestSuite) TestControllerUnpublishVolumeError() {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	tests := []struct {
		msg           string
		request       *proto.ControllerUnpublishVolumeRequest
		expectedError error
		// expectedRename is the rename back to the placeholder vmid the detach must
		// ask proxmod for, on the configs that have reassignVolumeOnAttach on.
		expectedRename *testcluster.RenameRequest
	}{
		{
			msg: "NodeID",
			request: &proto.ControllerUnpublishVolumeRequest{
				VolumeId: "volume-id",
			},
			expectedError: status.Error(codes.InvalidArgument, "NodeID must be in format <nodeName>/<vmID> or <nodeName>"),
		},
		{
			msg: "WrongVolumeID",
			request: &proto.ControllerUnpublishVolumeRequest{
				NodeId:   "node-id",
				VolumeId: "volume-id",
			},
			expectedError: status.Error(codes.InvalidArgument, "VolumeID must be in the format of region/zone/storageName/diskName"),
		},
		{
			msg: "WrongCluster",
			request: &proto.ControllerUnpublishVolumeRequest{
				NodeId:   "node-id",
				VolumeId: "fake-region/node/data/volume-id",
			},
			expectedError: status.Error(codes.Internal, "region not found"),
		},
		{
			msg: "WrongPVZone",
			request: &proto.ControllerUnpublishVolumeRequest{
				NodeId:   "cluster-1-node-2",
				VolumeId: "cluster-1/pve-removed/local-lvm/vm-9999-pvc-exist",
			},
		},
		{
			msg: "AlreadyDetached",
			request: &proto.ControllerUnpublishVolumeRequest{
				NodeId:   "cluster-1-node-2",
				VolumeId: "cluster-1/pve-1/local-lvm/vm-9999-pvc-123",
			},
		},
		{
			// A volume reassignVolumeOnAttach renamed onto VM 101 on the way in.
			// Detaching it must rename it back to the placeholder vmid the
			// VolumeID carries, so the handle is valid again at rest.
			msg: "UnpublishReassigned",
			request: &proto.ControllerUnpublishVolumeRequest{
				NodeId:   "cluster-1-node-2",
				VolumeId: "cluster-1/pve-2/local-lvm/vm-9999-pvc-reassigned",
			},
			expectedRename: &testcluster.RenameRequest{
				Node:          "pve-2",
				Storage:       "local-lvm",
				Volume:        "vm-101-pvc-reassigned",
				TargetVMID:    9999,
				TargetVolname: "vm-9999-pvc-reassigned",
			},
		},
	}

	for _, testCase := range tests {
		ts.Run(fmt.Sprint(testCase.msg), func() {
			testcluster.ResetRenameRequests()

			_, err := ts.s.ControllerUnpublishVolume(context.Background(), testCase.request)
			if testCase.expectedError == nil {
				ts.Require().NoError(err)
			} else {
				ts.Require().Error(err)
				ts.Require().Equal(testCase.expectedError.Error(), err.Error())
			}

			if testCase.expectedRename != nil && ts.configCase.reassign {
				ts.Require().Equal([]testcluster.RenameRequest{*testCase.expectedRename}, testcluster.RenameRequests())
			} else {
				ts.Require().Empty(testcluster.RenameRequests())
			}
		})
	}
}

func (ts *configuredTestSuite) TestValidateVolumeCapabilities() {
	_, err := ts.s.ValidateVolumeCapabilities(context.Background(), &proto.ValidateVolumeCapabilitiesRequest{})
	ts.Require().Error(err)
	ts.Require().Equal(status.Error(codes.Unimplemented, ""), err)
}

func (ts *configuredTestSuite) TestListVolumes() {
	_, err := ts.s.ListVolumes(context.Background(), &proto.ListVolumesRequest{})
	ts.Require().Error(err)
	ts.Require().Equal(status.Error(codes.Unimplemented, ""), err)
}

func (ts *configuredTestSuite) TestGetCapacity() {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	tests := []struct {
		msg           string
		request       *proto.GetCapacityRequest
		expected      *proto.GetCapacityResponse
		expectedError error
	}{
		{
			msg:           "NoTopology",
			request:       &proto.GetCapacityRequest{},
			expectedError: status.Error(codes.InvalidArgument, "no topology specified"),
		},
		{
			msg: "NoTopology",
			request: &proto.GetCapacityRequest{
				AccessibleTopology: &proto.Topology{},
			},
			expectedError: status.Error(codes.InvalidArgument, "region and storage must be provided"),
		},
		{
			msg: "TopologyRegion",
			request: &proto.GetCapacityRequest{
				AccessibleTopology: &proto.Topology{
					Segments: map[string]string{
						corev1.LabelTopologyRegion: "region",
					},
				},
				Parameters: map[string]string{
					csi.StorageIDKey: "local-lvm",
				},
			},
			expectedError: status.Error(codes.Internal, "region not found"),
		},
		{
			msg: "TopologyZone",
			request: &proto.GetCapacityRequest{
				AccessibleTopology: &proto.Topology{
					Segments: map[string]string{
						corev1.LabelTopologyZone: "zone",
					},
				},
				Parameters: map[string]string{
					csi.StorageIDKey: "local-lvm",
				},
			},
			expectedError: status.Error(codes.InvalidArgument, "region and storage must be provided"),
		},
		{
			msg: "TopologyStorageName",
			request: &proto.GetCapacityRequest{
				AccessibleTopology: &proto.Topology{
					Segments: map[string]string{
						corev1.LabelTopologyRegion: "region",
						corev1.LabelTopologyZone:   "zone",
					},
				},
			},
			expectedError: status.Error(codes.InvalidArgument, "region and storage must be provided"),
		},
		{
			msg: "Topology",
			request: &proto.GetCapacityRequest{
				AccessibleTopology: &proto.Topology{
					Segments: map[string]string{
						corev1.LabelTopologyRegion: "region",
						corev1.LabelTopologyZone:   "zone",
					},
				},
				Parameters: map[string]string{
					csi.StorageIDKey: "local-lvm",
				},
			},
			expectedError: status.Error(codes.Internal, "region not found"),
		},
		{
			msg: "StorageNotExists",
			request: &proto.GetCapacityRequest{
				AccessibleTopology: &proto.Topology{
					Segments: map[string]string{
						corev1.LabelTopologyRegion: "cluster-1",
						corev1.LabelTopologyZone:   "pve-1",
					},
				},
				Parameters: map[string]string{
					csi.StorageIDKey: "storage",
				},
			},
			expectedError: status.Error(codes.Internal, "not found"),
		},
		{
			msg: "Storage",
			request: &proto.GetCapacityRequest{
				AccessibleTopology: &proto.Topology{
					Segments: map[string]string{
						corev1.LabelTopologyRegion: "cluster-1",
						corev1.LabelTopologyZone:   "pve-1",
					},
				},
				Parameters: map[string]string{
					csi.StorageIDKey: "local-lvm",
				},
			},
			expected: &proto.GetCapacityResponse{
				AvailableCapacity: 50 * 1024 * 1024 * 1024,
			},
		},
	}

	for _, testCase := range tests {
		ts.Run(fmt.Sprint(testCase.msg), func() {
			resp, err := ts.s.GetCapacity(context.Background(), testCase.request)
			if testCase.expectedError == nil {
				ts.Require().NoError(err)
				ts.Require().Equal(testCase.expected, resp)
			} else {
				ts.Require().Error(err)
				ts.Require().Equal(testCase.expectedError, err)
			}
		})
	}
}

func (ts *configuredTestSuite) TestCreateSnapshot() {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	tests := []struct {
		msg           string
		request       *proto.CreateSnapshotRequest
		expected      *proto.CreateSnapshotResponse
		expectedError error
	}{
		{
			msg:           "VolumeID",
			request:       &proto.CreateSnapshotRequest{},
			expectedError: status.Error(codes.InvalidArgument, "VolumeID must be in the format of region/zone/storageName/diskName"),
		},
		{
			msg: "WrongCluster",
			request: &proto.CreateSnapshotRequest{
				Name: "name",
				Parameters: map[string]string{
					"param": "value",
				},
				SourceVolumeId: "fake-region/node/data/volume-id",
			},
			expectedError: status.Error(codes.Internal, "region not found"),
		},
	}

	for _, testCase := range tests {
		ts.Run(fmt.Sprint(testCase.msg), func() {
			resp, err := ts.s.CreateSnapshot(context.Background(), testCase.request)
			if testCase.expectedError == nil {
				ts.Require().NoError(err)
				ts.Require().Equal(testCase.expected, resp)
			} else {
				ts.Require().Error(err)
				ts.Require().Equal(testCase.expectedError, err)
			}
		})
	}
}

func (ts *configuredTestSuite) TestDeleteSnapshot() {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	tests := []struct {
		msg           string
		request       *proto.DeleteSnapshotRequest
		expected      *proto.DeleteSnapshotResponse
		expectedError error
	}{
		{
			msg:           "VolumeID",
			request:       &proto.DeleteSnapshotRequest{},
			expectedError: status.Error(codes.InvalidArgument, "VolumeID must be in the format of region/zone/storageName/diskName"),
		},
		{
			msg: "WrongCluster",
			request: &proto.DeleteSnapshotRequest{
				SnapshotId: "fake-region/node/data/volume-id",
			},
			expectedError: status.Error(codes.Internal, "region not found"),
		},
		{
			msg: "PVCNonExist",
			request: &proto.DeleteSnapshotRequest{
				SnapshotId: "cluster-1/pve-1/local-lvm/vm-9999-pvc-none",
			},
			expected: &proto.DeleteSnapshotResponse{},
		},
	}

	for _, testCase := range tests {
		ts.Run(fmt.Sprint(testCase.msg), func() {
			resp, err := ts.s.DeleteSnapshot(context.Background(), testCase.request)
			if testCase.expectedError == nil {
				ts.Require().NoError(err)
				ts.Require().Equal(testCase.expected, resp)
			} else {
				ts.Require().Error(err)
				ts.Require().Equal(testCase.expectedError, err)
			}
		})
	}
}

func (ts *configuredTestSuite) TestListSnapshots() {
	_, err := ts.s.ListSnapshots(context.Background(), &proto.ListSnapshotsRequest{})
	ts.Require().Error(err)
	ts.Require().Equal(status.Error(codes.Unimplemented, ""), err)
}

func (ts *configuredTestSuite) TestControllerExpandVolumeError() {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	capRange := &proto.CapacityRange{
		RequiredBytes: 100 * csi.GiB,
		LimitBytes:    150 * csi.GiB,
	}

	tests := []struct {
		msg      string
		request  *proto.ControllerExpandVolumeRequest
		expected *proto.ControllerExpandVolumeResponse
		// expectedError applies to every config; expectedErrorNoReassign overrides it
		// for the configs that have reassignVolumeOnAttach off, for cases whose volume
		// only exists under a renamed name and so is unreachable without the feature.
		expectedError           error
		expectedErrorNoReassign error
	}{
		{
			msg: "VolumeID",
			request: &proto.ControllerExpandVolumeRequest{
				CapacityRange: capRange,
			},
			expectedError: status.Error(codes.InvalidArgument, "VolumeID must be in the format of region/zone/storageName/diskName"),
		},
		{
			msg: "CapacityRange",
			request: &proto.ControllerExpandVolumeRequest{
				VolumeId: "volume-id",
			},
			expectedError: status.Error(codes.InvalidArgument, "CapacityRange must be provided"),
		},
		{
			msg: "CapacityRangeLimit",
			request: &proto.ControllerExpandVolumeRequest{
				VolumeId: "volume-id",
				CapacityRange: &proto.CapacityRange{
					RequiredBytes: 150 * csi.GiB,
					LimitBytes:    100 * csi.GiB,
				},
			},
			expectedError: status.Error(codes.OutOfRange, "after round-up, volume size exceeds the limit specified"),
		},
		{
			msg: "WrongCluster",
			request: &proto.ControllerExpandVolumeRequest{
				VolumeId:      "fake-region/node/data/volume-id",
				CapacityRange: capRange,
			},
			expectedError: status.Error(codes.Internal, "region not found"),
		},
		{
			msg: "WrongPVC",
			request: &proto.ControllerExpandVolumeRequest{
				VolumeId:      "cluster-1/pve-1/local-lvm/vm-9999-pvc-none",
				CapacityRange: capRange,
			},
			expectedError: status.Error(codes.NotFound, "volume cluster-1/pve-1/local-lvm/vm-9999-pvc-none not found"),
		},
		{
			msg: "WrongPVZone",
			request: &proto.ControllerExpandVolumeRequest{
				VolumeId:      "cluster-1/pve-removed/local-lvm/vm-9999-pvc-exist",
				CapacityRange: capRange,
			},
			expectedError: status.Error(codes.NotFound, "zone pve-removed not found in cluster cluster-1"),
		},
		{
			msg: "UnpublishedVolume",
			request: &proto.ControllerExpandVolumeRequest{
				VolumeId:      "cluster-1/pve-1/local-lvm/vm-9999-pvc-unpublished",
				CapacityRange: capRange,
			},
			expectedError: status.Error(codes.Internal, "cannot resize unpublished"),
		},
		{
			msg: "ExpandVolume",
			request: &proto.ControllerExpandVolumeRequest{
				VolumeId:      "cluster-1/pve-1/local-lvm/vm-9999-pvc-123",
				CapacityRange: capRange,
			},
			expected: &proto.ControllerExpandVolumeResponse{
				CapacityBytes:         100 * csi.GiB,
				NodeExpansionRequired: true,
			},
		},
		{
			// A volume reassignVolumeOnAttach has already renamed onto the VM holding
			// it: Proxmox stores it as vm-101-pvc-reassigned while the PV's immutable
			// volumeHandle still says 9999. Expanding it exercises both halves of the
			// stale-handle path — resolveVolume finding it under the other name, and
			// getVMByAttachedVolume not mistaking VM 101 for the placeholder owner it
			// is told to skip. Without the feature the volume is simply not there.
			msg: "ExpandVolumeReassigned",
			request: &proto.ControllerExpandVolumeRequest{
				VolumeId:      "cluster-1/pve-2/local-lvm/vm-9999-pvc-reassigned",
				CapacityRange: capRange,
			},
			expected: &proto.ControllerExpandVolumeResponse{
				CapacityBytes:         100 * csi.GiB,
				NodeExpansionRequired: true,
			},
			expectedErrorNoReassign: status.Error(codes.NotFound, "volume cluster-1/pve-2/local-lvm/vm-9999-pvc-reassigned not found"),
		},
		{
			// The volume ID has no zone (cluster//<storage>/<disk>), so vol.Node() starts
			// empty and checkVolume must iterate all nodes to find the disk. vm-9999-volume-rbd.raw
			// only exists in pve-2's storage content.
			msg: "ExpandVolumeSharedStorageVMOnDifferentNode",
			request: &proto.ControllerExpandVolumeRequest{
				VolumeId:      "cluster-1//rbd/9999/vm-9999-volume-rbd.raw",
				CapacityRange: capRange,
			},
			expected: &proto.ControllerExpandVolumeResponse{
				CapacityBytes:         100 * csi.GiB,
				NodeExpansionRequired: true,
			},
		},
	}

	for _, testCase := range tests {
		ts.Run(fmt.Sprint(testCase.msg), func() {
			if testCase.expectedErrorNoReassign != nil && !ts.configCase.reassign {
				testCase.expectedError = testCase.expectedErrorNoReassign
			}

			resp, err := ts.s.ControllerExpandVolume(context.Background(), testCase.request)
			if testCase.expectedError == nil {
				ts.Require().NoError(err)
				ts.Require().Equal(testCase.expected, resp)
			} else {
				ts.Require().Error(err)
				ts.Require().Equal(testCase.expectedError, err)
			}
		})
	}
}

func (ts *configuredTestSuite) TestControllerGetVolume() {
	_, err := ts.s.ControllerGetVolume(context.Background(), &proto.ControllerGetVolumeRequest{})
	ts.Require().Error(err)
	ts.Require().Equal(status.Error(codes.Unimplemented, ""), err)
}

// publishVolume attaches the claim-bound scratch volume to VM 100, which is the
// attach the reassignment rename runs under on the configs that enable it.
func (ts *configuredTestSuite) publishVolume() (*proto.ControllerPublishVolumeResponse, error) {
	return ts.s.ControllerPublishVolume(context.Background(), &proto.ControllerPublishVolumeRequest{
		NodeId:   "cluster-1-node-1",
		VolumeId: "cluster-1/pve-1/local-lvm/vm-9999-pvc-unpublished",
		VolumeCapability: &proto.VolumeCapability{
			AccessMode: &proto.VolumeCapability_AccessMode{
				Mode: proto.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
			AccessType: &proto.VolumeCapability_Mount{
				Mount: &proto.VolumeCapability_MountVolume{FsType: "ext4"},
			},
		},
		VolumeContext: map[string]string{csi.StorageIDKey: "local-lvm"},
	})
}

// recordEvents swaps in a recorder a test can read, and returns the reasons of
// everything emitted through it. Nothing here blocks: the fake drops events once
// its buffer fills rather than waiting for a reader.
func (ts *configuredTestSuite) recordEvents() func() []string {
	recorder := record.NewFakeRecorder(16)
	ts.s.Recorder = recorder

	return func() []string {
		close(recorder.Events)

		reasons := []string{}
		for event := range recorder.Events {
			reasons = append(reasons, event)
		}

		return reasons
	}
}
