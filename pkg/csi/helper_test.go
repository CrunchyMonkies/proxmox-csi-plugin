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
	"fmt"
	"testing"

	proto "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"

	corev1 "k8s.io/api/core/v1"
)

func TestParseEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		msg            string
		endpoint       string
		expectedScheme string
		expectedAddr   string
		expectedError  error
	}{
		{
			msg:            "unix socket",
			endpoint:       "unix://tmp/csi.sock",
			expectedScheme: "unix",
			expectedAddr:   "/tmp/csi.sock",
		},
		{
			msg:           "http",
			endpoint:      "http://tmp/csi.sock",
			expectedError: fmt.Errorf("unsupported protocol: http"),
		},
	}

	for _, testCase := range tests {
		t.Run(fmt.Sprint(testCase.msg), func(t *testing.T) {
			t.Parallel()

			scheme, addr, err := ParseEndpoint(testCase.endpoint)
			if testCase.expectedError != nil {
				assert.NotNil(t, err)
				assert.Equal(t, err.Error(), testCase.expectedError.Error())
			} else {
				assert.Nil(t, err)
				assert.NotNil(t, scheme, testCase.expectedScheme)
				assert.Equal(t, addr, testCase.expectedAddr)
			}
		})
	}
}

func TestLocationFromTopologyRequirement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		msg            string
		topology       *proto.TopologyRequirement
		expectedRegion string
		expectedZone   string
	}{
		{
			msg:            "EmptyTopologyRequirement",
			topology:       &proto.TopologyRequirement{},
			expectedRegion: "",
			expectedZone:   "",
		},
		{
			msg: "EmptyTopologyPreferredZone",
			topology: &proto.TopologyRequirement{
				Preferred: []*proto.Topology{
					{
						Segments: map[string]string{
							corev1.LabelTopologyRegion: "region1",
						},
					},
				},
			},
			expectedRegion: "region1",
			expectedZone:   "",
		},
		{
			msg: "EmptyTopologyRequisiteZone",
			topology: &proto.TopologyRequirement{
				Requisite: []*proto.Topology{
					{
						Segments: map[string]string{
							corev1.LabelTopologyRegion: "region1",
						},
					},
				},
			},
			expectedRegion: "region1",
			expectedZone:   "",
		},
		{
			msg: "EmptyTopologyPreferredRegion",
			topology: &proto.TopologyRequirement{
				Preferred: []*proto.Topology{
					{
						Segments: map[string]string{
							corev1.LabelTopologyZone: "zone1",
						},
					},
				},
			},
			expectedRegion: "",
			expectedZone:   "",
		},
		{
			msg: "TopologyPreferred",
			topology: &proto.TopologyRequirement{
				Preferred: []*proto.Topology{
					{
						Segments: map[string]string{
							corev1.LabelTopologyRegion: "region1",
							corev1.LabelTopologyZone:   "zone1",
						},
					},
				},
			},
			expectedRegion: "region1",
			expectedZone:   "zone1",
		},
		{
			msg: "TopologyRequisite",
			topology: &proto.TopologyRequirement{
				Requisite: []*proto.Topology{
					{
						Segments: map[string]string{
							corev1.LabelTopologyRegion: "region1",
							corev1.LabelTopologyZone:   "zone1",
						},
					},
				},
			},
			expectedRegion: "region1",
			expectedZone:   "zone1",
		},
	}

	for _, testCase := range tests {
		t.Run(fmt.Sprint(testCase.msg), func(t *testing.T) {
			t.Parallel()

			region, zone := locationFromTopologyRequirement(testCase.topology)

			assert.Equal(t, testCase.expectedRegion, region)
			assert.Equal(t, testCase.expectedZone, zone)
		})
	}
}

func TestRoundUpSizeBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		msg                 string
		volumeSize          int64
		allocationUnitBytes int64
		expected            int64
	}{
		{
			msg:                 "Zero size",
			volumeSize:          0,
			allocationUnitBytes: GiB,
			expected:            1024 * 1024 * 1024,
		},
		{
			msg:                 "KiB",
			volumeSize:          123,
			allocationUnitBytes: KiB,
			expected:            1024,
		},
		{
			msg:                 "MiB",
			volumeSize:          123,
			allocationUnitBytes: MiB,
			expected:            1024 * 1024,
		},
		{
			msg:                 "GiB",
			volumeSize:          123,
			allocationUnitBytes: GiB,
			expected:            1024 * 1024 * 1024,
		},
		{
			msg:                 "256MiB -> GiB",
			volumeSize:          256 * 1024 * 1024,
			allocationUnitBytes: GiB,
			expected:            1024 * 1024 * 1024,
		},
		{
			msg:                 "256MiB -> GiB/2",
			volumeSize:          256 * 1024 * 1024,
			allocationUnitBytes: 512 * MiB,
			expected:            512 * 1024 * 1024,
		},
	}

	for _, testCase := range tests {
		testCase := testCase

		t.Run(fmt.Sprint(testCase.msg), func(t *testing.T) {
			t.Parallel()

			expected := RoundUpSizeBytes(testCase.volumeSize, testCase.allocationUnitBytes)
			assert.Equal(t, testCase.expected, expected)
		})
	}
}

func TestProxmoxVMIDbyNodeInstanceIDFallback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		msg           string
		providerID    string
		annotations   map[string]string
		expectedVMID  int
		expectedError bool
	}{
		{
			msg:          "providerID wins",
			providerID:   "proxmox://region-1/100",
			expectedVMID: 100,
		},
		{
			msg:          "canonical instance-id annotation",
			annotations:  map[string]string{"proxmox.crunchymonkies.com/instance-id": "101"},
			expectedVMID: 101,
		},
		{
			msg:          "legacy instance-id annotation still read",
			annotations:  map[string]string{"proxmox.sinextra.dev/instance-id": "102"},
			expectedVMID: 102,
		},
		{
			msg: "canonical annotation wins over legacy",
			annotations: map[string]string{
				"proxmox.crunchymonkies.com/instance-id": "103",
				"proxmox.sinextra.dev/instance-id":       "999",
			},
			expectedVMID: 103,
		},
		{
			msg:           "no providerID and no annotation",
			expectedError: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.msg, func(t *testing.T) {
			t.Parallel()

			node := &corev1.Node{}
			node.Spec.ProviderID = testCase.providerID
			node.Annotations = testCase.annotations

			vmID, err := ProxmoxVMIDbyNode(node)
			if testCase.expectedError {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, testCase.expectedVMID, vmID)
			}
		})
	}
}

func TestGetNodeTopologyFallback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		msg            string
		labels         map[string]string
		expectedRegion string
		expectedZone   string
	}{
		{
			msg: "canonical proxmox topology labels",
			labels: map[string]string{
				"topology.proxmox.crunchymonkies.com/region": "region-1",
				"topology.proxmox.crunchymonkies.com/node":   "pve-1",
			},
			expectedRegion: "region-1",
			expectedZone:   "pve-1",
		},
		{
			msg: "legacy proxmox topology labels still read",
			labels: map[string]string{
				"topology.proxmox.sinextra.dev/region": "region-1",
				"topology.proxmox.sinextra.dev/node":   "pve-1",
			},
			expectedRegion: "region-1",
			expectedZone:   "pve-1",
		},
		{
			msg: "canonical labels win over legacy and kubernetes.io",
			labels: map[string]string{
				"topology.proxmox.crunchymonkies.com/region": "region-1",
				"topology.proxmox.crunchymonkies.com/node":   "pve-1",
				"topology.proxmox.sinextra.dev/region":       "legacy-region",
				"topology.proxmox.sinextra.dev/node":         "legacy-node",
				corev1.LabelTopologyRegion:                   "k8s-region",
				corev1.LabelTopologyZone:                     "k8s-zone",
			},
			expectedRegion: "region-1",
			expectedZone:   "pve-1",
		},
		{
			msg: "legacy labels win over kubernetes.io",
			labels: map[string]string{
				"topology.proxmox.sinextra.dev/region": "legacy-region",
				"topology.proxmox.sinextra.dev/node":   "legacy-node",
				corev1.LabelTopologyRegion:             "k8s-region",
				corev1.LabelTopologyZone:               "k8s-zone",
			},
			expectedRegion: "legacy-region",
			expectedZone:   "legacy-node",
		},
		{
			msg: "kubernetes.io labels as last resort",
			labels: map[string]string{
				corev1.LabelTopologyRegion: "k8s-region",
				corev1.LabelTopologyZone:   "k8s-zone",
			},
			expectedRegion: "k8s-region",
			expectedZone:   "k8s-zone",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.msg, func(t *testing.T) {
			t.Parallel()

			region, zone := GetNodeTopology(testCase.labels)
			assert.Equal(t, testCase.expectedRegion, region)
			assert.Equal(t, testCase.expectedZone, zone)
		})
	}
}
