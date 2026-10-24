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
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	proto "github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/provider"

	corev1 "k8s.io/api/core/v1"
)

// Common allocation units
const (
	KiB int64 = 1024
	MiB int64 = 1024 * KiB
	GiB int64 = 1024 * MiB
	TiB int64 = 1024 * GiB
)

const (
	// Group is the project's canonical annotation namespace group.
	Group = "proxmox.crunchymonkies.com"

	// GroupLegacy is the upstream author's annotation group, still read for
	// backward compatibility.
	//
	// Deprecated: use Group. Legacy keys remain accepted on reads only.
	GroupLegacy = "proxmox.sinextra.dev"

	// AnnotationProxmoxInstanceID is the annotation used to store the Proxmox node virtual machine ID.
	AnnotationProxmoxInstanceID = Group + "/instance-id"

	// AnnotationProxmoxInstanceIDLegacy is the upstream variant of
	// AnnotationProxmoxInstanceID, still read for backward compatibility.
	//
	// Deprecated: use AnnotationProxmoxInstanceID.
	AnnotationProxmoxInstanceIDLegacy = GroupLegacy + "/instance-id"
)

// VMLocks is a structure that protects to multiple VMs changes.
type VMLocks struct {
	locks sync.Map
}

// NewVMLocks creates a new instance of VMLocks.
func NewVMLocks() *VMLocks {
	return &VMLocks{}
}

// Lock method locks a VM by its name.
func (v *VMLocks) Lock(name string) {
	actual, _ := v.locks.LoadOrStore(name, &sync.Mutex{})
	actual.(*sync.Mutex).Lock()
}

// Unlock method unlocks a VM by its name.
func (v *VMLocks) Unlock(name string) {
	if actual, ok := v.locks.Load(name); ok {
		actual.(*sync.Mutex).Unlock()
	}
}

// ParseEndpoint parses the endpoint string and returns the scheme and address
func ParseEndpoint(endpoint string) (string, string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", "", fmt.Errorf("could not parse endpoint: %v", err)
	}

	addr := path.Join(u.Host, filepath.FromSlash(u.Path))

	scheme := strings.ToLower(u.Scheme)
	switch scheme {
	case "tcp":
	case "unix":
		if runtime.GOOS == "windows" {
			addr = windowsSocketPath(u)
		} else {
			addr = path.Join("/", addr)
		}

		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			return "", "", fmt.Errorf("could not remove unix domain socket %q: %v", addr, err)
		}
	default:
		return "", "", fmt.Errorf("unsupported protocol: %s", scheme)
	}

	return scheme, addr, nil
}

// windowsSocketPath turns a unix:// endpoint into a Windows file path. The drive letter
// lands in the opaque part for unix:C:/dir/csi.sock (the form gRPC clients such as the
// CSI sidecars accept), in the host for unix://C:/dir/csi.sock and at the start of the
// path for unix:///C:/dir/csi.sock; all three mean C:\dir\csi.sock.
func windowsSocketPath(u *url.URL) string {
	p := u.Opaque
	if p == "" {
		p = u.Host + u.Path
	}

	if len(p) > 2 && p[0] == '/' && p[2] == ':' {
		p = p[1:]
	}

	return strings.ReplaceAll(p, "/", `\`)
}

// ProxmoxVMIDbyNode returns the Proxmox VM ID from the specified kubernetes node.
// The canonical instance-id annotation is read first, the legacy variant second.
func ProxmoxVMIDbyNode(node *corev1.Node) (int, error) {
	vmID, err := provider.GetVMID(node.Spec.ProviderID)
	if err != nil {
		for _, key := range []string{AnnotationProxmoxInstanceID, AnnotationProxmoxInstanceIDLegacy} {
			if vmID, aerr := strconv.Atoi(node.Annotations[key]); aerr == nil {
				return vmID, nil
			}
		}
	}

	return vmID, err
}

// GetNodeTopology extracts region and zone from the provided labels map: the
// canonical proxmox labels first, the legacy upstream variants second, the
// well-known kubernetes.io topology labels last.
func GetNodeTopology(labels map[string]string) (region, zone string) {
	region = firstLabel(labels, ProxmoxRegion, ProxmoxRegionLegacy, corev1.LabelTopologyRegion)
	zone = firstLabel(labels, ProxmoxNode, ProxmoxNodeLegacy, corev1.LabelTopologyZone)

	return region, zone
}

// firstLabel returns the first non-empty value among keys.
func firstLabel(labels map[string]string, keys ...string) string {
	for _, key := range keys {
		if v := labels[key]; v != "" {
			return v
		}
	}

	return ""
}

func locationFromTopologyRequirement(tr *proto.TopologyRequirement) (region, zone string) {
	if tr == nil {
		return "", ""
	}

	for _, top := range tr.GetPreferred() {
		segment := top.GetSegments()

		tsr, tsz := GetNodeTopology(segment)
		if tsr != "" && tsz != "" {
			return tsr, tsz
		}

		if tsr != "" && region == "" {
			region = tsr
		}
	}

	for _, top := range tr.GetRequisite() {
		segment := top.GetSegments()

		tsr, tsz := GetNodeTopology(segment)
		if tsr != "" && tsz != "" {
			return tsr, tsz
		}

		if tsr != "" && region == "" {
			region = tsr
		}
	}

	return region, ""
}

// RoundUpSizeBytes calculates how many allocation units are needed to accommodate
// a volume of given size. E.g. when user wants 1500MiB volume, while AWS EBS
// allocates volumes in gibibyte-sized chunks,
// RoundUpSizeBytes(1500 * 1024*1024, 1024*1024*1024) returns '2*1024*1024*1024' (2GiB)
// (2 GiB is the smallest allocatable volume that can hold 1500MiB)
func RoundUpSizeBytes(volumeSizeBytes int64, allocationUnitBytes int64) int64 {
	if volumeSizeBytes == 0 {
		return allocationUnitBytes
	}

	return allocationUnitBytes * ((volumeSizeBytes + allocationUnitBytes - 1) / allocationUnitBytes)
}
