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

// Kata Containers direct-assigned volumes.
//
// A pod running in a Kata VM normally sees a filesystem volume through virtio-fs: the
// volume is mounted on the node and every file operation crosses the VM boundary. Kata can
// instead hot-plug the block device into the VM and mount it inside the guest when the CSI
// node plugin describes the volume in
//
//	<root>/<base64url(volume path)>/mountInfo.json
//
// where the volume path is the NodePublish target path (the mount source Kata sees). See
// kata-containers src/libs/kata-types/src/mount.rs (DirectVolumeMountInfo, join_path).
//
// The staged mount on the node stays the normal state of a volume. Publishing to a Kata pod
// unmounts the staging path and writes mountInfo.json; unpublishing removes it and mounts the
// staging path again. A filesystem mounted in a VM must not be mounted anywhere else at the
// same time, so while a VM holds a volume no other pod on the node can publish it.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	mountutil "k8s.io/mount-utils"
)

const (
	// DefaultKataDirectVolumeRoot is where Kata looks for direct-assigned volume descriptions.
	DefaultKataDirectVolumeRoot = "/run/kata-containers/shared/direct-volumes"
	// DefaultKataHandlerPrefix marks RuntimeClass handlers that run pods in Kata VMs.
	DefaultKataHandlerPrefix = "kata"

	kataMountInfoFile = "mountInfo.json"
	// kataVolumeTypeBlock is Kata runtime-rs's raw block direct volume, mounted in the guest
	// (KATA_DIRECT_VOLUME_TYPE in runtime-rs resource/src/volume/direct_volumes/mod.rs; any other
	// value is rejected with "volume type ... is invalid").
	kataVolumeTypeBlock = "directvol"

	kataMetaStagingPath  = "stagingPath"
	kataMetaVolumeID     = "volumeID"
	kataMetaStageOptions = "stageOptions"

	podNamespaceKey = "csi.storage.k8s.io/pod.namespace"
	podNameKey      = "csi.storage.k8s.io/pod.name"

	runtimeClassCacheTTL = time.Minute
)

// KataMountInfo is Kata's DirectVolumeMountInfo.
type KataMountInfo struct {
	VolumeType string            `json:"volume-type"`
	Device     string            `json:"device"`
	FsType     string            `json:"fstype"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	Options    []string          `json:"options,omitempty"`
}

// KataDirectVolumes configures direct-assigned volumes for Kata pods.
type KataDirectVolumes struct {
	// Enabled publishes filesystem volumes of Kata pods as direct-assigned volumes.
	Enabled bool
	// HandlerPrefix marks Kata RuntimeClass handlers.
	HandlerPrefix string
	// Root is Kata's direct volume directory.
	Root string
}

// kataDirectVolumeDir is the directory Kata reads for the volume mounted at targetPath.
// Kata encodes the path with base64 URL_SAFE, padded (Go's base64.URLEncoding).
func kataDirectVolumeDir(root, targetPath string) string {
	return filepath.Join(root, base64.URLEncoding.EncodeToString([]byte(targetPath)))
}

// readKataMountInfo returns the description of the volume at targetPath, or nil if there is none.
func readKataMountInfo(root, targetPath string) (*KataMountInfo, error) {
	data, err := os.ReadFile(filepath.Join(kataDirectVolumeDir(root, targetPath), kataMountInfoFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	info := &KataMountInfo{}
	if err := json.Unmarshal(data, info); err != nil {
		return nil, fmt.Errorf("invalid %s for %s: %w", kataMountInfoFile, targetPath, err)
	}

	return info, nil
}

// writeKataMountInfo atomically writes the description of the volume at targetPath.
func writeKataMountInfo(root, targetPath string, info *KataMountInfo) error {
	dir := kataDirectVolumeDir(root, targetPath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}

	data, err := json.Marshal(info)
	if err != nil {
		return err
	}

	tmp := filepath.Join(dir, "."+kataMountInfoFile+".tmp")
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}

	return os.Rename(tmp, filepath.Join(dir, kataMountInfoFile))
}

// removeKataMountInfo removes the description of the volume at targetPath.
func removeKataMountInfo(root, targetPath string) error {
	return os.RemoveAll(kataDirectVolumeDir(root, targetPath))
}

// findKataHolder returns the target path and description of the direct-assigned volume
// whose staging path is stagingPath, if a Kata VM holds it.
func findKataHolder(root, stagingPath string) (string, *KataMountInfo, error) {
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil, nil
	}

	if err != nil {
		return "", nil, err
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		target, err := base64.URLEncoding.DecodeString(e.Name())
		if err != nil {
			continue
		}

		info, err := readKataMountInfo(root, string(target))
		if err != nil || info == nil {
			continue
		}

		if info.Metadata[kataMetaStagingPath] == stagingPath {
			return string(target), info, nil
		}
	}

	return "", nil, nil
}

// otherMountsOf lists mount points of device other than the given paths: bind mounts of a
// staged volume into other pods on this node.
func otherMountsOf(mounts []mountutil.MountPoint, device string, except ...string) []string {
	var out []string

	for _, m := range mounts {
		if m.Device != device {
			continue
		}

		skip := false

		for _, e := range except {
			if filepath.Clean(m.Path) == filepath.Clean(e) {
				skip = true

				break
			}
		}

		if !skip {
			out = append(out, m.Path)
		}
	}

	return out
}

// guestMountOptions are the options Kata mounts the filesystem with inside the VM.
func guestMountOptions(stageOptions []string, readOnly bool) []string {
	opts := make([]string, 0, len(stageOptions)+1)

	for _, o := range stageOptions {
		if o != "rw" && o != "ro" {
			opts = append(opts, o)
		}
	}

	if readOnly {
		opts = append(opts, "ro")
	}

	return opts
}

// splitOptions parses a comma-joined option list.
func splitOptions(s string) []string {
	if s == "" {
		return nil
	}

	return strings.Split(s, ",")
}

// kataRuntimes tells whether a pod runs in a Kata VM, caching RuntimeClass handlers.
type kataRuntimes struct {
	kclient kubernetes.Interface
	prefix  string

	mu       sync.Mutex
	handlers map[string]runtimeClassEntry
}

type runtimeClassEntry struct {
	handler string
	at      time.Time
}

func newKataRuntimes(kclient kubernetes.Interface, prefix string) *kataRuntimes {
	if prefix == "" {
		prefix = DefaultKataHandlerPrefix
	}

	return &kataRuntimes{kclient: kclient, prefix: prefix, handlers: map[string]runtimeClassEntry{}}
}

// isKataPod reports whether the pod named in the NodePublish volume context runs in a Kata
// VM. Without pod information (podInfoOnMount disabled) it reports false.
func (k *kataRuntimes) isKataPod(ctx context.Context, volumeContext map[string]string) (bool, error) {
	ns, name := volumeContext[podNamespaceKey], volumeContext[podNameKey]
	if ns == "" || name == "" || k.kclient == nil {
		return false, nil
	}

	pod, err := k.kclient.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("get pod %s/%s: %w", ns, name, err)
	}

	rc := pod.Spec.RuntimeClassName
	if rc == nil || *rc == "" {
		return false, nil
	}

	handler, err := k.handler(ctx, *rc)
	if err != nil {
		return false, err
	}

	return strings.HasPrefix(handler, k.prefix), nil
}

func (k *kataRuntimes) handler(ctx context.Context, runtimeClass string) (string, error) {
	k.mu.Lock()
	e, ok := k.handlers[runtimeClass]
	k.mu.Unlock()

	if ok && time.Since(e.at) < runtimeClassCacheTTL {
		return e.handler, nil
	}

	rc, err := k.kclient.NodeV1().RuntimeClasses().Get(ctx, runtimeClass, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get RuntimeClass %s: %w", runtimeClass, err)
	}

	k.mu.Lock()
	k.handlers[runtimeClass] = runtimeClassEntry{handler: rc.Handler, at: time.Now()}
	k.mu.Unlock()

	return rc.Handler, nil
}

// SetKataDirectVolumes enables direct-assigned volumes for pods running in Kata VMs.
func (n *NodeService) SetKataDirectVolumes(cfg KataDirectVolumes) {
	if cfg.Root == "" {
		cfg.Root = DefaultKataDirectVolumeRoot
	}

	n.kata = &cfg
	n.kataRuntimes = newKataRuntimes(n.kclient, cfg.HandlerPrefix)
}

func (n *NodeService) kataEnabled() bool {
	return n.kata != nil && n.kata.Enabled
}

// publishKata publishes a filesystem volume of a Kata pod as a direct-assigned volume. It
// reports false when the volume should be published the usual way (not a Kata pod, or the
// StorageClass opted out).
func (n *NodeService) publishKata(
	ctx context.Context,
	request *csi.NodePublishVolumeRequest,
	stagingTargetPath, targetPath string,
	capability *csi.VolumeCapability,
) (bool, error) {
	root := n.kata.Root
	volumeID := request.GetVolumeId()

	info, err := readKataMountInfo(root, targetPath)
	if err != nil {
		return false, status.Error(codes.Internal, err.Error())
	}

	if info != nil {
		return true, nil
	}

	// While a VM holds the volume the staging path is not mounted, so nobody else may use it.
	holder, _, err := findKataHolder(root, stagingTargetPath)
	if err != nil {
		return false, status.Error(codes.Internal, err.Error())
	}

	if holder != "" {
		return false, status.Errorf(codes.FailedPrecondition,
			"volume %s is mounted inside a Kata VM (%s) and cannot be used by another pod on this node at the same time", volumeID, holder)
	}

	mnt := capability.GetMount()
	if mnt == nil {
		return false, nil
	}

	params, err := ExtractParameters(request.GetVolumeContext())
	if err != nil {
		return false, status.Error(codes.InvalidArgument, err.Error())
	}

	if params.KataDirectVolume != nil && !*params.KataDirectVolume {
		return false, nil
	}

	kata, err := n.kataRuntimes.isKataPod(ctx, request.GetVolumeContext())
	if err != nil {
		return false, status.Errorf(codes.Unavailable, "cannot tell whether the pod runs in a Kata VM: %v", err)
	}

	if !kata {
		return false, nil
	}

	n.volumeLocks.LockKey(volumeID)
	defer n.volumeLocks.UnlockKey(volumeID) //nolint:errcheck

	m := n.Mount.Mounter()

	notMnt, err := m.IsLikelyNotMountPoint(stagingTargetPath)
	if err != nil || notMnt {
		return false, status.Errorf(codes.FailedPrecondition, "staging path %s is not mounted", stagingTargetPath)
	}

	out, err := n.Mount.GetMountFs(stagingTargetPath)
	if err != nil {
		return false, status.Errorf(codes.Internal, "failed to find the device of %s: %v", stagingTargetPath, err)
	}

	device := strings.TrimSpace(string(out))
	if device == "" {
		return false, status.Errorf(codes.Internal, "no device mounted at %s", stagingTargetPath)
	}

	mounts, err := m.List()
	if err != nil {
		return false, status.Errorf(codes.Internal, "failed to list mounts: %v", err)
	}

	if others := otherMountsOf(mounts, device, stagingTargetPath, targetPath); len(others) > 0 {
		return false, status.Errorf(codes.FailedPrecondition,
			"volume %s is in use by another pod on this node (%s); a Kata VM needs it to itself", volumeID, strings.Join(others, ", "))
	}

	fsType := FSTypeExt4
	if mnt.GetFsType() != "" {
		fsType = mnt.GetFsType()
	}

	stageOptions := collectMountOptions(params, fsType, mnt.GetMountFlags())

	info = &KataMountInfo{
		VolumeType: kataVolumeTypeBlock,
		Device:     device,
		FsType:     fsType,
		Options:    guestMountOptions(stageOptions, request.GetReadonly()),
		Metadata: map[string]string{
			kataMetaStagingPath:  stagingTargetPath,
			kataMetaVolumeID:     volumeID,
			kataMetaStageOptions: strings.Join(stageOptions, ","),
		},
	}

	// The VM must be the only one with the filesystem mounted.
	if err := m.Unmount(stagingTargetPath); err != nil {
		return false, status.Errorf(codes.Internal, "failed to unmount %s: %v", stagingTargetPath, err)
	}

	restage := func() {
		if err := m.Mount(device, stagingTargetPath, fsType, stageOptions); err != nil {
			klog.ErrorS(err, "NodePublishVolume: failed to mount the staging path again", "device", device, "path", stagingTargetPath)
		}
	}

	if err := writeKataMountInfo(root, targetPath, info); err != nil {
		restage()

		return false, status.Errorf(codes.Internal, "failed to write the Kata volume description: %v", err)
	}

	if err := os.MkdirAll(targetPath, 0o750); err != nil {
		if rerr := removeKataMountInfo(root, targetPath); rerr != nil {
			klog.ErrorS(rerr, "NodePublishVolume: failed to remove the Kata volume description", "path", targetPath)
		}

		restage()

		return false, status.Errorf(codes.Internal, "failed to create %s: %v", targetPath, err)
	}

	klog.V(3).InfoS("NodePublishVolume: volume published into a Kata VM", "device", device, "fsType", fsType,
		"pod", klog.KRef(request.GetVolumeContext()[podNamespaceKey], request.GetVolumeContext()[podNameKey]),
	)

	return true, nil
}

// unpublishKata hands a direct-assigned volume back to the node: the staging path is mounted
// again, so the volume can be published to another pod or unstaged as usual.
func (n *NodeService) unpublishKata(targetPath string, info *KataMountInfo) error {
	if volumeID := info.Metadata[kataMetaVolumeID]; volumeID != "" {
		n.volumeLocks.LockKey(volumeID)
		defer n.volumeLocks.UnlockKey(volumeID) //nolint:errcheck
	}

	if staging := info.Metadata[kataMetaStagingPath]; staging != "" {
		m := n.Mount.Mounter()

		if err := os.MkdirAll(staging, 0o750); err != nil {
			return status.Errorf(codes.Internal, "failed to create %s: %v", staging, err)
		}

		notMnt, err := m.IsLikelyNotMountPoint(staging)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to check %s: %v", staging, err)
		}

		if notMnt {
			if err := m.Mount(info.Device, staging, info.FsType, splitOptions(info.Metadata[kataMetaStageOptions])); err != nil {
				return status.Errorf(codes.Internal, "failed to mount %s at %s again: %v", info.Device, staging, err)
			}
		}
	}

	if err := removeKataMountInfo(n.kata.Root, targetPath); err != nil {
		return status.Errorf(codes.Internal, "failed to remove the Kata volume description: %v", err)
	}

	if err := os.Remove(targetPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return status.Errorf(codes.Internal, "failed to remove %s: %v", targetPath, err)
	}

	klog.V(3).InfoS("NodeUnpublishVolume: volume returned from a Kata VM", "device", info.Device, "path", targetPath)

	return nil
}
