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
	"os"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	mountutil "k8s.io/mount-utils"
	"k8s.io/utils/keymutex"
)

const testTarget = "/var/lib/kubelet/pods/7f3c/volumes/kubernetes.io~csi/pvc-1/mount"

func TestKataDirectVolumeDirMatchesKataEncoding(t *testing.T) {
	// base64 URL_SAFE with padding, as kata-types join_path computes it.
	assert.Equal(t,
		"/root/L3Zhci9saWIva3ViZWxldC9wb2RzLzdmM2Mvdm9sdW1lcy9rdWJlcm5ldGVzLmlvfmNzaS9wdmMtMS9tb3VudA==",
		kataDirectVolumeDir("/root", testTarget))
}

func TestKataMountInfoRoundTripAndHolder(t *testing.T) {
	root := t.TempDir()

	info, err := readKataMountInfo(root, testTarget)
	require.NoError(t, err)
	assert.Nil(t, info)

	want := &KataMountInfo{
		VolumeType: kataVolumeTypeBlock,
		Device:     "/dev/sdb",
		FsType:     FSTypeExt4,
		Options:    []string{"noatime"},
		Metadata:   map[string]string{kataMetaStagingPath: "/stage/pv-1", kataMetaVolumeID: "vol-1"},
	}
	require.NoError(t, writeKataMountInfo(root, testTarget, want))

	// The file uses Kata's field names.
	raw, err := os.ReadFile(filepath.Join(kataDirectVolumeDir(root, testTarget), kataMountInfoFile))
	require.NoError(t, err)
	assert.JSONEq(t,
		`{"volume-type":"directvol","device":"/dev/sdb","fstype":"ext4","options":["noatime"],
		  "metadata":{"stagingPath":"/stage/pv-1","volumeID":"vol-1"}}`, string(raw))

	got, err := readKataMountInfo(root, testTarget)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	holder, hinfo, err := findKataHolder(root, "/stage/pv-1")
	require.NoError(t, err)
	assert.Equal(t, testTarget, holder)
	assert.Equal(t, "/dev/sdb", hinfo.Device)

	holder, _, err = findKataHolder(root, "/stage/other")
	require.NoError(t, err)
	assert.Empty(t, holder)

	require.NoError(t, removeKataMountInfo(root, testTarget))

	holder, _, err = findKataHolder(root, "/stage/pv-1")
	require.NoError(t, err)
	assert.Empty(t, holder)

	holder, _, err = findKataHolder(filepath.Join(root, "missing"), "/stage/pv-1")
	require.NoError(t, err)
	assert.Empty(t, holder)
}

func TestOtherMountsOf(t *testing.T) {
	mounts := []mountutil.MountPoint{
		{Device: "/dev/sdb", Path: "/stage/pv-1"},
		{Device: "/dev/sdb", Path: "/pods/a/mount"},
		{Device: "/dev/sdb", Path: "/pods/b/mount/"},
		{Device: "/dev/sdc", Path: "/stage/pv-2"},
	}
	assert.Equal(t, []string{"/pods/a/mount"}, otherMountsOf(mounts, "/dev/sdb", "/stage/pv-1", "/pods/b/mount"))
	assert.Empty(t, otherMountsOf(mounts, "/dev/sdc", "/stage/pv-2"))
}

func TestGuestMountOptions(t *testing.T) {
	assert.Equal(t, []string{"noatime", "nouuid"}, guestMountOptions([]string{"noatime", "rw", "nouuid"}, false))
	assert.Equal(t, []string{"noatime", "ro"}, guestMountOptions([]string{"noatime"}, true))
	assert.Equal(t, []string{"a", "b"}, splitOptions("a,b"))
	assert.Nil(t, splitOptions(""))
}

func kataTestClient() *fake.Clientset {
	kata, runc := "kata-clh", "runc"

	return fake.NewClientset(
		&nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: kata}, Handler: "kata-clh-runtime-rs"},
		&nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: runc}, Handler: "runc"},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "vm", Namespace: "ns"}, Spec: corev1.PodSpec{RuntimeClassName: &kata}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "ct", Namespace: "ns"}, Spec: corev1.PodSpec{RuntimeClassName: &runc}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "plain", Namespace: "ns"}},
	)
}

func podContext(name string) map[string]string {
	return map[string]string{podNamespaceKey: "ns", podNameKey: name}
}

func TestIsKataPod(t *testing.T) {
	k := newKataRuntimes(kataTestClient(), "")
	ctx := context.Background()

	for name, want := range map[string]bool{"vm": true, "ct": false, "plain": false} {
		got, err := k.isKataPod(ctx, podContext(name))
		require.NoError(t, err, name)
		assert.Equal(t, want, got, name)
	}

	// No pod information (podInfoOnMount off): never Kata.
	got, err := k.isKataPod(ctx, map[string]string{})
	require.NoError(t, err)
	assert.False(t, got)

	_, err = k.isKataPod(ctx, podContext("missing"))
	assert.Error(t, err)
}

func kataTestService(t *testing.T) *NodeService {
	t.Helper()

	n := &NodeService{nodeID: "node", kclient: kataTestClient(), volumeLocks: keymutex.NewHashed(4)}
	n.SetKataDirectVolumes(KataDirectVolumes{Enabled: true, Root: t.TempDir()})

	return n
}

func publishRequest(pod string, volumeContext map[string]string) *csi.NodePublishVolumeRequest {
	vc := podContext(pod)
	for k, v := range volumeContext {
		vc[k] = v
	}

	return &csi.NodePublishVolumeRequest{
		VolumeId:          "vol-1",
		StagingTargetPath: "/stage/pv-1",
		TargetPath:        testTarget,
		VolumeContext:     vc,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: FSTypeExt4}},
		},
	}
}

func TestPublishKataFallsBackForNonKataAndOptOut(t *testing.T) {
	n := kataTestService(t)
	ctx := context.Background()

	for _, req := range []*csi.NodePublishVolumeRequest{
		publishRequest("ct", nil),
		publishRequest("plain", nil),
		publishRequest("vm", map[string]string{"kataDirectVolume": "false"}),
	} {
		handled, err := n.publishKata(ctx, req, req.GetStagingTargetPath(), req.GetTargetPath(), req.GetVolumeCapability())
		require.NoError(t, err)
		assert.False(t, handled)
	}

	// Raw block volumes are passed to Kata as devices already.
	req := publishRequest("vm", nil)
	req.VolumeCapability = &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}}
	handled, err := n.publishKata(ctx, req, req.GetStagingTargetPath(), req.GetTargetPath(), req.GetVolumeCapability())
	require.NoError(t, err)
	assert.False(t, handled)
}

func TestPublishKataIsIdempotentAndExclusive(t *testing.T) {
	n := kataTestService(t)
	ctx := context.Background()

	info := &KataMountInfo{
		VolumeType: kataVolumeTypeBlock, Device: "/dev/sdb", FsType: FSTypeExt4,
		Metadata: map[string]string{kataMetaStagingPath: "/stage/pv-1", kataMetaVolumeID: "vol-1"},
	}
	require.NoError(t, writeKataMountInfo(n.kata.Root, testTarget, info))

	req := publishRequest("vm", nil)
	handled, err := n.publishKata(ctx, req, req.GetStagingTargetPath(), req.GetTargetPath(), req.GetVolumeCapability())
	require.NoError(t, err)
	assert.True(t, handled, "already published to this VM")

	// Any other pod on the node, Kata or not, is refused while the VM holds the volume.
	for _, pod := range []string{"vm", "ct"} {
		other := publishRequest(pod, nil)
		other.TargetPath = "/var/lib/kubelet/pods/other/volumes/kubernetes.io~csi/pvc-1/mount"
		_, err = n.publishKata(ctx, other, other.GetStagingTargetPath(), other.GetTargetPath(), other.GetVolumeCapability())
		assert.Equal(t, codes.FailedPrecondition, status.Code(err), pod)
	}
}

func TestUnpublishKataRemovesDescription(t *testing.T) {
	n := kataTestService(t)
	target := filepath.Join(t.TempDir(), "mount")
	require.NoError(t, os.MkdirAll(target, 0o750))

	// No staging path recorded: nothing to mount again, only the description and target go.
	info := &KataMountInfo{VolumeType: kataVolumeTypeBlock, Device: "/dev/sdb", FsType: FSTypeExt4, Metadata: map[string]string{kataMetaVolumeID: "vol-1"}}
	require.NoError(t, writeKataMountInfo(n.kata.Root, target, info))

	resp, err := n.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{VolumeId: "vol-1", TargetPath: target})
	require.NoError(t, err)
	assert.NotNil(t, resp)

	got, err := readKataMountInfo(n.kata.Root, target)
	require.NoError(t, err)
	assert.Nil(t, got)
	assert.NoDirExists(t, target)
}

func TestExpandKataVolumeWaitsForRestart(t *testing.T) {
	n := kataTestService(t)
	require.NoError(t, writeKataMountInfo(n.kata.Root, testTarget, &KataMountInfo{VolumeType: kataVolumeTypeBlock, Device: "/dev/sdb"}))

	_, err := n.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId:   "vol-1",
		VolumePath: testTarget,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		},
	})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestKataDirectVolumeParameter(t *testing.T) {
	p, err := ExtractParameters(map[string]string{"kataDirectVolume": "false"})
	require.NoError(t, err)
	require.NotNil(t, p.KataDirectVolume)
	assert.False(t, *p.KataDirectVolume)

	p, err = ExtractParameters(map[string]string{})
	require.NoError(t, err)
	assert.Nil(t, p.KataDirectVolume)
}
