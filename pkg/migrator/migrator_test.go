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

package migrator_test

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/migrator"
	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	testcluster "github.com/sergelogvinov/proxmox-csi-plugin/test/cluster"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const (
	testRegion  = "cluster-1"
	testZone    = "pve-1"
	testStorage = "local-lvm"
	testPVName  = "pvc-0d79713b-6d0b-41e5-b387-42af370d083f"
	testPVCName = "storage-test-0"
	testNS      = "default"
)

func newProxmoxPool(t *testing.T) *pxpool.ProxmoxPool {
	t.Helper()

	pool, err := pxpool.NewProxmoxPool([]*pxpool.ProxmoxCluster{
		{
			URL:         "https://127.0.0.1:8006/api2/json",
			Insecure:    false,
			TokenID:     "user!token-id",
			TokenSecret: "secret",
			Region:      testRegion,
		},
	})
	require.NoError(t, err)

	return pool
}

// nolint: unparam
func newPV(disk string, annotations map[string]string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:        testPVName,
			Annotations: annotations,
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("5Gi"),
			},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       csi.DriverName,
					VolumeHandle: testRegion + "/" + testZone + "/" + testStorage + "/" + disk,
				},
			},
			ClaimRef: &corev1.ObjectReference{
				Namespace: testNS,
				Name:      testPVCName,
			},
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{Key: corev1.LabelTopologyRegion, Operator: "In", Values: []string{testRegion}},
								{Key: corev1.LabelTopologyZone, Operator: "In", Values: []string{testZone}},
							},
						},
					},
				},
			},
		},
	}
}

func newPVC(annotations map[string]string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        testPVCName,
			Namespace:   testNS,
			Annotations: annotations,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: testPVName,
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("5Gi"),
			},
		},
	}
}

func newNode() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cluster-1-node-1",
			Labels: map[string]string{
				corev1.LabelTopologyRegion: testRegion,
				corev1.LabelTopologyZone:   testZone,
			},
		},
		Spec: corev1.NodeSpec{
			ProviderID: "proxmox://" + testRegion + "/100",
		},
	}
}

func newCSINode() *storagev1.CSINode {
	return &storagev1.CSINode{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cluster-1-node-1",
		},
		Spec: storagev1.CSINodeSpec{
			Drivers: []storagev1.CSINodeDriver{
				{
					Name:   csi.DriverName,
					NodeID: "cluster-1-node-1/100",
				},
			},
		},
	}
}

func newPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-0",
			Namespace: testNS,
		},
		Spec: corev1.PodSpec{
			NodeName: "cluster-1-node-1",
			Volumes: []corev1.Volume{
				{
					Name: "storage",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: testPVCName,
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
}

// registerTargetContent registers an exact-URL (beats the shared mock's
// catch-alls) content listing for a storage on a node.
//
// nolint: unparam
func registerTargetContent(node, storage string, volids ...string) {
	items := make([]map[string]any, 0, len(volids))
	for _, v := range volids {
		items = append(items, map[string]any{"volid": v, "format": "raw", "size": 5 * gib})
	}

	httpmock.RegisterResponder(http.MethodGet, "https://127.0.0.1:8006/api2/json/nodes/"+node+"/storage/"+storage+"/content",
		httpmock.NewJsonResponderOrPanic(200, map[string]any{"data": items}))
}

// registerMoveResponder makes the Proxmox disk-move POST succeed for the given
// disk: the POST returns a task UPID and the task status reports stopped/OK.
// The target node's content listing is empty until the move happens and
// contains the target volid afterwards (the migrator checks it before the
// move for shared storage, and after the move for verification).
func registerMoveResponder(disk, targetVolid string) {
	upid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:qmmove:103:root@pam:"
	moved := false

	httpmock.RegisterResponder(http.MethodPost, `=~/nodes/pve-1/storage/local-lvm/content/`+disk,
		func(_ *http.Request) (*http.Response, error) {
			moved = true

			return httpmock.NewJsonResponse(200, map[string]any{"data": upid})
		})

	httpmock.RegisterResponder(http.MethodGet, "https://127.0.0.1:8006/api2/json/nodes/pve-2/storage/local-lvm/content",
		func(_ *http.Request) (*http.Response, error) {
			if moved {
				return httpmock.NewJsonResponse(200, map[string]any{
					"data": []map[string]any{{"volid": targetVolid, "format": "raw", "size": 5 * gib}},
				})
			}

			return httpmock.NewJsonResponse(200, map[string]any{"data": []map[string]any{}})
		})

	httpmock.RegisterResponder(http.MethodGet, `=~/nodes/pve-1/tasks/`+upid+`/status`,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": map[string]any{
				"upid":       upid,
				"node":       "pve-1",
				"status":     "stopped",
				"exitstatus": "OK",
			},
		}))
}

func TestMigrateSuccess(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	disk := "vm-9999-pvc-exist"
	registerMoveResponder(disk, "local-lvm:"+disk)

	pvc := newPVC(map[string]string{
		migrator.AnnotationMigrateNode:  "pve-2",
		migrator.AnnotationMigratePhase: migrator.PhaseMoving,
		"unrelated":                     "keep-me",
	})
	pv := newPV(disk, nil)

	kclient := fake.NewClientset(pvc, pv, newNode(), newCSINode())

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.NoError(t, err)

	newPV, err := kclient.CoreV1().PersistentVolumes().Get(context.Background(), testPVName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, testRegion+"/pve-2/"+testStorage+"/"+disk, newPV.Spec.CSI.VolumeHandle)
	assert.Equal(t, []string{"pve-2"}, newPV.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions[1].Values)
	assert.NotContains(t, newPV.Annotations, migrator.AnnotationMigrateState)

	newPVC, err := kclient.CoreV1().PersistentVolumeClaims(testNS).Get(context.Background(), testPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, newPVC.Annotations, migrator.AnnotationMigrateNode)
	assert.NotContains(t, newPVC.Annotations, migrator.AnnotationMigratePhase)
	assert.Equal(t, "keep-me", newPVC.Annotations["unrelated"])
}

func TestMigrateTaskFailedNoRewire(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	disk := "vm-9999-pvc-exist"
	upid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:qmmove:104:root@pam:"

	registerTargetContent("pve-2", "local-lvm")

	httpmock.RegisterResponder(http.MethodPost, `=~/nodes/pve-1/storage/local-lvm/content/`+disk,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{"data": upid}))

	// The Proxmox task completes WITH AN ERROR: this must be a failure, and
	// the PV must not be rewired (regression: a silently-failed move followed
	// by the rewire lets the provisioner delete the only copy of the disk).
	httpmock.RegisterResponder(http.MethodGet, `=~/nodes/pve-1/tasks/`+upid+`/status`,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": map[string]any{"upid": upid, "node": "pve-1", "status": "stopped", "exitstatus": "storage migration failed"},
		}))

	kclient := fake.NewClientset(newPVC(nil), newPV(disk, nil), newNode(), newCSINode())

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exit status")

	// PV untouched: same volume handle, still on the source zone.
	pv, err := kclient.CoreV1().PersistentVolumes().Get(context.Background(), testPVName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, testRegion+"/"+testZone+"/"+testStorage+"/"+disk, pv.Spec.CSI.VolumeHandle)
}

func TestMigrateCrossStorage(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	disk := "vm-9999-pvc-exist"
	upid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:qmmove:103:root@pam:"

	// The move POST must request the target volume on the NEW storage.
	movedTo := ""

	httpmock.RegisterResponder(http.MethodPost, `=~/nodes/pve-1/storage/local-lvm/content/`+disk,
		func(req *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(req.Body) //nolint: errcheck
			movedTo = string(body)

			return httpmock.NewJsonResponse(200, map[string]any{"data": upid})
		})

	httpmock.RegisterResponder(http.MethodGet, `=~/nodes/pve-1/tasks/`+upid+`/status`,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": map[string]any{"upid": upid, "node": "pve-1", "status": "stopped", "exitstatus": "OK"},
		}))

	// Pre-flight shared-storage check inspects the SOURCE storage on the
	// target node: it must be empty so the move proceeds.
	registerTargetContent("pve-2", "local-lvm")

	// Post-move verification lists the target storage content on the target
	// node. Exact URL: it must win over the mock's catch-all 500 regexp.
	registerTargetContent("pve-2", "zfs", "zfs:"+disk)

	kclient := fake.NewClientset(newPVC(nil), newPV(disk, nil), newNode(), newCSINode())

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:     testNS,
		PVCName:       testPVCName,
		TargetNode:    "pve-2",
		TargetStorage: "zfs",
	})
	require.NoError(t, err)

	assert.Contains(t, movedTo, "zfs:"+disk, "the move request must target the new storage")

	newPV, err := kclient.CoreV1().PersistentVolumes().Get(context.Background(), testPVName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, testRegion+"/pve-2/zfs/"+disk, newPV.Spec.CSI.VolumeHandle, "volume handle must carry the new storage")
}

func TestMigrateCrossStorageInvalidStorage(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	kclient := fake.NewClientset(newPVC(nil), newPV("vm-9999-pvc-exist", nil), newNode(), newCSINode())

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:     testNS,
		PVCName:       testPVCName,
		TargetNode:    "pve-2",
		TargetStorage: "no-such-storage",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, migrator.ErrInvalidTarget)
}

func TestMigrateInvalidTarget(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	kclient := fake.NewClientset(newPVC(nil), newPV("vm-9999-pvc-exist", nil), newNode(), newCSINode())

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-9",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not have storage")
}

func TestMigrateAlreadyOnTarget(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	kclient := fake.NewClientset(newPVC(nil), newPV("vm-9999-pvc-exist", nil), newNode(), newCSINode())

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: testZone,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already on proxmox node")
}

func TestMigrateInUseWithoutForce(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	registerTargetContent("pve-2", "local-lvm")

	kclient := fake.NewClientset(newPVC(nil), newPV("vm-9999-pvc-exist", nil), newNode(), newCSINode(), newPod())

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot move volume")

	// No cordoning should have happened.
	node, err := kclient.CoreV1().Nodes().Get(context.Background(), "cluster-1-node-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.False(t, node.Spec.Unschedulable)
}

func TestMigrateForceUncordonsOnFailure(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// No POST responder for the disk move -> MoveQemuDisk fails after the
	// nodes have been cordoned. The deferred cleanup must uncordon them.
	disk := "vm-9999-pvc-exist"

	registerTargetContent("pve-2", "local-lvm")

	kclient := fake.NewClientset(newPVC(nil), newPV(disk, nil), newNode(), newCSINode(), newPod())

	// Record node patches so we can prove the node was cordoned and then uncordoned.
	nodePatches := []string{}

	kclient.PrependReactor("patch", "nodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if patch, ok := action.(k8stesting.PatchAction); ok {
			nodePatches = append(nodePatches, string(patch.GetPatch()))
		}

		return false, nil, nil
	})

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
		Force:      true,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to move disk")

	// The node was cordoned and then uncordoned by the deferred cleanup.
	assert.Contains(t, nodePatches, `{"spec":{"unschedulable":true}}`, "node must have been cordoned")
	assert.Contains(t, nodePatches, `{"spec":{"unschedulable":false}}`, "node must be uncordoned after a failed migration")

	node, err := kclient.CoreV1().Nodes().Get(context.Background(), "cluster-1-node-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.False(t, node.Spec.Unschedulable, "node must not stay cordoned")
}

func TestMigrateResumePartialDiskRedoesMove(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// A previous attempt stamped the PV state annotation and left a PARTIAL
	// target file (size < capacity). The migrator must NOT resume onto it:
	// it must delete the partial file and redo the move (regression: resuming
	// onto a 0-byte file rewires the PV to garbage and the provisioner then
	// deletes the only real copy).
	disk := "vm-9999-pvc-exist"
	moveUpid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:qmmove:106:root@pam:"
	delUpid := "UPID:pve-2:003B4235:1DF4ABCA:667C1C45:imgdel:106:root@pam:"

	deleted := false
	moved := false

	// Target content: partial file until deleted+moved, full size after.
	httpmock.RegisterResponder(http.MethodGet, "https://127.0.0.1:8006/api2/json/nodes/pve-2/storage/local-lvm/content",
		func(_ *http.Request) (*http.Response, error) {
			switch {
			case moved:
				return httpmock.NewJsonResponse(200, map[string]any{
					"data": []map[string]any{{"volid": "local-lvm:" + disk, "size": 5 * gib}},
				})
			case deleted:
				return httpmock.NewJsonResponse(200, map[string]any{"data": []map[string]any{}})
			default:
				return httpmock.NewJsonResponse(200, map[string]any{
					"data": []map[string]any{{"volid": "local-lvm:" + disk, "size": 1024}},
				})
			}
		})

	httpmock.RegisterResponder(http.MethodDelete, "https://127.0.0.1:8006/api2/json/nodes/pve-2/storage/local-lvm/content/"+disk,
		func(_ *http.Request) (*http.Response, error) {
			deleted = true

			return httpmock.NewJsonResponse(200, map[string]any{"data": delUpid})
		})
	httpmock.RegisterResponder(http.MethodGet, `=~/nodes/pve-2/tasks/`+delUpid+`/status`,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": map[string]any{"upid": delUpid, "node": "pve-2", "status": "stopped", "exitstatus": "OK"},
		}))

	httpmock.RegisterResponder(http.MethodPost, `=~/nodes/pve-1/storage/local-lvm/content/`+disk,
		func(_ *http.Request) (*http.Response, error) {
			moved = true

			return httpmock.NewJsonResponse(200, map[string]any{"data": moveUpid})
		})
	httpmock.RegisterResponder(http.MethodGet, `=~/nodes/pve-1/tasks/`+moveUpid+`/status`,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": map[string]any{"upid": moveUpid, "node": "pve-1", "status": "stopped", "exitstatus": "OK"},
		}))

	pv := newPV(disk, map[string]string{migrator.AnnotationMigrateState: "pve-2"})

	kclient := fake.NewClientset(newPVC(nil), pv, newNode(), newCSINode())

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.NoError(t, err)

	assert.True(t, deleted, "the partial file must be deleted")
	assert.True(t, moved, "the move must be redone")

	newPV, err := kclient.CoreV1().PersistentVolumes().Get(context.Background(), testPVName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, testRegion+"/pve-2/"+testStorage+"/"+disk, newPV.Spec.CSI.VolumeHandle)
}

func TestMigrateSharedStorageSkip(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// The shared mock returns the disk in the content listing of EVERY node:
	// from the migrator's perspective the storage is shared with the target,
	// so the migration must be skipped without any move attempt (no POST
	// responder is registered) and without touching the PV.
	disk := "vm-9999-pvc-exist"

	kclient := fake.NewClientset(newPVC(nil), newPV(disk, nil), newNode(), newCSINode())

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, migrator.ErrSharedStorage)

	pv, err := kclient.CoreV1().PersistentVolumes().Get(context.Background(), testPVName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, testRegion+"/"+testZone+"/"+testStorage+"/"+disk, pv.Spec.CSI.VolumeHandle, "PV must be untouched")
}

func TestMigrateQcow2ConvertAndMove(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// qcow2 volumes are converted to a raw copy via a transient helper VM
	// (file-to-file qemu-img), and the raw copy is then moved with the
	// standard copy endpoint. The volume arrives renamed to .raw.
	disk := "9999/vm-9999-pvc-qcow.qcow2"
	rawTarget := "9999/vm-9999-pvc-qcow.raw"
	intermediate := "9998/vm-9998-disk-0.raw"
	createUpid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:qmcreate:107:root@pam:"
	convertUpid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:qmmove:107:root@pam:"
	copyUpid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:imgcopy:107:root@pam:"
	deleteUpid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:qmdestroy:107:root@pam:"

	attached := ""
	converted := false
	copiedTo := ""
	vmDeleted := false

	registerTargetContent("pve-2", "local-lvm", "local-lvm:"+rawTarget)

	httpmock.RegisterResponder(http.MethodPost, "https://127.0.0.1:8006/api2/json/nodes/pve-1/qemu",
		httpmock.NewJsonResponderOrPanic(200, map[string]any{"data": createUpid}))

	httpmock.RegisterResponder(http.MethodPut, "https://127.0.0.1:8006/api2/json/nodes/pve-1/qemu/9998/config",
		func(req *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(req.Body) //nolint: errcheck
			attached = string(body)

			return httpmock.NewJsonResponse(200, map[string]any{"data": nil})
		})

	httpmock.RegisterResponder(http.MethodPost, "https://127.0.0.1:8006/api2/json/nodes/pve-1/qemu/9998/move_disk",
		func(_ *http.Request) (*http.Response, error) {
			converted = true

			return httpmock.NewJsonResponse(200, map[string]any{"data": convertUpid})
		})

	httpmock.RegisterResponder(http.MethodGet, "https://127.0.0.1:8006/api2/json/nodes/pve-1/qemu/9998/config",
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": map[string]any{"scsi0": "local-lvm:" + intermediate + ",size=5G"},
		}))

	httpmock.RegisterResponder(http.MethodPost, `=~/nodes/pve-1/storage/local-lvm/content/`+intermediate,
		func(req *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(req.Body) //nolint: errcheck
			copiedTo = string(body)

			return httpmock.NewJsonResponse(200, map[string]any{"data": copyUpid})
		})

	httpmock.RegisterResponder(http.MethodDelete, "https://127.0.0.1:8006/api2/json/nodes/pve-1/qemu/9998",
		func(_ *http.Request) (*http.Response, error) {
			vmDeleted = true

			return httpmock.NewJsonResponse(200, map[string]any{"data": deleteUpid})
		})

	for _, upid := range []string{createUpid, convertUpid, copyUpid, deleteUpid} {
		httpmock.RegisterResponder(http.MethodGet, `=~/nodes/pve-1/tasks/`+upid+`/status`,
			httpmock.NewJsonResponderOrPanic(200, map[string]any{
				"data": map[string]any{"upid": upid, "node": "pve-1", "status": "stopped", "exitstatus": "OK"},
			}))
	}

	kclient := fake.NewClientset(newPVC(nil), newPV(disk, nil), newNode(), newCSINode())

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.NoError(t, err)

	assert.Contains(t, attached, "local-lvm:"+disk, "the original must be attached to the helper VM")
	assert.True(t, converted, "the disk must be converted via move_disk")
	assert.Contains(t, copiedTo, "local-lvm:"+rawTarget, "the converted copy must be moved to the .raw target volid")
	assert.True(t, vmDeleted, "the helper VM must be deleted")

	pv, err := kclient.CoreV1().PersistentVolumes().Get(context.Background(), testPVName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, testRegion+"/pve-2/"+testStorage+"/"+rawTarget, pv.Spec.CSI.VolumeHandle)
}

func TestMigrateQcow2CopyFailsHelperCleanedUp(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	disk := "9999/vm-9999-pvc-qcow.qcow2"
	intermediate := "9998/vm-9998-disk-0.raw"
	createUpid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:qmcreate:108:root@pam:"
	convertUpid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:qmmove:108:root@pam:"
	deleteUpid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:qmdestroy:108:root@pam:"

	vmDeleted := false

	// No copy responder: the cross-node move fails after conversion.
	registerTargetContent("pve-2", "local-lvm")

	httpmock.RegisterResponder(http.MethodPost, "https://127.0.0.1:8006/api2/json/nodes/pve-1/qemu",
		httpmock.NewJsonResponderOrPanic(200, map[string]any{"data": createUpid}))
	httpmock.RegisterResponder(http.MethodPut, "https://127.0.0.1:8006/api2/json/nodes/pve-1/qemu/9998/config",
		httpmock.NewJsonResponderOrPanic(200, map[string]any{"data": nil}))
	httpmock.RegisterResponder(http.MethodPost, "https://127.0.0.1:8006/api2/json/nodes/pve-1/qemu/9998/move_disk",
		httpmock.NewJsonResponderOrPanic(200, map[string]any{"data": convertUpid}))
	httpmock.RegisterResponder(http.MethodGet, "https://127.0.0.1:8006/api2/json/nodes/pve-1/qemu/9998/config",
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": map[string]any{"scsi0": "local-lvm:" + intermediate + ",size=5G"},
		}))
	httpmock.RegisterResponder(http.MethodDelete, "https://127.0.0.1:8006/api2/json/nodes/pve-1/qemu/9998",
		func(_ *http.Request) (*http.Response, error) {
			vmDeleted = true

			return httpmock.NewJsonResponse(200, map[string]any{"data": deleteUpid})
		})

	for _, upid := range []string{createUpid, convertUpid, deleteUpid} {
		httpmock.RegisterResponder(http.MethodGet, `=~/nodes/pve-1/tasks/`+upid+`/status`,
			httpmock.NewJsonResponderOrPanic(200, map[string]any{
				"data": map[string]any{"upid": upid, "node": "pve-1", "status": "stopped", "exitstatus": "OK"},
			}))
	}

	kclient := fake.NewClientset(newPVC(nil), newPV(disk, nil), newNode(), newCSINode())

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.Error(t, err)
	assert.True(t, vmDeleted, "the helper VM (and its disposable copy) must be cleaned up")

	pv, err := kclient.CoreV1().PersistentVolumes().Get(context.Background(), testPVName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, testRegion+"/"+testZone+"/"+testStorage+"/"+disk, pv.Spec.CSI.VolumeHandle, "PV must be untouched")
}

func TestMigrateQcow2HelperVMIDOccupied(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	registerTargetContent("pve-2", "local-lvm")

	// VM 9998 exists but is NOT our helper: never touch it, fail terminally.
	httpmock.RegisterResponder(http.MethodGet, "https://127.0.0.1:8006/api2/json/cluster/resources?type=vm",
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": []map[string]any{{"type": "qemu", "vmid": 9998, "node": "pve-1", "name": "production-vm"}},
		}))

	kclient := fake.NewClientset(newPVC(nil), newPV("9999/vm-9999-pvc-qcow.qcow2", nil), newNode(), newCSINode())

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, migrator.ErrInvalidTarget)
	assert.Contains(t, err.Error(), "production-vm")
}

func TestMigrateResumeSkipsMove(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// PV carries the resume marker and the disk already exists on the target
	// node (the shared local-lvm content mock returns it for every node), so
	// the move step must be skipped: no POST responder is registered and the
	// migration still succeeds.
	disk := "vm-9999-pvc-exist"
	pv := newPV(disk, map[string]string{migrator.AnnotationMigrateState: "pve-2"})

	kclient := fake.NewClientset(newPVC(nil), pv, newNode(), newCSINode())

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.NoError(t, err)

	newPV, err := kclient.CoreV1().PersistentVolumes().Get(context.Background(), testPVName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, testRegion+"/pve-2/"+testStorage+"/"+disk, newPV.Spec.CSI.VolumeHandle)
	assert.NotContains(t, newPV.Annotations, migrator.AnnotationMigrateState)
}
