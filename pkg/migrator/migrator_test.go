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
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/luthermonson/go-proxmox"
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

// Kubernetes PV-controller / scheduler bookkeeping annotations a live BOUND
// PVC carries (upstream keeps the constants in internal packages). Every
// bound fixture PVC includes them, and the simulated binder enforces the Lost
// trap they set up, so recreating a PVC from a bound copy WITHOUT stripping
// them fails these tests the way it fails on a live cluster.
const (
	annBindCompleted          = "pv.kubernetes.io/bind-completed"
	annBoundByController      = "pv.kubernetes.io/bound-by-controller"
	annSelectedNode           = "volume.kubernetes.io/selected-node"
	annStorageProvisioner     = "volume.kubernetes.io/storage-provisioner"
	annBetaStorageProvisioner = "volume.beta.kubernetes.io/storage-provisioner"
)

// binderBookkeepingAnnotations lists the five keys above: the annotations a
// rewired (recreated) PVC must NOT carry at Create time.
var binderBookkeepingAnnotations = []string{
	annBindCompleted,
	annBoundByController,
	annSelectedNode,
	annStorageProvisioner,
	annBetaStorageProvisioner,
}

// migratedPV returns the PersistentVolume the test PVC is bound to after a
// successful migration. The rewire reserves a freshly named PV (claimRef
// pre-bind), so the migrated volume is found through the PVC's volumeName
// rather than the original PV name.
func migratedPV(t *testing.T, kclient *fake.Clientset) *corev1.PersistentVolume {
	t.Helper()

	pvc, err := kclient.CoreV1().PersistentVolumeClaims(testNS).Get(context.Background(), testPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, pvc.Spec.VolumeName, "PVC must be bound after migration")

	pv, err := kclient.CoreV1().PersistentVolumes().Get(context.Background(), pvc.Spec.VolumeName, metav1.GetOptions{})
	require.NoError(t, err)

	return pv
}

// countPVCUpdates returns how many times the PVC resource received an update
// action. The claimRef pre-bind rewire must never update a PVC (the migrator's
// RBAC omits the update verb and a bound PVC's volumeName is immutable).
func countPVCUpdates(kclient *fake.Clientset) int {
	n := 0

	for _, a := range kclient.Actions() {
		if a.GetVerb() == "update" && a.GetResource().Resource == "persistentvolumeclaims" {
			n++
		}
	}

	return n
}

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
	// A live bound PVC carries the PV controller's and scheduler's bookkeeping
	// annotations. The fixtures stamp them on every bound source PVC so the
	// rewire's annotation strip is exercised by every migration test.
	anns := map[string]string{
		annBindCompleted:          "yes",
		annBoundByController:      "yes",
		annSelectedNode:           "cluster-1-node-1",
		annStorageProvisioner:     csi.DriverName,
		annBetaStorageProvisioner: csi.DriverName,
	}

	for k, v := range annotations {
		anns[k] = v
	}

	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        testPVCName,
			Namespace:   testNS,
			Annotations: anns,
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

// newForeignNode returns the node as registered by a non-Proxmox CCM (e.g.
// RKE2): foreign providerID, no instance-id annotation, but a SMBIOS system
// UUID reported by the kubelet.
func newForeignNode(systemUUID string) *corev1.Node {
	node := newNode()
	node.Spec.ProviderID = "rke2://cluster-1-node-1"
	node.Status.NodeInfo.SystemUUID = systemUUID

	return node
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
// move for shared storage, and after the move for verification). It also mocks
// the post-success source-disk DELETE (the migrator reclaims the source copy the
// move leaves at the origin) so the standard cross-node success path is fully served.
//
// nolint: unparam
func registerMoveResponder(disk, targetVolid string) {
	upid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:qmmove:103:root@pam:"
	delUpid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:imgdel:103:root@pam:"
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

	// Source-disk reclaim after a verified success: DELETE on the source node/storage.
	httpmock.RegisterResponder(http.MethodDelete, "https://127.0.0.1:8006/api2/json/nodes/pve-1/storage/local-lvm/content/"+disk,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{"data": delUpid}))
	httpmock.RegisterResponder(http.MethodGet, `=~/nodes/pve-1/tasks/`+delUpid+`/status`,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": map[string]any{"upid": delUpid, "node": "pve-1", "status": "stopped", "exitstatus": "OK"},
		}))

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
	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.NoError(t, err)

	newPV := migratedPV(t, kclient)
	assert.Equal(t, testRegion+"/pve-2/"+testStorage+"/"+disk, newPV.Spec.CSI.VolumeHandle)
	assert.Equal(t, []string{"pve-2"}, newPV.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions[1].Values)
	assert.NotContains(t, newPV.Annotations, migrator.AnnotationMigrateState)

	newPVC, err := kclient.CoreV1().PersistentVolumeClaims(testNS).Get(context.Background(), testPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, newPVC.Annotations, migrator.AnnotationMigrateNode)
	assert.NotContains(t, newPVC.Annotations, migrator.AnnotationMigratePhase)
	assert.Equal(t, "keep-me", newPVC.Annotations["unrelated"])
}

func TestMigrateSuccessTokenCopyEndpoint(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// With TokenCopyEndpoint the copy must POST the pve-csi-copy package's
	// storage-level method — EXACT URL /nodes/{node}/storage/{storage}/csi-copy —
	// and never the content/{volume} URL. The old content/{volume}/copy shape is
	// unroutable on a real PVE: the router's greedy {volume} parameter swallows
	// the '/copy' suffix and the request lands on the root-only built-in
	// (regression: the client posted there and PVE answered 400 "duplicate
	// parameter ... with conflicting values!").
	disk := "vm-9999-pvc-exist"
	upid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:imgcopy:103:root@pam:"
	moved := false
	body := map[string]any{}

	httpmock.RegisterResponder(http.MethodPost, "https://127.0.0.1:8006/api2/json/nodes/pve-1/storage/local-lvm/csi-copy",
		func(req *http.Request) (*http.Response, error) {
			moved = true

			raw, _ := io.ReadAll(req.Body) //nolint: errcheck
			_ = json.Unmarshal(raw, &body) //nolint: errcheck

			return httpmock.NewJsonResponse(200, map[string]any{"data": upid})
		})

	// A POST to the content/{volume} URL in token mode is the bug this guards
	// against: answer 500 so the migration would fail loudly, and assert below
	// that it was never called.
	httpmock.RegisterResponder(http.MethodPost, `=~/nodes/pve-1/storage/local-lvm/content/`,
		httpmock.NewStringResponder(500, "token mode must not use the content copy URL"))

	httpmock.RegisterResponder(http.MethodGet, "https://127.0.0.1:8006/api2/json/nodes/pve-2/storage/local-lvm/content",
		func(_ *http.Request) (*http.Response, error) {
			if moved {
				return httpmock.NewJsonResponse(200, map[string]any{
					"data": []map[string]any{{"volid": "local-lvm:" + disk, "format": "raw", "size": 5 * gib}},
				})
			}

			return httpmock.NewJsonResponse(200, map[string]any{"data": []map[string]any{}})
		})

	httpmock.RegisterResponder(http.MethodGet, `=~/nodes/pve-1/tasks/`+upid+`/status`,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": map[string]any{"upid": upid, "node": "pve-1", "status": "stopped", "exitstatus": "OK"},
		}))

	kclient := fake.NewClientset(newPVC(nil), newPV(disk, nil), newNode(), newCSINode())
	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t), TokenCopyEndpoint: true}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.NoError(t, err)
	assert.True(t, moved, "the csi-copy endpoint must have been posted")

	// Body carries volume/target/target_node ONLY: node comes from the URI, and
	// PVE 400s a request whose body duplicates a URI parameter.
	assert.Equal(t, disk, body["volume"])
	assert.Equal(t, "local-lvm:"+disk, body["target"])
	assert.Equal(t, "pve-2", body["target_node"])
	assert.NotContains(t, body, "node", "the URI supplies node; a body copy conflicts")

	for key, count := range httpmock.GetCallCountInfo() {
		if strings.HasPrefix(key, http.MethodPost) && strings.Contains(key, "/storage/local-lvm/content/") {
			assert.Zero(t, count, "token mode must not POST the content/{volume} URL: %s", key)
		}
	}

	newPV := migratedPV(t, kclient)
	assert.Equal(t, testRegion+"/pve-2/"+testStorage+"/"+disk, newPV.Spec.CSI.VolumeHandle)
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
	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:     testNS,
		PVCName:       testPVCName,
		TargetNode:    "pve-2",
		TargetStorage: "zfs",
	})
	require.NoError(t, err)

	assert.Contains(t, movedTo, "zfs:"+disk, "the move request must target the new storage")

	newPV := migratedPV(t, kclient)
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

func TestMigrateSameNodeCrossStorage(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// Same-node storage move: the disk stays on pve-1 but moves from
	// local-lvm to zfs. The shared mock lists the source disk in pve-1's own
	// local-lvm content — trivially true for the disk's own node — so this
	// also proves the shared-storage pre-flight is skipped for same-node
	// moves (it would otherwise misfire as ErrSharedStorage).
	disk := "vm-9999-pvc-exist"
	upid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:qmmove:103:root@pam:"

	movedTo := ""
	moved := false

	httpmock.RegisterResponder(http.MethodPost, `=~/nodes/pve-1/storage/local-lvm/content/`+disk,
		func(req *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(req.Body) //nolint: errcheck
			movedTo = string(body)
			moved = true

			return httpmock.NewJsonResponse(200, map[string]any{"data": upid})
		})

	httpmock.RegisterResponder(http.MethodGet, `=~/nodes/pve-1/tasks/`+upid+`/status`,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": map[string]any{"upid": upid, "node": "pve-1", "status": "stopped", "exitstatus": "OK"},
		}))

	// Post-move verification lists the TARGET storage on the SAME node: empty
	// before the move, the full-size target volid after. Exact URL: it must
	// win over the mock's catch-all 500 regexp.
	httpmock.RegisterResponder(http.MethodGet, "https://127.0.0.1:8006/api2/json/nodes/pve-1/storage/zfs/content",
		func(_ *http.Request) (*http.Response, error) {
			if moved {
				return httpmock.NewJsonResponse(200, map[string]any{
					"data": []map[string]any{{"volid": "zfs:" + disk, "format": "raw", "size": 5 * gib}},
				})
			}

			return httpmock.NewJsonResponse(200, map[string]any{"data": []map[string]any{}})
		})

	kclient := fake.NewClientset(newPVC(nil), newPV(disk, nil), newNode(), newCSINode())
	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:     testNS,
		PVCName:       testPVCName,
		TargetNode:    testZone,
		TargetStorage: "zfs",
	})
	require.NoError(t, err)

	assert.Contains(t, movedTo, "zfs:"+disk, "the move request must target the new storage")

	newPV := migratedPV(t, kclient)
	assert.Equal(t, testRegion+"/"+testZone+"/zfs/"+disk, newPV.Spec.CSI.VolumeHandle, "volume handle must carry the new storage on the same node")
	assert.Equal(t, []string{testZone}, newPV.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions[1].Values, "zone must stay unchanged")
	assert.NotContains(t, newPV.Annotations, migrator.AnnotationMigrateState)
}

func TestMigrateReclaimsSourceDiskOnSuccess(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// A verified, fully-bound migration must reclaim the source disk copy the move
	// leaves at the origin (the rewire forces the source PV to Retain, so the
	// provisioner never does). The DELETE must hit the SOURCE node+storage+disk and
	// fire only AFTER the PVC bound to the freshly reserved (migrated) PV.
	disk := "vm-9999-pvc-exist"
	registerMoveResponder(disk, "local-lvm:"+disk)

	kclient := fake.NewClientset(newPVC(nil), newPV(disk, nil), newNode(), newCSINode())
	simulateClaimRefBinder(kclient)

	sourceDeleted := false
	boundAtDelete := false
	delUpid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:imgdel:200:root@pam:"

	httpmock.RegisterResponder(http.MethodDelete, "https://127.0.0.1:8006/api2/json/nodes/pve-1/storage/local-lvm/content/"+disk,
		func(_ *http.Request) (*http.Response, error) {
			sourceDeleted = true

			// The PVC is already rebound to the migrated PV (never the original)
			// by the time the source is reclaimed: this proves the ordering.
			pvc, gerr := kclient.CoreV1().PersistentVolumeClaims(testNS).Get(context.Background(), testPVCName, metav1.GetOptions{})
			if gerr == nil && pvc.Spec.VolumeName != "" && pvc.Spec.VolumeName != testPVName {
				boundAtDelete = true
			}

			return httpmock.NewJsonResponse(200, map[string]any{"data": delUpid})
		})
	httpmock.RegisterResponder(http.MethodGet, `=~/nodes/pve-1/tasks/`+delUpid+`/status`,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": map[string]any{"upid": delUpid, "node": "pve-1", "status": "stopped", "exitstatus": "OK"},
		}))

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.NoError(t, err)

	assert.True(t, sourceDeleted, "the source disk copy on pve-1/local-lvm must be reclaimed on success")
	assert.True(t, boundAtDelete, "the source disk must be deleted only AFTER the PVC bound to the migrated PV")
}

func TestMigrateFailedRewireKeepsSourceDisk(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// The rewire fails: a racing provisioner binds the recreated PVC to a fresh
	// empty volume instead of the reserved one. At that point the source copy is
	// the ONLY good copy of the data, so it must NOT be deleted — it stays under
	// Retain as the safety net.
	disk := "vm-9999-pvc-exist"
	registerMoveResponder(disk, "local-lvm:"+disk)

	kclient := fake.NewClientset(newPVC(nil), newPV(disk, nil), newNode(), newCSINode())

	sourceDeleted := false

	httpmock.RegisterResponder(http.MethodDelete, "https://127.0.0.1:8006/api2/json/nodes/pve-1/storage/local-lvm/content/"+disk,
		func(_ *http.Request) (*http.Response, error) {
			sourceDeleted = true

			return httpmock.NewJsonResponse(200, map[string]any{"data": "UPID:pve-1:0:0:0:imgdel:0:root@pam:"})
		})

	tracker := kclient.Tracker()
	emptyPVName := "pvc-controller-empty"

	kclient.PrependReactor("delete", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		da, ok := action.(k8stesting.DeleteAction)
		if !ok || da.GetName() != testPVCName {
			return false, nil, nil
		}

		_ = tracker.Delete(pvcGVR, testNS, testPVCName) //nolint: errcheck

		emptyPV := newPV("vm-9999-pvc-empty-provisioned", nil)
		emptyPV.Name = emptyPVName
		emptyPV.Spec.ClaimRef = nil
		_ = tracker.Create(pvGVR, emptyPV, "") //nolint: errcheck

		fresh := newPVC(nil)
		fresh.Spec.VolumeName = emptyPVName
		fresh.UID = "controller-recreated-uid"
		_ = tracker.Create(pvcGVR, fresh, testNS) //nolint: errcheck

		return true, nil, nil
	})

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not the reserved volume")

	assert.False(t, sourceDeleted, "a failed rewire must never delete the source disk — it is the only good copy")
}

func TestMigrateSameNodeCrossStorageReclaimsSourceStorage(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// Same-node storage move (local-lvm -> zfs on pve-1). The reclaim must delete
	// the SOURCE-storage copy (local-lvm), never the target-storage copy (zfs) the
	// disk was just migrated to.
	disk := "vm-9999-pvc-exist"
	upid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:qmmove:103:root@pam:"
	delUpid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:imgdel:103:root@pam:"
	moved := false

	httpmock.RegisterResponder(http.MethodPost, `=~/nodes/pve-1/storage/local-lvm/content/`+disk,
		func(_ *http.Request) (*http.Response, error) {
			moved = true

			return httpmock.NewJsonResponse(200, map[string]any{"data": upid})
		})
	httpmock.RegisterResponder(http.MethodGet, `=~/nodes/pve-1/tasks/`+upid+`/status`,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": map[string]any{"upid": upid, "node": "pve-1", "status": "stopped", "exitstatus": "OK"},
		}))

	httpmock.RegisterResponder(http.MethodGet, "https://127.0.0.1:8006/api2/json/nodes/pve-1/storage/zfs/content",
		func(_ *http.Request) (*http.Response, error) {
			if moved {
				return httpmock.NewJsonResponse(200, map[string]any{
					"data": []map[string]any{{"volid": "zfs:" + disk, "format": "raw", "size": 5 * gib}},
				})
			}

			return httpmock.NewJsonResponse(200, map[string]any{"data": []map[string]any{}})
		})

	sourceDeleted := false
	targetDeleted := false

	httpmock.RegisterResponder(http.MethodDelete, "https://127.0.0.1:8006/api2/json/nodes/pve-1/storage/local-lvm/content/"+disk,
		func(_ *http.Request) (*http.Response, error) {
			sourceDeleted = true

			return httpmock.NewJsonResponse(200, map[string]any{"data": delUpid})
		})
	httpmock.RegisterResponder(http.MethodDelete, "https://127.0.0.1:8006/api2/json/nodes/pve-1/storage/zfs/content/"+disk,
		func(_ *http.Request) (*http.Response, error) {
			targetDeleted = true

			return httpmock.NewJsonResponse(200, map[string]any{"data": delUpid})
		})
	httpmock.RegisterResponder(http.MethodGet, `=~/nodes/pve-1/tasks/`+delUpid+`/status`,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": map[string]any{"upid": delUpid, "node": "pve-1", "status": "stopped", "exitstatus": "OK"},
		}))

	kclient := fake.NewClientset(newPVC(nil), newPV(disk, nil), newNode(), newCSINode())
	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:     testNS,
		PVCName:       testPVCName,
		TargetNode:    testZone,
		TargetStorage: "zfs",
	})
	require.NoError(t, err)

	assert.True(t, sourceDeleted, "the source-storage (local-lvm) copy must be reclaimed")
	assert.False(t, targetDeleted, "the target-storage (zfs) copy must never be deleted")

	newPV := migratedPV(t, kclient)
	assert.Equal(t, testRegion+"/"+testZone+"/zfs/"+disk, newPV.Spec.CSI.VolumeHandle)
}

func TestMigrateSameNodeSameStorage(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// Same node AND same (explicitly requested) storage: a true no-op.
	kclient := fake.NewClientset(newPVC(nil), newPV("vm-9999-pvc-exist", nil), newNode(), newCSINode())

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:     testNS,
		PVCName:       testPVCName,
		TargetNode:    testZone,
		TargetStorage: testStorage,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, migrator.ErrAlreadyOnTarget)
}

func TestMigrateSameNodeSameStorageDefaulted(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// Same node with no target storage requested: the storage defaults to the
	// volume's current one, so this is still "already on target".
	kclient := fake.NewClientset(newPVC(nil), newPV("vm-9999-pvc-exist", nil), newNode(), newCSINode())

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: testZone,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, migrator.ErrAlreadyOnTarget)
}

func TestMigrateSameNodeSkipsSharedStorageCheck(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// The shared mock lists the source disk in the local-lvm content of every
	// node — the exact condition that returns ErrSharedStorage for cross-node
	// requests (TestMigrateSharedStorageSkip). For a same-node storage move
	// the source disk is trivially present on its own node, so the pre-flight
	// must be skipped: without a POST responder the migration proceeds all the
	// way to the move and fails THERE, not with ErrSharedStorage.
	disk := "vm-9999-pvc-exist"

	kclient := fake.NewClientset(newPVC(nil), newPV(disk, nil), newNode(), newCSINode())

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:     testNS,
		PVCName:       testPVCName,
		TargetNode:    testZone,
		TargetStorage: "zfs",
	})
	require.Error(t, err)
	assert.NotErrorIs(t, err, migrator.ErrSharedStorage)
	assert.Contains(t, err.Error(), "failed to move disk")

	// The PV was not rewired.
	pv, err := kclient.CoreV1().PersistentVolumes().Get(context.Background(), testPVName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, testRegion+"/"+testZone+"/"+testStorage+"/"+disk, pv.Spec.CSI.VolumeHandle, "PV must be untouched")
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

func TestMigrateForceForeignProviderIDFallsBackToVMLookup(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// The node was registered without the Proxmox CCM: foreign providerID and
	// no instance-id annotation. The migrator must resolve the VMID with the
	// same name+UUID VM lookup the CSI controller uses — automatically, with
	// no extra configuration — and complete the migration (previously it
	// errored out and required annotating the node).
	disk := "vm-9999-pvc-exist"
	registerMoveResponder(disk, "local-lvm:"+disk)

	kclient := fake.NewClientset(newPVC(nil), newPV(disk, nil), newForeignNode("11833f4c-341f-4bd3-aad7-f7abed000000"), newCSINode(), newPod())
	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
		Force:      true,
	})
	require.NoError(t, err)

	pv := migratedPV(t, kclient)
	assert.Equal(t, testRegion+"/pve-2/"+testStorage+"/"+disk, pv.Spec.CSI.VolumeHandle)
}

func TestMigrateForceForeignProviderIDNoVMMatch(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	registerTargetContent("pve-2", "local-lvm")

	// The node's SMBIOS UUID matches no Proxmox VM: both the name-based and
	// the UUID-only lookup come up empty, and the migration must fail loudly
	// before any drain or move instead of guessing a VMID.
	kclient := fake.NewClientset(newPVC(nil), newPV("vm-9999-pvc-exist", nil), newForeignNode("00000000-0000-0000-0000-000000000000"), newCSINode(), newPod())

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
		Force:      true,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to resolve Proxmox VMID")
	assert.Contains(t, err.Error(), "no VM matches the node name or system UUID")

	// The VMID is resolved before the force-drain path: nothing was cordoned.
	node, err := kclient.CoreV1().Nodes().Get(context.Background(), "cluster-1-node-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.False(t, node.Spec.Unschedulable)
}

func TestMigrateForceForeignProviderIDFallsBackToUUIDLookup(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// The kubernetes node name shares no prefix with any Proxmox VM name, so
	// the name-based lookup finds nothing. The system-UUID-only lookup must
	// still resolve the VM (VM 100 reports the node's SMBIOS UUID) without
	// requiring the instance-id annotation.
	disk := "vm-9999-pvc-exist"
	registerMoveResponder(disk, "local-lvm:"+disk)

	node := newForeignNode("11833f4c-341f-4bd3-aad7-f7abed000000")
	node.Name = "k8s-worker-a"

	csiNode := newCSINode()
	csiNode.Name = "k8s-worker-a"
	csiNode.Spec.Drivers[0].NodeID = "k8s-worker-a/100"

	pod := newPod()
	pod.Spec.NodeName = "k8s-worker-a"

	kclient := fake.NewClientset(newPVC(nil), newPV(disk, nil), node, csiNode, pod)
	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
		Force:      true,
	})
	require.NoError(t, err)

	pv := migratedPV(t, kclient)
	assert.Equal(t, testRegion+"/pve-2/"+testStorage+"/"+disk, pv.Spec.CSI.VolumeHandle)
}

func TestMigrateForceForeignProviderIDNoSystemUUID(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	registerTargetContent("pve-2", "local-lvm")

	// Foreign providerID, no annotation, and the node reports no SMBIOS
	// system UUID: there is nothing to verify a VM lookup against, so the
	// migration must fail (pointing at the annotation) without guessing.
	kclient := fake.NewClientset(newPVC(nil), newPV("vm-9999-pvc-exist", nil), newForeignNode(""), newCSINode(), newPod())

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
		Force:      true,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to resolve Proxmox VMID")
	assert.Contains(t, err.Error(), "reports no system UUID")

	node, err := kclient.CoreV1().Nodes().Get(context.Background(), "cluster-1-node-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.False(t, node.Spec.Unschedulable)
}

func TestMigrateForceVMFoundInOtherRegionRejected(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	registerTargetContent("pve-2", "local-lvm")

	// The volume's own cluster (cluster-1, 127.0.0.1) has no VM matching the
	// node: an exact-URL responder (beats the shared catch-alls) serves only
	// the storage entries the pre-flight needs. The VM lookup then finds the
	// node's VM in cluster-2 (127.0.0.2, still served by the catch-alls) —
	// that VMID would alias an unrelated VM in the volume's cluster during
	// the detach wait, so the migration must be rejected before any cordon.
	httpmock.RegisterResponder(http.MethodGet, "https://127.0.0.1:8006/api2/json/cluster/resources",
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": proxmox.ClusterResources{
				&proxmox.ClusterResource{ID: "storage/lvm", Type: "storage", PluginType: "lvm", Node: "pve-1", Storage: "local-lvm", Content: "images", Status: "available"},
				&proxmox.ClusterResource{ID: "storage/lvm", Type: "storage", PluginType: "lvm", Node: "pve-2", Storage: "local-lvm", Content: "images", Status: "available"},
			},
		}))

	pool, err := pxpool.NewProxmoxPool([]*pxpool.ProxmoxCluster{
		{URL: "https://127.0.0.1:8006/api2/json", TokenID: "user!token-id", TokenSecret: "secret", Region: testRegion},
		{URL: "https://127.0.0.2:8006/api2/json", TokenID: "user!token-id", TokenSecret: "secret", Region: "cluster-2"},
	})
	require.NoError(t, err)

	kclient := fake.NewClientset(newPVC(nil), newPV("vm-9999-pvc-exist", nil), newForeignNode("11833f4c-341f-4bd3-aad7-f7abed000000"), newCSINode(), newPod())

	m := &migrator.Migrator{KClient: kclient, PClient: pool}

	err = m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
		Force:      true,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "in region cluster-2, but the volume is in region cluster-1")

	node, err := kclient.CoreV1().Nodes().Get(context.Background(), "cluster-1-node-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.False(t, node.Spec.Unschedulable)
}

func TestMigrateForceProviderIDRegionMismatch(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	registerTargetContent("pve-2", "local-lvm")

	// A proxmox:// providerID whose region differs from the volume's would
	// hand the detach wait a VMID that aliases an unrelated VM in the
	// volume's cluster: the migration must be rejected before any cordon,
	// mirroring the region guard on the VM-lookup path.
	node := newNode()
	node.Spec.ProviderID = "proxmox://cluster-2/100"

	kclient := fake.NewClientset(newPVC(nil), newPV("vm-9999-pvc-exist", nil), node, newCSINode(), newPod())

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
		Force:      true,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "providerID places Proxmox VMID 100 in region cluster-2, but the volume is in region cluster-1")

	got, err := kclient.CoreV1().Nodes().Get(context.Background(), "cluster-1-node-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.False(t, got.Spec.Unschedulable)
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
	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.NoError(t, err)

	assert.True(t, deleted, "the partial file must be deleted")
	assert.True(t, moved, "the move must be redone")

	newPV := migratedPV(t, kclient)
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
	simulateClaimRefBinder(kclient)

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

	pv := migratedPV(t, kclient)
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
	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.NoError(t, err)

	newPV := migratedPV(t, kclient)
	assert.Equal(t, testRegion+"/pve-2/"+testStorage+"/"+disk, newPV.Spec.CSI.VolumeHandle)
	assert.NotContains(t, newPV.Annotations, migrator.AnnotationMigrateState)
}

// pvcGVR / pvListGVK are used by the controller-recreation reactors below to
// drive the fake object tracker directly (tracker mutations are invisible to
// the recorded action list, so a simulated binder does not register as a PVC
// update by the migrator).
var (
	pvcGVR    = corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims")
	pvGVR     = corev1.SchemeGroupVersion.WithResource("persistentvolumes")
	pvListGVK = corev1.SchemeGroupVersion.WithKind("PersistentVolume")
)

// reservedPV returns the migrator's reserved (claimRef pre-bind) PV in the
// tracker: the CSI PV that claims the test PVC and is not the original.
func reservedPV(tracker k8stesting.ObjectTracker) *corev1.PersistentVolume {
	obj, err := tracker.List(pvGVR, pvListGVK, "")
	if err != nil {
		return nil
	}

	list, ok := obj.(*corev1.PersistentVolumeList)
	if !ok {
		return nil
	}

	for i := range list.Items {
		pv := &list.Items[i]
		if pv.Name != testPVName && pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Name == testPVCName {
			return pv
		}
	}

	return nil
}

// simulateControllerRecreate installs a delete reactor that re-creates the PVC
// the instant the migrator deletes it — modeling ArgoCD selfHeal / a
// StatefulSet / an operator recreating a managed PVC. The recreated PVC has no
// volumeName (Pending), so only the reserved PV's claimRef can bind it. The
// optional mutators shape the recreated PVC (e.g. the StorageClass an
// admission default would fill in, or an owner-pinned class).
func simulateControllerRecreate(kclient *fake.Clientset, mutators ...func(*corev1.PersistentVolumeClaim)) {
	tracker := kclient.Tracker()

	kclient.PrependReactor("delete", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		da, ok := action.(k8stesting.DeleteAction)
		if !ok || da.GetName() != testPVCName {
			return false, nil, nil
		}

		_ = tracker.Delete(pvcGVR, testNS, testPVCName) //nolint: errcheck

		fresh := newPVC(nil)
		// An owner recreates the PVC from its OWN manifest: it carries none of
		// the binder bookkeeping annotations a bound copy would.
		fresh.ObjectMeta.Annotations = nil
		fresh.Spec.VolumeName = ""
		fresh.UID = "controller-recreated-uid"
		fresh.Status = corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending}

		for _, mutate := range mutators {
			mutate(fresh)
		}

		_ = tracker.Create(pvcGVR, fresh, testNS) //nolint: errcheck

		return true, nil, nil
	})
}

// pvcStorageClass mirrors how the binder reads a PVC's class (nil means "").
func pvcStorageClass(pvc *corev1.PersistentVolumeClaim) string {
	if pvc.Spec.StorageClassName == nil {
		return ""
	}

	return *pvc.Spec.StorageClassName
}

// simulateClaimRefBinder installs a get reactor that models the Kubernetes PV
// binder with REAL apiserver semantics, so a regression cannot slip past it. It
// mutates the tracker directly, so a simulated bind never appears as a PVC
// update action. The three modeled cases mirror what a live apiserver does:
//
//   - A still-Pending PVC WITHOUT spec.volumeName whose reserved PV exists (an
//     empty-UID claimRef to this namespace/name and a matching storageClassName)
//     BINDS via the reservation: the PV.claimRef.UID is populated with the PVC's
//     UID, the PVC's spec.volumeName is set to the reserved PV, and both are
//     marked Bound. This is the claimRef-only reservation path the migrator relies
//     on for BOTH the unmanaged (migrator create) and managed (owner recreate)
//     cases — neither carries a volumeName.
//   - A PVC created WITH spec.volumeName alongside a reserved PV whose claimRef
//     still has an EMPTY UID is the buggy "double pre-bind": a live apiserver
//     leaves the PVC Lost and the PV Available (UID never populated), so the
//     binder does NOT complete it. Reintroducing volumeName in the migrator
//     therefore fails to bind here.
//   - A PVC WITH spec.volumeName pointing at a foreign PV whose claimRef already
//     carries a NON-empty UID that differs from the PVC's (a racing bind) is not
//     the reservation completing either, and is left untouched.
//   - A PVC whose annotations contain pv.kubernetes.io/bind-completed while its
//     spec.volumeName is EMPTY has, per the PV controller, LOST its volume: it
//     is marked Lost and is NEVER considered for binding again. This is the trap
//     a recreated PVC copied from a bound claim (without stripping the binder
//     bookkeeping annotations) falls into on a live cluster.
func simulateClaimRefBinder(kclient *fake.Clientset) {
	tracker := kclient.Tracker()

	kclient.PrependReactor("get", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ga, ok := action.(k8stesting.GetAction)
		if !ok || ga.GetName() != testPVCName {
			return false, nil, nil
		}

		obj, err := tracker.Get(pvcGVR, testNS, testPVCName)
		if err != nil {
			return false, nil, nil //nolint: nilerr
		}

		claim, ok := obj.(*corev1.PersistentVolumeClaim)
		if !ok {
			return false, nil, nil
		}

		claim = claim.DeepCopy()

		// Real PV-controller semantics: bind-completed with an EMPTY volumeName
		// means the claim LOST its volume — phase Lost, never bound again.
		if _, completed := claim.Annotations[annBindCompleted]; completed && claim.Spec.VolumeName == "" {
			claim.Status.Phase = corev1.ClaimLost

			return true, claim, nil
		}

		// Only a still-Pending claim WITHOUT a volumeName is completable via the
		// reservation. A volumeName already set — whether the buggy double
		// pre-bind (reserved PV, empty-UID claimRef) or a racing bind to a
		// foreign, already-claimed PV — is exactly what a live apiserver refuses
		// to turn into a reservation bind, so it is returned as-is (unbound).
		if claim.Spec.VolumeName == "" && claim.Status.Phase != corev1.ClaimBound {
			bindReservation(tracker, claim)
		}

		return true, claim, nil
	})
}

// bindReservation binds the claim to its reserved PV when one exists (an
// empty-UID claimRef to the claim's namespace/name and a matching
// storageClassName), replicating the apiserver: the PV.claimRef.UID is populated
// with the claim's UID and both the PV and PVC are marked Bound. It mutates the
// tracker in place; a claim with no matching reservation is left unbound.
func bindReservation(tracker k8stesting.ObjectTracker, claim *corev1.PersistentVolumeClaim) {
	reserved := reservedPV(tracker)
	if reserved == nil || reserved.Spec.StorageClassName != pvcStorageClass(claim) {
		return
	}

	// Only an empty-UID claimRef that targets THIS claim is a reservation the
	// binder completes; a populated UID means the PV is already bound.
	if ref := reserved.Spec.ClaimRef; ref == nil || ref.UID != "" || ref.Namespace != testNS || ref.Name != claim.Name {
		return
	}

	reserved = reserved.DeepCopy()
	reserved.Spec.ClaimRef.UID = claim.UID
	reserved.Status.Phase = corev1.VolumeBound
	_ = tracker.Update(pvGVR, reserved, "") //nolint: errcheck

	claim.Spec.VolumeName = reserved.Name
	claim.Status.Phase = corev1.ClaimBound
	_ = tracker.Update(pvcGVR, claim, testNS) //nolint: errcheck
}

// assertRetainedBeforeDelete asserts the old PV was patched to Retain before it
// was deleted, so the already-moved disk can never be garbage collected during
// the rewire.
func assertRetainedBeforeDelete(t *testing.T, kclient *fake.Clientset) {
	t.Helper()

	retainPatched := false
	retainBeforeDelete := false

	for _, a := range kclient.Actions() {
		if a.GetVerb() == "patch" && a.GetResource().Resource == "persistentvolumes" {
			if pa, ok := a.(k8stesting.PatchAction); ok && pa.GetName() == testPVName &&
				strings.Contains(string(pa.GetPatch()), "Retain") {
				retainPatched = true
			}
		}

		if a.GetVerb() == "delete" && a.GetResource().Resource == "persistentvolumes" {
			if da, ok := a.(k8stesting.DeleteAction); ok && da.GetName() == testPVName && retainPatched {
				retainBeforeDelete = true
			}
		}
	}

	assert.True(t, retainBeforeDelete, "old PV must be patched to Retain before it is deleted")
}

func TestMigrateManagedPVCRecreatedBindsReservedPV(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// A controller-managed PVC: the instant the migrator deletes it, the
	// controller recreates it (Pending). The claimRef pre-bind must make it bind
	// to the reserved data PV — never to a freshly provisioned empty volume —
	// and the migrator must never update a PVC. The source PV starts as Delete so
	// this also exercises the Retain guard and the reclaim-policy restore.
	disk := "vm-9999-pvc-exist"
	registerMoveResponder(disk, "local-lvm:"+disk)

	pv := newPV(disk, nil)
	pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete

	kclient := fake.NewClientset(newPVC(nil), pv, newNode(), newCSINode())

	simulateControllerRecreate(kclient)
	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.NoError(t, err)

	newPV := migratedPV(t, kclient)
	assert.Equal(t, testRegion+"/pve-2/"+testStorage+"/"+disk, newPV.Spec.CSI.VolumeHandle, "PVC must bind to the reserved data PV, preserving the moved volume handle")
	require.NotNil(t, newPV.Spec.ClaimRef)
	assert.Equal(t, testPVCName, newPV.Spec.ClaimRef.Name)
	assert.Equal(t, "controller-recreated-uid", string(newPV.Spec.ClaimRef.UID), "a completed bind populates the reservation's claimRef UID with the recreated PVC's UID")
	assert.Equal(t, corev1.PersistentVolumeReclaimDelete, newPV.Spec.PersistentVolumeReclaimPolicy, "the reserved PV's reclaim policy must be restored after binding")

	assert.Zero(t, countPVCUpdates(kclient), "the rewire must never update a PVC")
	assertRetainedBeforeDelete(t, kclient)
}

func TestMigrateUnmanagedPVCBindsReservedPV(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// No controller recreates the PVC: the migrator creates it WITHOUT a
	// volumeName and the binder binds it via the reserved PV's claimRef. A
	// Delete-policy source PV exercises the Retain guard and the reclaim-policy
	// restore on the happy path too.
	disk := "vm-9999-pvc-exist"
	registerMoveResponder(disk, "local-lvm:"+disk)

	pv := newPV(disk, nil)
	pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete

	kclient := fake.NewClientset(newPVC(nil), pv, newNode(), newCSINode())
	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.NoError(t, err)

	newPV := migratedPV(t, kclient)
	assert.Equal(t, testRegion+"/pve-2/"+testStorage+"/"+disk, newPV.Spec.CSI.VolumeHandle)
	require.NotNil(t, newPV.Spec.ClaimRef)
	assert.Equal(t, testPVCName, newPV.Spec.ClaimRef.Name, "the reserved PV's claimRef must target the recreated PVC")
	assert.Equal(t, corev1.PersistentVolumeReclaimDelete, newPV.Spec.PersistentVolumeReclaimPolicy)

	// The migrator's own recreate carries no volumeName; the binder set it.
	boundPVC, err := kclient.CoreV1().PersistentVolumeClaims(testNS).Get(context.Background(), testPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, newPV.Name, boundPVC.Spec.VolumeName, "the binder (not the migrator) populated the PVC's volumeName")

	assert.Zero(t, countPVCUpdates(kclient), "the rewire must never update a PVC")
	assertRetainedBeforeDelete(t, kclient)
}

// createdPVCs returns the PersistentVolumeClaims the migrator created through the
// clientset (its own recreate attempts). A controller's recreate mutates the
// tracker directly and never appears as a create action, so this isolates the
// migrator's OWN recreated PVC.
func createdPVCs(kclient *fake.Clientset) []*corev1.PersistentVolumeClaim {
	var pvcs []*corev1.PersistentVolumeClaim

	for _, a := range kclient.Actions() {
		if a.GetVerb() != "create" || a.GetResource().Resource != "persistentvolumeclaims" {
			continue
		}

		ca, ok := a.(k8stesting.CreateAction)
		if !ok {
			continue
		}

		if pvc, ok := ca.GetObject().(*corev1.PersistentVolumeClaim); ok {
			pvcs = append(pvcs, pvc)
		}
	}

	return pvcs
}

func TestMigrateRecreatedPVCHasNoVolumeNameBindsViaClaimRef(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// The migrator must recreate the PVC WITHOUT a spec.volumeName: setting one
	// alongside the reserved PV's empty-UID claimRef is the double pre-bind a live
	// apiserver leaves Lost/Available. Binding must complete via the claimRef
	// alone — the binder populates the PV.claimRef.UID and both go Bound.
	disk := "vm-9999-pvc-exist"
	registerMoveResponder(disk, "local-lvm:"+disk)

	// A controller-recreated PVC carries a UID, so the completed bind has an
	// observable claimRef UID to assert on.
	kclient := fake.NewClientset(newPVC(nil), newPV(disk, nil), newNode(), newCSINode())
	simulateControllerRecreate(kclient)
	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.NoError(t, err)

	// Every PVC the migrator itself created must carry an EMPTY volumeName —
	// reintroducing a volumeName here is the regression this guards.
	created := createdPVCs(kclient)
	require.NotEmpty(t, created, "the migrator must have issued its own recreate")

	for _, c := range created {
		assert.Empty(t, c.Spec.VolumeName, "the migrator must never set spec.volumeName on the recreated PVC")
	}

	// Binding completed via the claimRef: the PVC is Bound to the reserved data
	// PV, and the binder (not the migrator) populated both the PV.claimRef UID
	// and the PVC's volumeName.
	boundPV := migratedPV(t, kclient)
	assert.Equal(t, testRegion+"/pve-2/"+testStorage+"/"+disk, boundPV.Spec.CSI.VolumeHandle)
	require.NotNil(t, boundPV.Spec.ClaimRef)
	assert.Equal(t, testPVCName, boundPV.Spec.ClaimRef.Name)
	assert.NotEmpty(t, boundPV.Spec.ClaimRef.UID, "a completed claimRef bind populates the PV.claimRef UID")
	assert.Equal(t, corev1.VolumeBound, boundPV.Status.Phase, "the reserved PV must be Bound after the claimRef bind")

	boundPVC, err := kclient.CoreV1().PersistentVolumeClaims(testNS).Get(context.Background(), testPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, boundPV.Name, boundPVC.Spec.VolumeName)
	assert.Equal(t, corev1.ClaimBound, boundPVC.Status.Phase)
}

func TestClaimRefBinderRefusesDoublePrebind(t *testing.T) {
	// Invariant, pinned so a regression cannot pass: a PVC carrying a
	// spec.volumeName that points at a reserved PV whose claimRef still has an
	// EMPTY UID is the buggy double pre-bind. A live apiserver never completes it
	// (PVC Lost, PV Available), and neither may the test binder — otherwise
	// reintroducing volumeName in the migrator would silently pass. Clearing the
	// volumeName (the correct claimRef-only shape) then lets the reservation bind.
	reservedName := "pvc-reserved-data"

	reserved := newPV("vm-9999-pvc-exist", nil)
	reserved.Name = reservedName
	reserved.Spec.ClaimRef = &corev1.ObjectReference{
		Kind:       "PersistentVolumeClaim",
		APIVersion: "v1",
		Namespace:  testNS,
		Name:       testPVCName,
		// EMPTY UID: an unbound reservation.
	}

	// Buggily created WITH the volumeName already set (the double pre-bind).
	// The bookkeeping annotations are cleared to isolate the volumeName trap:
	// a migrator-created recreate carries none of them (they are stripped),
	// and the bind-completed Lost trap has its own guard test.
	pvc := newPVC(nil)
	pvc.ObjectMeta.Annotations = nil
	pvc.Spec.VolumeName = reservedName
	pvc.UID = "double-prebind-uid"
	pvc.Status = corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending}

	kclient := fake.NewClientset(pvc, reserved)
	simulateClaimRefBinder(kclient)

	got, err := kclient.CoreV1().PersistentVolumeClaims(testNS).Get(context.Background(), testPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotEqual(t, corev1.ClaimBound, got.Status.Phase, "a double pre-bind must not be marked Bound")

	pv, err := kclient.CoreV1().PersistentVolumes().Get(context.Background(), reservedName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, pv.Spec.ClaimRef)
	assert.Empty(t, pv.Spec.ClaimRef.UID, "the reservation's claimRef UID must stay empty for a double pre-bind")

	// The correct shape: no volumeName. The reservation now binds.
	cleared := got.DeepCopy()
	cleared.Spec.VolumeName = ""
	_, err = kclient.CoreV1().PersistentVolumeClaims(testNS).Update(context.Background(), cleared, metav1.UpdateOptions{})
	require.NoError(t, err)

	bound, err := kclient.CoreV1().PersistentVolumeClaims(testNS).Get(context.Background(), testPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, corev1.ClaimBound, bound.Status.Phase, "a claimRef-only PVC must bind the reservation")
	assert.Equal(t, reservedName, bound.Spec.VolumeName)

	pv, err = kclient.CoreV1().PersistentVolumes().Get(context.Background(), reservedName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "double-prebind-uid", string(pv.Spec.ClaimRef.UID), "the completed reservation populates the PV.claimRef UID")
}

func TestMigrateRecreatedPVCStripsBindAnnotations(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// The source PVC is a live BOUND claim: it carries the PV controller's
	// bind-completed/bound-by-controller bookkeeping, the scheduler's
	// selected-node pin, and the provisioner annotations (the newPVC fixture
	// stamps all five). The migrator's recreate is a DeepCopy of it, so every
	// one of them MUST be stripped at Create time: bind-completed with the
	// cleared volumeName marks the recreated claim Lost (never bound again),
	// and selected-node would pin a WFFC bind to the evacuated node.
	disk := "vm-9999-pvc-exist"
	registerMoveResponder(disk, "local-lvm:"+disk)

	kclient := fake.NewClientset(newPVC(nil), newPV(disk, nil), newNode(), newCSINode())
	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.NoError(t, err)

	// Inspect the PVC object handed to Create, not the stored state: the strip
	// must happen in the manifest the migrator submits.
	created := createdPVCs(kclient)
	require.NotEmpty(t, created, "the migrator must have issued its own recreate")

	for _, c := range created {
		for _, ann := range binderBookkeepingAnnotations {
			assert.NotContains(t, c.Annotations, ann, "the recreated PVC must not carry the binder bookkeeping annotation %s", ann)
		}
	}

	// And binding completed via the reservation.
	boundPV := migratedPV(t, kclient)
	assert.Equal(t, testRegion+"/pve-2/"+testStorage+"/"+disk, boundPV.Spec.CSI.VolumeHandle)

	boundPVC, err := kclient.CoreV1().PersistentVolumeClaims(testNS).Get(context.Background(), testPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, corev1.ClaimBound, boundPVC.Status.Phase, "the stripped recreate must bind the reservation")
}

func TestClaimRefBinderBindCompletedWithoutVolumeIsLost(t *testing.T) {
	// The regression this documents (root-caused live): a PVC recreated as a
	// copy of the old bound claim KEEPS pv.kubernetes.io/bind-completed while
	// its volumeName is cleared. The PV controller then treats the claim as
	// having LOST its volume — the reservation sits Available with its correct
	// empty-UID claimRef, the PVC goes Lost, and the bind wait times out.
	// Removing the annotation is all it takes for the binder to complete the
	// reservation (exactly what unblocked the live migration within seconds).
	reservedName := "pvc-reserved-data"

	reserved := newPV("vm-9999-pvc-exist", nil)
	reserved.Name = reservedName
	reserved.Spec.ClaimRef = &corev1.ObjectReference{
		Kind:       "PersistentVolumeClaim",
		APIVersion: "v1",
		Namespace:  testNS,
		Name:       testPVCName,
		// EMPTY UID: an unbound reservation.
	}

	// The buggy shape: bind-completed retained (the fixture stamps it), no
	// volumeName.
	pvc := newPVC(nil)
	pvc.Spec.VolumeName = ""
	pvc.UID = "lost-claim-uid"
	pvc.Status = corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending}

	kclient := fake.NewClientset(pvc, reserved)
	simulateClaimRefBinder(kclient)

	got, err := kclient.CoreV1().PersistentVolumeClaims(testNS).Get(context.Background(), testPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, corev1.ClaimLost, got.Status.Phase, "bind-completed with an empty volumeName is a LOST claim")
	assert.Empty(t, got.Spec.VolumeName, "a Lost claim is never bound")

	pv, err := kclient.CoreV1().PersistentVolumes().Get(context.Background(), reservedName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, pv.Spec.ClaimRef)
	assert.Empty(t, pv.Spec.ClaimRef.UID, "the reservation must stay Available: a Lost claim never completes it")

	// Stripping the bookkeeping annotations (what the rewire does) makes the
	// same claim bindable again.
	fixed := got.DeepCopy()
	for _, ann := range binderBookkeepingAnnotations {
		delete(fixed.Annotations, ann)
	}

	fixed.Status = corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending}
	_, err = kclient.CoreV1().PersistentVolumeClaims(testNS).Update(context.Background(), fixed, metav1.UpdateOptions{})
	require.NoError(t, err)

	bound, err := kclient.CoreV1().PersistentVolumeClaims(testNS).Get(context.Background(), testPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, corev1.ClaimBound, bound.Status.Phase, "with the annotations stripped the reservation binds")
	assert.Equal(t, reservedName, bound.Spec.VolumeName)

	pv, err = kclient.CoreV1().PersistentVolumes().Get(context.Background(), reservedName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "lost-claim-uid", string(pv.Spec.ClaimRef.UID), "the completed reservation populates the PV.claimRef UID")
}

func TestMigrateManagedPVCReboundToDifferentPVFails(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// The controller recreates the PVC and a racing provisioner binds it to a
	// FRESH EMPTY volume before the reservation can take effect. The migrator
	// must detect the wrong binding and fail loudly — leaving the moved disk safe
	// on its reserved, Retain PV — rather than silently accepting the empty one.
	disk := "vm-9999-pvc-exist"
	registerMoveResponder(disk, "local-lvm:"+disk)

	kclient := fake.NewClientset(newPVC(nil), newPV(disk, nil), newNode(), newCSINode())

	tracker := kclient.Tracker()
	emptyPVName := "pvc-controller-empty"

	kclient.PrependReactor("delete", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		da, ok := action.(k8stesting.DeleteAction)
		if !ok || da.GetName() != testPVCName {
			return false, nil, nil
		}

		_ = tracker.Delete(pvcGVR, testNS, testPVCName) //nolint: errcheck

		// A fresh empty PV the racing provisioner created, and a recreated PVC
		// already bound to it.
		emptyPV := newPV("vm-9999-pvc-empty-provisioned", nil)
		emptyPV.Name = emptyPVName
		emptyPV.Spec.ClaimRef = nil
		_ = tracker.Create(pvGVR, emptyPV, "") //nolint: errcheck

		fresh := newPVC(nil)
		fresh.Spec.VolumeName = emptyPVName
		fresh.UID = "controller-recreated-uid"
		_ = tracker.Create(pvcGVR, fresh, testNS) //nolint: errcheck

		return true, nil, nil
	})

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not the reserved volume")

	assert.Zero(t, countPVCUpdates(kclient), "the rewire must never update a PVC, even on the failure path")

	// The moved disk is preserved: its reserved PV still exists, Available and Retain.
	reserved := reservedPV(tracker)
	require.NotNil(t, reserved, "the reserved data PV must survive so the moved disk is not lost")

	pv, err := kclient.CoreV1().PersistentVolumes().Get(context.Background(), reserved.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, testRegion+"/pve-2/"+testStorage+"/"+disk, pv.Spec.CSI.VolumeHandle)
	assert.Equal(t, corev1.PersistentVolumeReclaimRetain, pv.Spec.PersistentVolumeReclaimPolicy, "the reserved data PV must stay Retain so the moved disk is never garbage collected")
}

// newTestStorageClass returns a StorageClass of this driver naming the given
// Proxmox storage; isDefault marks it as the cluster default.
func newTestStorageClass(name, storage string, isDefault bool) *storagev1.StorageClass {
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: name},
		Provisioner: csi.DriverName,
		Parameters:  map[string]string{csi.StorageIDKey: storage},
	}
	if isDefault {
		sc.Annotations = map[string]string{migrator.AnnotationDefaultStorageClass: "true"}
	}

	return sc
}

// registerCrossStorageMove wires the responders for a cross-storage move of
// the test disk from pve-1/local-lvm to pve-2/zfs (the TestMigrateCrossStorage
// shape, shared by the StorageClass-alignment tests).
//
// nolint: unparam
func registerCrossStorageMove(disk string) {
	upid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:qmmove:103:root@pam:"

	httpmock.RegisterResponder(http.MethodPost, `=~/nodes/pve-1/storage/local-lvm/content/`+disk,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{"data": upid}))
	httpmock.RegisterResponder(http.MethodGet, `=~/nodes/pve-1/tasks/`+upid+`/status`,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": map[string]any{"upid": upid, "node": "pve-1", "status": "stopped", "exitstatus": "OK"},
		}))

	// Shared-storage pre-flight (source storage on the target node) is empty;
	// post-move verification lists the disk on the target storage.
	registerTargetContent("pve-2", "local-lvm")
	registerTargetContent("pve-2", "zfs", "zfs:"+disk)
}

func TestMigrateCrossStorageManagedPVCAdoptsTargetClass(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// Cross-storage move of a CONTROLLER-MANAGED PVC: the owner recreates the
	// PVC from its manifest, which omits storageClassName, so admission fills
	// the cluster DEFAULT class — here proxmox-new, the (single) class of the
	// TARGET storage. The rewired PV/PVC must adopt that class; keeping the old
	// class would make the apiserver refuse the claimRef pre-bind (class
	// mismatch) and hand the PVC to the dynamic provisioner instead.
	disk := "vm-9999-pvc-exist"
	registerCrossStorageMove(disk)

	oldClass := "proxmox-old"

	pvc := newPVC(nil)
	pvc.Spec.StorageClassName = &oldClass

	pv := newPV(disk, nil)
	pv.Spec.StorageClassName = oldClass

	kclient := fake.NewClientset(pvc, pv, newNode(), newCSINode(),
		newTestStorageClass("proxmox-old", "local-lvm", false),
		newTestStorageClass("proxmox-new", "zfs", true))

	newClass := "proxmox-new"

	simulateControllerRecreate(kclient, func(fresh *corev1.PersistentVolumeClaim) {
		// Admission fills the cluster-default StorageClass on the owner's
		// class-less manifest.
		fresh.Spec.StorageClassName = &newClass
	})
	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:     testNS,
		PVCName:       testPVCName,
		TargetNode:    "pve-2",
		TargetStorage: "zfs",
	})
	require.NoError(t, err)

	boundPV := migratedPV(t, kclient)
	assert.Equal(t, testRegion+"/pve-2/zfs/"+disk, boundPV.Spec.CSI.VolumeHandle, "the recreated PVC must bind the reserved data PV")
	assert.Equal(t, "proxmox-new", boundPV.Spec.StorageClassName, "the reserved PV must carry the target storage's class")

	boundPVC, err := kclient.CoreV1().PersistentVolumeClaims(testNS).Get(context.Background(), testPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, boundPVC.Spec.StorageClassName)
	assert.Equal(t, "proxmox-new", *boundPVC.Spec.StorageClassName)

	assert.Zero(t, countPVCUpdates(kclient), "the rewire must never update a PVC")
}

func TestMigrateCrossStorageAmbiguousClassKeepsOldClass(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// TWO StorageClasses name the target storage: adopting either would be a
	// guess, so the rewired PV/PVC must keep the old class (current behavior).
	disk := "vm-9999-pvc-exist"
	registerCrossStorageMove(disk)

	oldClass := "proxmox-old"

	pvc := newPVC(nil)
	pvc.Spec.StorageClassName = &oldClass

	pv := newPV(disk, nil)
	pv.Spec.StorageClassName = oldClass

	kclient := fake.NewClientset(pvc, pv, newNode(), newCSINode(),
		newTestStorageClass("proxmox-new-1", "zfs", true),
		newTestStorageClass("proxmox-new-2", "zfs", false))
	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:     testNS,
		PVCName:       testPVCName,
		TargetNode:    "pve-2",
		TargetStorage: "zfs",
	})
	require.NoError(t, err)

	boundPV := migratedPV(t, kclient)
	assert.Equal(t, testRegion+"/pve-2/zfs/"+disk, boundPV.Spec.CSI.VolumeHandle)
	assert.Equal(t, oldClass, boundPV.Spec.StorageClassName, "an ambiguous class match must fall back to the old class")

	boundPVC, err := kclient.CoreV1().PersistentVolumeClaims(testNS).Get(context.Background(), testPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, boundPVC.Spec.StorageClassName)
	assert.Equal(t, oldClass, *boundPVC.Spec.StorageClassName)
}

func TestMigrateManagedPVCPinnedClassReReservesAndBinds(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// The owner's manifest PINS a storageClassName (StatefulSet
	// volumeClaimTemplates): the recreated PVC carries proxmox-pinned, not the
	// target storage's class the reserved PV was aligned to. The PVC stays
	// Pending (nothing provisions the pinned class), so the migrator must
	// re-reserve the data PV under the OBSERVED class and the claim then binds
	// the data disk — instead of failing loudly on a class mismatch.
	disk := "vm-9999-pvc-exist"
	registerCrossStorageMove(disk)

	oldClass := "proxmox-old"

	pvc := newPVC(nil)
	pvc.Spec.StorageClassName = &oldClass

	pv := newPV(disk, nil)
	pv.Spec.StorageClassName = oldClass

	kclient := fake.NewClientset(pvc, pv, newNode(), newCSINode(),
		newTestStorageClass("proxmox-old", "local-lvm", false),
		newTestStorageClass("proxmox-new", "zfs", true))

	pinned := "proxmox-pinned"

	simulateControllerRecreate(kclient, func(fresh *corev1.PersistentVolumeClaim) {
		fresh.Spec.StorageClassName = &pinned
	})
	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:     testNS,
		PVCName:       testPVCName,
		TargetNode:    "pve-2",
		TargetStorage: "zfs",
	})
	require.NoError(t, err)

	boundPV := migratedPV(t, kclient)
	assert.Equal(t, testRegion+"/pve-2/zfs/"+disk, boundPV.Spec.CSI.VolumeHandle, "the pinned-class PVC must bind the re-reserved DATA PV")
	assert.Equal(t, pinned, boundPV.Spec.StorageClassName, "the re-reserved PV must carry the observed pinned class")

	boundPVC, err := kclient.CoreV1().PersistentVolumeClaims(testNS).Get(context.Background(), testPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, boundPVC.Spec.StorageClassName)
	assert.Equal(t, pinned, *boundPVC.Spec.StorageClassName)

	assert.Zero(t, countPVCUpdates(kclient), "the rewire must never update a PVC")
}

func TestMigrateManagedPVCPinnedClassRacingEmptyPVReplaced(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// Worst case: the owner recreates the PVC with a PINNED class AND a dynamic
	// provisioner immediately binds it to a fresh EMPTY volume. The migrator
	// must delete that racing PVC/PV pair (the empty disk, never the data
	// disk), re-reserve the data PV under the pinned class, and let the owner's
	// SECOND recreate bind the data disk.
	disk := "vm-9999-pvc-exist"
	registerCrossStorageMove(disk)

	oldClass := "proxmox-old"

	pvc := newPVC(nil)
	pvc.Spec.StorageClassName = &oldClass

	pv := newPV(disk, nil)
	pv.Spec.StorageClassName = oldClass

	kclient := fake.NewClientset(pvc, pv, newNode(), newCSINode(),
		newTestStorageClass("proxmox-old", "local-lvm", false),
		newTestStorageClass("proxmox-new", "zfs", true))

	tracker := kclient.Tracker()
	pinned := "proxmox-pinned"
	racingPVName := "pvc-racing-empty"
	deletes := 0

	kclient.PrependReactor("delete", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		da, ok := action.(k8stesting.DeleteAction)
		if !ok || da.GetName() != testPVCName {
			return false, nil, nil
		}

		deletes++

		_ = tracker.Delete(pvcGVR, testNS, testPVCName) //nolint: errcheck

		fresh := newPVC(nil)
		// An owner recreate from its own manifest: no binder bookkeeping.
		fresh.ObjectMeta.Annotations = nil
		fresh.Spec.StorageClassName = &pinned
		fresh.Spec.VolumeName = ""
		fresh.UID = "controller-recreated-uid"
		fresh.Status = corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending}

		if deletes == 1 {
			// The racing provisioner already produced a fresh EMPTY volume and
			// bound the first recreate to it.
			racing := newPV("vm-9999-pvc-racing-empty", nil)
			racing.Name = racingPVName
			// A freshly provisioned PV bound to the recreated PVC claims it back
			// (namespace/name) — the guard requires this to delete it.
			racing.Spec.ClaimRef = &corev1.ObjectReference{
				Kind:       "PersistentVolumeClaim",
				APIVersion: "v1",
				Namespace:  testNS,
				Name:       testPVCName,
				UID:        "controller-recreated-uid",
			}
			racing.Spec.StorageClassName = pinned
			racing.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete
			// Clearly after the (un-truncated) rewire start so the strict-after
			// freshness guard treats it as a racing volume.
			racing.CreationTimestamp = metav1.NewTime(time.Now().Add(time.Minute))
			_ = tracker.Create(pvGVR, racing, "") //nolint: errcheck

			fresh.Spec.VolumeName = racingPVName
			fresh.Status.Phase = corev1.ClaimBound
		}

		_ = tracker.Create(pvcGVR, fresh, testNS) //nolint: errcheck

		return true, nil, nil
	})

	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:     testNS,
		PVCName:       testPVCName,
		TargetNode:    "pve-2",
		TargetStorage: "zfs",
	})
	require.NoError(t, err)
	assert.Equal(t, 2, deletes, "the migrator must delete the racing PVC so the owner recreates it once more")

	_, err = kclient.CoreV1().PersistentVolumes().Get(context.Background(), racingPVName, metav1.GetOptions{})
	require.Error(t, err, "the racing fresh empty PV must be deleted")

	boundPV := migratedPV(t, kclient)
	assert.Equal(t, testRegion+"/pve-2/zfs/"+disk, boundPV.Spec.CSI.VolumeHandle, "the second recreate must bind the re-reserved DATA PV, not an empty one")
	assert.Equal(t, pinned, boundPV.Spec.StorageClassName)
	assert.Zero(t, countPVCUpdates(kclient), "the rewire must never update a PVC")
}

func TestMigrateManagedPVCClassFlipFlopFailsLoudlyAfterCap(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// Pathological owner: every observation of the recreated PVC reports a
	// DIFFERENT pinned class, so no re-reservation can ever match. The
	// adaptation is bounded: after two re-reservations the migration must fail
	// loudly, with the data PV still reserved, Retain and Available.
	disk := "vm-9999-pvc-exist"
	registerCrossStorageMove(disk)

	oldClass := "proxmox-old"

	pvc := newPVC(nil)
	pvc.Spec.StorageClassName = &oldClass

	pv := newPV(disk, nil)
	pv.Spec.StorageClassName = oldClass

	kclient := fake.NewClientset(pvc, pv, newNode(), newCSINode(),
		newTestStorageClass("proxmox-old", "local-lvm", false),
		newTestStorageClass("proxmox-new", "zfs", true))

	pinnedA := "proxmox-pinned-a"

	simulateControllerRecreate(kclient, func(fresh *corev1.PersistentVolumeClaim) {
		fresh.Spec.StorageClassName = &pinnedA
	})

	// Flip the class the recreated PVC reports on every get: the reconciled
	// reservation is always one class behind.
	tracker := kclient.Tracker()
	gets := 0

	kclient.PrependReactor("get", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ga, ok := action.(k8stesting.GetAction)
		if !ok || ga.GetName() != testPVCName {
			return false, nil, nil
		}

		obj, err := tracker.Get(pvcGVR, testNS, testPVCName)
		if err != nil {
			return false, nil, nil //nolint: nilerr
		}

		claim, ok := obj.(*corev1.PersistentVolumeClaim)
		if !ok || claim.UID != "controller-recreated-uid" {
			return false, nil, nil
		}

		claim = claim.DeepCopy()
		gets++

		flip := "proxmox-pinned-a"
		if gets%2 == 0 {
			flip = "proxmox-pinned-b"
		}

		claim.Spec.StorageClassName = &flip

		return true, claim, nil
	})

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:     testNS,
		PVCName:       testPVCName,
		TargetNode:    "pve-2",
		TargetStorage: "zfs",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "after 2 re-reservations", "the adaptation must be bounded")
	assert.ErrorIs(t, err, migrator.ErrInvalidTarget, "a capped class reconciliation must be terminal so the controller stops retrying")

	// The data disk is still protected: its reserved PV exists, Retain, and
	// carries the migrated volume handle.
	reserved := reservedPV(tracker)
	require.NotNil(t, reserved, "the reserved data PV must survive the bounded failure")
	assert.Equal(t, testRegion+"/pve-2/zfs/"+disk, reserved.Spec.CSI.VolumeHandle)
	assert.Equal(t, corev1.PersistentVolumeReclaimRetain, reserved.Spec.PersistentVolumeReclaimPolicy)
	assert.Empty(t, reserved.Spec.ClaimRef.UID, "the surviving reservation must stay pre-bound (empty claimRef UID)")
	assert.Zero(t, countPVCUpdates(kclient), "the rewire must never update a PVC, even while adapting")
}

// newClassQuota returns a ResourceQuota in the test namespace whose status
// carries the given hard/used quantities.
func newClassQuota(name string, hard, used corev1.ResourceList) *corev1.ResourceQuota {
	return &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       corev1.ResourceQuotaSpec{Hard: hard},
		Status:     corev1.ResourceQuotaStatus{Hard: hard, Used: used},
	}
}

// runQuotaBlockedMigration drives a cross-storage migration (class change
// proxmox-old -> proxmox-new) against the given namespace quota and asserts it
// aborts BEFORE the old PVC is deleted.
func runQuotaBlockedMigration(t *testing.T, quota *corev1.ResourceQuota) {
	t.Helper()

	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	disk := "vm-9999-pvc-exist"
	registerCrossStorageMove(disk)

	oldClass := "proxmox-old"

	pvc := newPVC(nil)
	pvc.Spec.StorageClassName = &oldClass

	pv := newPV(disk, nil)
	pv.Spec.StorageClassName = oldClass

	kclient := fake.NewClientset(pvc, pv, newNode(), newCSINode(), quota,
		newTestStorageClass("proxmox-old", "local-lvm", false),
		newTestStorageClass("proxmox-new", "zfs", true))

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:     testNS,
		PVCName:       testPVCName,
		TargetNode:    "pve-2",
		TargetStorage: "zfs",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, migrator.ErrInvalidTarget)
	assert.Contains(t, err.Error(), "resourcequota")

	// Nothing destructive happened: the old PVC is still present, still bound
	// to the old PV, and no PVC delete was ever issued.
	oldPVC, err := kclient.CoreV1().PersistentVolumeClaims(testNS).Get(context.Background(), testPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, testPVName, oldPVC.Spec.VolumeName, "the old PVC must remain bound to the old PV")

	for _, a := range kclient.Actions() {
		if a.GetVerb() == "delete" && a.GetResource().Resource == "persistentvolumeclaims" {
			t.Errorf("the quota pre-flight must abort before any PVC delete, got %v", a)
		}
	}

	_, err = kclient.CoreV1().PersistentVolumes().Get(context.Background(), testPVName, metav1.GetOptions{})
	require.NoError(t, err, "the old PV must remain")
}

func TestMigrateCrossStorageQuotaBlocksBeforePVCDelete(t *testing.T) {
	// The volume requests 5Gi; the target class quota leaves only 4Gi.
	runQuotaBlockedMigration(t, newClassQuota("target-class-storage",
		corev1.ResourceList{"proxmox-new.storageclass.storage.k8s.io/requests.storage": resource.MustParse("14Gi")},
		corev1.ResourceList{"proxmox-new.storageclass.storage.k8s.io/requests.storage": resource.MustParse("10Gi")},
	))
}

func TestMigrateCrossStorageQuotaClaimCountBlocksBeforePVCDelete(t *testing.T) {
	// The target class quota has no PVC slots left.
	runQuotaBlockedMigration(t, newClassQuota("target-class-count",
		corev1.ResourceList{"proxmox-new.storageclass.storage.k8s.io/persistentvolumeclaims": resource.MustParse("2")},
		corev1.ResourceList{"proxmox-new.storageclass.storage.k8s.io/persistentvolumeclaims": resource.MustParse("2")},
	))
}

func TestMigrateCrossStorageQuotaWithHeadroomProceeds(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// The target class quota leaves plenty of headroom (5Gi needed, 90Gi free
	// and 3 of 5 claim slots left): the pre-flight must not block anything.
	disk := "vm-9999-pvc-exist"
	registerCrossStorageMove(disk)

	oldClass := "proxmox-old"

	pvc := newPVC(nil)
	pvc.Spec.StorageClassName = &oldClass

	pv := newPV(disk, nil)
	pv.Spec.StorageClassName = oldClass

	quota := newClassQuota("target-class-roomy",
		corev1.ResourceList{
			"proxmox-new.storageclass.storage.k8s.io/requests.storage":       resource.MustParse("100Gi"),
			"proxmox-new.storageclass.storage.k8s.io/persistentvolumeclaims": resource.MustParse("5"),
		},
		corev1.ResourceList{
			"proxmox-new.storageclass.storage.k8s.io/requests.storage":       resource.MustParse("10Gi"),
			"proxmox-new.storageclass.storage.k8s.io/persistentvolumeclaims": resource.MustParse("2"),
		})

	kclient := fake.NewClientset(pvc, pv, newNode(), newCSINode(), quota,
		newTestStorageClass("proxmox-old", "local-lvm", false),
		newTestStorageClass("proxmox-new", "zfs", true))
	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:     testNS,
		PVCName:       testPVCName,
		TargetNode:    "pve-2",
		TargetStorage: "zfs",
	})
	require.NoError(t, err)

	boundPV := migratedPV(t, kclient)
	assert.Equal(t, testRegion+"/pve-2/zfs/"+disk, boundPV.Spec.CSI.VolumeHandle)
	assert.Equal(t, "proxmox-new", boundPV.Spec.StorageClassName)
}

func TestMigrateIdleWFFCManagedPVCReservationSucceeds(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// An IDLE volume (no consumer pod) whose StorageClass is
	// WaitForFirstConsumer: the managed owner recreates the PVC Pending, and
	// with no consumer it never binds (binding is deferred to pod scheduling).
	// The migrator must treat the intact reservation as SUCCESS instead of
	// timing out — the reserved data PV binds when a consumer later schedules.
	disk := "vm-9999-pvc-exist"
	registerMoveResponder(disk, "local-lvm:"+disk)

	wffc := storagev1.VolumeBindingWaitForFirstConsumer
	sc := newTestStorageClass("proxmox-wffc", testStorage, false)
	sc.VolumeBindingMode = &wffc

	class := "proxmox-wffc"

	pvc := newPVC(nil)
	pvc.Spec.StorageClassName = &class

	pv := newPV(disk, nil)
	pv.Spec.StorageClassName = class

	kclient := fake.NewClientset(pvc, pv, newNode(), newCSINode(), sc)

	simulateControllerRecreate(kclient, func(fresh *corev1.PersistentVolumeClaim) {
		fresh.Spec.StorageClassName = &class
	})
	// Deliberately NO simulateClaimRefBinder: a WFFC claim with no consumer
	// stays Pending forever, yet the migrator must still succeed.

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.NoError(t, err, "an idle WaitForFirstConsumer PVC must not fail the migration")

	// The reserved data PV survives, carrying the moved handle, still pre-bound
	// (empty claimRef UID) so it binds when a consumer schedules.
	reserved := reservedPV(kclient.Tracker())
	require.NotNil(t, reserved, "the reserved data PV must survive to bind a future consumer")
	assert.Equal(t, testRegion+"/pve-2/"+testStorage+"/"+disk, reserved.Spec.CSI.VolumeHandle)
	require.NotNil(t, reserved.Spec.ClaimRef)
	assert.Empty(t, reserved.Spec.ClaimRef.UID, "the reservation stays pre-bound until a consumer schedules")
	assert.Zero(t, countPVCUpdates(kclient), "the rewire must never update a PVC")
}

func TestMigrateWFFCPendingConsumerWaitsForRealBind(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// A WaitForFirstConsumer volume WITH a Pending consumer pod — the
	// force-migration/reactive shape, where the workload's pod is Pending waiting
	// for exactly this volume. The idle-WFFC success shortcut must NOT fire: a
	// Pending consumer still counts, so the migrator waits for the REAL bind the
	// scheduler drives instead of declaring premature success on an unbound PVC.
	disk := "vm-9999-pvc-exist"
	registerMoveResponder(disk, "local-lvm:"+disk)

	wffc := storagev1.VolumeBindingWaitForFirstConsumer
	sc := newTestStorageClass("proxmox-wffc", testStorage, false)
	sc.VolumeBindingMode = &wffc

	class := "proxmox-wffc"

	pvc := newPVC(nil)
	pvc.Spec.StorageClassName = &class

	pv := newPV(disk, nil)
	pv.Spec.StorageClassName = class

	// A Pending consumer pod referencing the PVC. PVCPodUsage (the force-drain
	// pre-flight) ignores Pending pods, so the migration is not force-drained;
	// PVCConsumers counts it, so the WFFC reservation is not treated as idle.
	pod := newPod()
	pod.Status.Phase = corev1.PodPending

	kclient := fake.NewClientset(pvc, pv, newNode(), newCSINode(), sc, pod)

	simulateControllerRecreate(kclient, func(fresh *corev1.PersistentVolumeClaim) {
		fresh.Spec.StorageClassName = &class
	})
	// The binder models the scheduler placing the Pending pod and completing the
	// WFFC bind: the migrator must reach this real bind, not the idle shortcut.
	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.NoError(t, err)

	// The migrator waited for the REAL bind: the PVC is bound to the reserved
	// data PV and the binder populated the PV.claimRef UID — proving it did not
	// take the premature idle-WFFC shortcut (which leaves the PVC unbound).
	boundPV := migratedPV(t, kclient)
	assert.Equal(t, testRegion+"/pve-2/"+testStorage+"/"+disk, boundPV.Spec.CSI.VolumeHandle)
	require.NotNil(t, boundPV.Spec.ClaimRef)
	assert.Equal(t, "controller-recreated-uid", string(boundPV.Spec.ClaimRef.UID), "the WFFC bind completed via the consumer, not the idle shortcut")
	assert.Zero(t, countPVCUpdates(kclient), "the rewire must never update a PVC")
}

func TestMigrateResumeReusesExistingReservation(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// A previous interrupted attempt already moved the disk and left a
	// reservation (Retain, Available, claimRef to the PVC, target-zone handle).
	// The resumed migration must ADOPT that reservation instead of creating a
	// second Retain PV of the same disk.
	disk := "vm-9999-pvc-exist"
	registerMoveResponder(disk, "local-lvm:"+disk)

	movedHandle := testRegion + "/pve-2/" + testStorage + "/" + disk

	existing := newPV(disk, nil)
	existing.Name = "pvc-existing-reservation"
	existing.Spec.CSI.VolumeHandle = movedHandle
	existing.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
	existing.Spec.ClaimRef = &corev1.ObjectReference{
		Kind:       "PersistentVolumeClaim",
		APIVersion: "v1",
		Namespace:  testNS,
		Name:       testPVCName,
	}
	existing.Status.Phase = corev1.VolumeAvailable

	kclient := fake.NewClientset(newPVC(nil), newPV(disk, nil), existing, newNode(), newCSINode())

	simulateControllerRecreate(kclient)
	simulateClaimRefBinder(kclient)

	m := &migrator.Migrator{KClient: kclient, PClient: newProxmoxPool(t)}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:  testNS,
		PVCName:    testPVCName,
		TargetNode: "pve-2",
	})
	require.NoError(t, err)

	// Exactly one PV carries the moved handle, and it is the pre-existing
	// reservation — no duplicate was created.
	pvs, err := kclient.CoreV1().PersistentVolumes().List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)

	matching := []string{}

	for i := range pvs.Items {
		if pvs.Items[i].Spec.CSI != nil && pvs.Items[i].Spec.CSI.VolumeHandle == movedHandle {
			matching = append(matching, pvs.Items[i].Name)
		}
	}

	require.Len(t, matching, 1, "the resume must reuse the existing reservation, not create a duplicate")
	assert.Equal(t, "pvc-existing-reservation", matching[0])

	// And no reserved PV was ever CREATED for the moved disk during this run.
	for _, a := range kclient.Actions() {
		if a.GetVerb() != "create" || a.GetResource().Resource != "persistentvolumes" {
			continue
		}

		ca, ok := a.(k8stesting.CreateAction)
		if !ok {
			continue
		}

		created, ok := ca.GetObject().(*corev1.PersistentVolume)
		if ok && created.Spec.CSI != nil && created.Spec.CSI.VolumeHandle == movedHandle {
			t.Errorf("a duplicate reserved PV was created for the moved disk: %s", created.Name)
		}
	}
}
