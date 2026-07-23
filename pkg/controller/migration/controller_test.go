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

package migration

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	proxmox "github.com/luthermonson/go-proxmox"
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
	"k8s.io/client-go/tools/record"
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
// PVC carries (mirrors the migrator package tests). The fixtures stamp them on
// every bound source PVC, and the simulated binder enforces the Lost trap they
// set up, so a rewire that copies a bound PVC without stripping them fails
// here the way it fails on a live cluster.
const (
	annBindCompleted          = "pv.kubernetes.io/bind-completed"
	annBoundByController      = "pv.kubernetes.io/bound-by-controller"
	annSelectedNode           = "volume.kubernetes.io/selected-node"
	annStorageProvisioner     = "volume.kubernetes.io/storage-provisioner"
	annBetaStorageProvisioner = "volume.beta.kubernetes.io/storage-provisioner"
)

func newTestController(t *testing.T, objects ...interface{}) (*Controller, *fake.Clientset, *record.FakeRecorder) {
	t.Helper()

	runtimeObjs := make([]interface{}, 0, len(objects))
	runtimeObjs = append(runtimeObjs, objects...)

	kclient := fake.NewClientset()

	for _, obj := range runtimeObjs {
		switch o := obj.(type) {
		case *corev1.PersistentVolumeClaim:
			_, err := kclient.CoreV1().PersistentVolumeClaims(o.Namespace).Create(context.Background(), o, metav1.CreateOptions{})
			require.NoError(t, err)
		case *corev1.PersistentVolume:
			_, err := kclient.CoreV1().PersistentVolumes().Create(context.Background(), o, metav1.CreateOptions{})
			require.NoError(t, err)
		case *corev1.Node:
			_, err := kclient.CoreV1().Nodes().Create(context.Background(), o, metav1.CreateOptions{})
			require.NoError(t, err)
		case *corev1.Pod:
			_, err := kclient.CoreV1().Pods(o.Namespace).Create(context.Background(), o, metav1.CreateOptions{})
			require.NoError(t, err)
		case *storagev1.StorageClass:
			_, err := kclient.StorageV1().StorageClasses().Create(context.Background(), o, metav1.CreateOptions{})
			require.NoError(t, err)
		}
	}

	pool, err := pxpool.NewProxmoxPool([]*pxpool.ProxmoxCluster{
		{
			URL:         "https://127.0.0.1:8006/api2/json",
			TokenID:     "user!token-id",
			TokenSecret: "secret",
			Region:      testRegion,
		},
	})
	require.NoError(t, err)

	recorder := record.NewFakeRecorder(100)

	m := &migrator.Migrator{KClient: kclient, PClient: pool, Recorder: recorder}

	c := New(kclient, m, recorder, Options{MaxAttempts: 3})

	return c, kclient, recorder
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
				Kind:      "PersistentVolumeClaim",
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
	// A live bound PVC carries the binder's bookkeeping annotations; including
	// them in every fixture exercises the rewire's annotation strip.
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
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func newNode(annotations map[string]string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cluster-1-node-1",
			Labels: map[string]string{
				corev1.LabelTopologyRegion: testRegion,
				corev1.LabelTopologyZone:   testZone,
			},
			Annotations: annotations,
		},
		Spec: corev1.NodeSpec{
			ProviderID: "proxmox://" + testRegion + "/100",
		},
	}
}

func getPVC(t *testing.T, c *Controller) *corev1.PersistentVolumeClaim {
	t.Helper()

	pvc, err := c.kclient.CoreV1().PersistentVolumeClaims(testNS).Get(context.Background(), testPVCName, metav1.GetOptions{})
	require.NoError(t, err)

	return pvc
}

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

// pvcStorageClass mirrors how the binder reads a PVC's class (nil means "").
func pvcStorageClass(pvc *corev1.PersistentVolumeClaim) string {
	if pvc.Spec.StorageClassName == nil {
		return ""
	}

	return *pvc.Spec.StorageClassName
}

// simulateClaimRefBinder models the Kubernetes PV binder with real apiserver
// semantics (see the migrator package tests for the full rationale): a
// still-Pending PVC with NO spec.volumeName binds its reserved PV via the
// empty-UID claimRef (populating the PV.claimRef UID and marking both Bound),
// while a PVC that already carries a volumeName is left untouched — the double
// pre-bind a live apiserver never completes. A PVC annotated
// pv.kubernetes.io/bind-completed while its volumeName is EMPTY has LOST its
// volume per the PV controller: it is marked Lost and NEVER bound — the trap a
// recreated PVC copied from a bound claim (without stripping the bookkeeping
// annotations) falls into on a live cluster.
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

		if claim.Spec.VolumeName == "" && claim.Status.Phase != corev1.ClaimBound {
			if reserved := reservedPV(tracker); reserved != nil && reserved.Spec.StorageClassName == pvcStorageClass(claim) {
				if ref := reserved.Spec.ClaimRef; ref != nil && ref.UID == "" && ref.Namespace == testNS && ref.Name == claim.Name {
					reserved = reserved.DeepCopy()
					reserved.Spec.ClaimRef.UID = claim.UID
					reserved.Status.Phase = corev1.VolumeBound
					_ = tracker.Update(pvGVR, reserved, "") //nolint: errcheck

					claim.Spec.VolumeName = reserved.Name
					claim.Status.Phase = corev1.ClaimBound
					_ = tracker.Update(pvcGVR, claim, testNS) //nolint: errcheck
				}
			}
		}

		return true, claim, nil
	})
}

func TestReconcilePVCNoAnnotation(t *testing.T) {
	c, _, _ := newTestController(t, newPVC(nil), newPV("vm-9999-pvc-exist", nil))

	err := c.reconcilePVC(context.Background(), testNS+"/"+testPVCName)
	require.NoError(t, err)

	pvc := getPVC(t, c)
	assert.Empty(t, pvc.Annotations[migrator.AnnotationMigratePhase])
}

func TestReconcilePVCAlreadyOnTarget(t *testing.T) {
	c, _, _ := newTestController(t,
		newPVC(map[string]string{migrator.AnnotationMigrateNode: testZone}),
		newPV("vm-9999-pvc-exist", nil))

	err := c.reconcilePVC(context.Background(), testNS+"/"+testPVCName)
	require.NoError(t, err)

	pvc := getPVC(t, c)
	assert.Empty(t, pvc.Annotations[migrator.AnnotationMigrateNode])
	assert.Equal(t, migrator.PhaseCompleted, pvc.Annotations[migrator.AnnotationMigratePhase])
}

func TestReconcilePVCAlreadyInTargetZoneCompletes(t *testing.T) {
	// migrate-node names the volume's current zone and NO storage change is
	// requested: the volume is truly already on target, so the controller
	// short-circuits to Completed and strips the request without a move.
	c, _, _ := newTestController(t,
		newPVC(map[string]string{migrator.AnnotationMigrateNode: testZone}),
		newPV("vm-9999-pvc-exist", nil))

	err := c.reconcilePVC(context.Background(), testNS+"/"+testPVCName)
	require.NoError(t, err)

	pvc := getPVC(t, c)
	assert.Empty(t, pvc.Annotations[migrator.AnnotationMigrateNode])
	assert.Equal(t, migrator.PhaseCompleted, pvc.Annotations[migrator.AnnotationMigratePhase])
}

func TestReconcilePVCSameNodeSameStorageCompletes(t *testing.T) {
	// migrate-node names the volume's current zone and migrate-storage names the
	// volume's CURRENT storage: nothing actually changes, so the controller must
	// still short-circuit to Completed (no move). No Proxmox responders are
	// registered, so reaching the migrator would fail — proving the short-circuit.
	c, _, _ := newTestController(t,
		newPVC(map[string]string{
			migrator.AnnotationMigrateNode:    testZone,
			migrator.AnnotationMigrateStorage: testStorage,
		}),
		newPV("vm-9999-pvc-exist", nil))

	err := c.reconcilePVC(context.Background(), testNS+"/"+testPVCName)
	require.NoError(t, err)

	pvc := getPVC(t, c)
	assert.Empty(t, pvc.Annotations[migrator.AnnotationMigrateNode])
	assert.Empty(t, pvc.Annotations[migrator.AnnotationMigrateStorage])
	assert.Equal(t, migrator.PhaseCompleted, pvc.Annotations[migrator.AnnotationMigratePhase])
}

func TestReconcilePVCSameNodeStorageChangeMigrates(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	disk := "vm-9999-pvc-exist"
	upid := "UPID:pve-1:003B4235:1DF4ABCA:667C1C45:qmmove:103:root@pam:"
	moved := false

	// A same-node storage move keeps the node (pve-1) and changes only the
	// storage (local-lvm -> zfs). The disk-move POST hits the SOURCE node and
	// storage; the verify + rewire read the TARGET storage on the SAME node.
	httpmock.RegisterResponder(http.MethodPost, `=~/nodes/pve-1/storage/local-lvm/content/`+disk,
		func(_ *http.Request) (*http.Response, error) {
			moved = true

			return httpmock.NewJsonResponse(200, map[string]any{"data": upid})
		})
	httpmock.RegisterResponder(http.MethodGet, "https://127.0.0.1:8006/api2/json/nodes/pve-1/storage/zfs/content",
		func(_ *http.Request) (*http.Response, error) {
			if moved {
				return httpmock.NewJsonResponse(200, map[string]any{"data": []map[string]any{{"volid": "zfs:" + disk, "size": int64(5 * 1024 * 1024 * 1024)}}})
			}

			return httpmock.NewJsonResponse(200, map[string]any{"data": []map[string]any{}})
		})
	httpmock.RegisterResponder(http.MethodGet, `=~/nodes/pve-1/tasks/`+upid+`/status`,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": map[string]any{"upid": upid, "node": "pve-1", "status": "stopped", "exitstatus": "OK"},
		}))

	// migrate-node is the volume's OWN zone, but migrate-storage names a
	// DIFFERENT storage: this is a real same-node cross-storage migration, not an
	// already-on-target no-op, so the controller must perform the move.
	c, kclient, _ := newTestController(t,
		newPVC(map[string]string{
			migrator.AnnotationMigrateNode:    testZone,
			migrator.AnnotationMigrateStorage: "zfs",
		}),
		newPV(disk, nil))

	// The recreated PVC carries no volumeName; the binder binds it to the
	// reserved data PV via the empty-UID claimRef, as a live apiserver would.
	simulateClaimRefBinder(kclient)

	err := c.reconcilePVC(context.Background(), testNS+"/"+testPVCName)
	require.NoError(t, err)

	// The disk was actually moved: NOT a silent already-on-target completion.
	assert.True(t, moved, "a same-node cross-storage request must perform a real disk move")

	pvc := getPVC(t, c)
	require.NotEmpty(t, pvc.Spec.VolumeName)

	pv, err := kclient.CoreV1().PersistentVolumes().Get(context.Background(), pvc.Spec.VolumeName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, testRegion+"/"+testZone+"/zfs/"+disk, pv.Spec.CSI.VolumeHandle,
		"the PV must rewire to the new storage on the same node")

	assert.Empty(t, pvc.Annotations[migrator.AnnotationMigrateNode])
	assert.Equal(t, migrator.PhaseCompleted, pvc.Annotations[migrator.AnnotationMigratePhase])
}

func TestReconcilePVCInUseWithoutForce(t *testing.T) {
	c, _, recorder := newTestController(t,
		newPVC(map[string]string{migrator.AnnotationMigrateNode: "pve-2"}),
		newPV("vm-9999-pvc-exist", nil),
		newPod())

	err := c.reconcilePVC(context.Background(), testNS+"/"+testPVCName)
	require.NoError(t, err)

	pvc := getPVC(t, c)
	assert.Equal(t, migrator.PhaseFailed, pvc.Annotations[migrator.AnnotationMigratePhase])
	assert.Contains(t, pvc.Annotations[migrator.AnnotationMigrateMessage], "in use by pods")

	event := <-recorder.Events
	assert.Contains(t, event, "MigrationSkipped")
}

func TestReconcilePVCFailedIsTerminal(t *testing.T) {
	c, _, _ := newTestController(t,
		newPVC(map[string]string{
			migrator.AnnotationMigrateNode:  "pve-2",
			migrator.AnnotationMigratePhase: migrator.PhaseFailed,
		}),
		newPV("vm-9999-pvc-exist", nil))

	err := c.reconcilePVC(context.Background(), testNS+"/"+testPVCName)
	require.NoError(t, err)

	// Nothing changed: terminal state.
	pvc := getPVC(t, c)
	assert.Equal(t, migrator.PhaseFailed, pvc.Annotations[migrator.AnnotationMigratePhase])
	assert.Equal(t, "pve-2", pvc.Annotations[migrator.AnnotationMigrateNode])
}

func TestReconcilePVCMaxAttempts(t *testing.T) {
	c, _, recorder := newTestController(t,
		newPVC(map[string]string{
			migrator.AnnotationMigrateNode:     "pve-2",
			migrator.AnnotationMigrateAttempts: "3",
		}),
		newPV("vm-9999-pvc-exist", nil))

	err := c.reconcilePVC(context.Background(), testNS+"/"+testPVCName)
	require.NoError(t, err)

	pvc := getPVC(t, c)
	assert.Equal(t, migrator.PhaseFailed, pvc.Annotations[migrator.AnnotationMigratePhase])

	event := <-recorder.Events
	assert.Contains(t, event, "MigrationFailed")
}

func TestReconcilePVCSuccess(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	disk := "vm-9999-pvc-exist"
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
				return httpmock.NewJsonResponse(200, map[string]any{"data": []map[string]any{{"volid": "local-lvm:" + disk, "size": int64(5 * 1024 * 1024 * 1024)}}})
			}

			return httpmock.NewJsonResponse(200, map[string]any{"data": []map[string]any{}})
		})
	httpmock.RegisterResponder(http.MethodGet, `=~/nodes/pve-1/tasks/`+upid+`/status`,
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": map[string]any{"upid": upid, "node": "pve-1", "status": "stopped", "exitstatus": "OK"},
		}))

	c, kclient, _ := newTestController(t,
		newPVC(map[string]string{migrator.AnnotationMigrateNode: "pve-2"}),
		newPV(disk, nil))

	// The recreated PVC carries no volumeName; the binder binds it to the
	// reserved data PV via the empty-UID claimRef, as a live apiserver would.
	simulateClaimRefBinder(kclient)

	err := c.reconcilePVC(context.Background(), testNS+"/"+testPVCName)
	require.NoError(t, err)

	// The rewire reserves a freshly named PV (claimRef pre-bind), so the
	// migrated volume is reached through the PVC's volumeName.
	pvc := getPVC(t, c)
	require.NotEmpty(t, pvc.Spec.VolumeName)

	pv, err := kclient.CoreV1().PersistentVolumes().Get(context.Background(), pvc.Spec.VolumeName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, testRegion+"/pve-2/"+testStorage+"/"+disk, pv.Spec.CSI.VolumeHandle)

	assert.Empty(t, pvc.Annotations[migrator.AnnotationMigrateNode])
	assert.Equal(t, migrator.PhaseCompleted, pvc.Annotations[migrator.AnnotationMigratePhase])
}

func TestReconcilePVCSharedStorageSkip(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// The shared mock lists the disk on every node: the storage is shared
	// with the target, so the controller must skip terminally — request
	// annotations stripped, phase Skipped, MigrationSkipped event.
	c, _, recorder := newTestController(t,
		newPVC(map[string]string{migrator.AnnotationMigrateNode: "pve-2"}),
		newPV("vm-9999-pvc-exist", nil))

	err := c.reconcilePVC(context.Background(), testNS+"/"+testPVCName)
	require.NoError(t, err)

	pvc := getPVC(t, c)
	assert.Empty(t, pvc.Annotations[migrator.AnnotationMigrateNode])
	assert.Equal(t, migrator.PhaseSkipped, pvc.Annotations[migrator.AnnotationMigratePhase])

	events := []string{<-recorder.Events, <-recorder.Events}
	assert.Contains(t, strings.Join(events, " "), "MigrationSkipped")
}

func TestReconcileNodeEvacuate(t *testing.T) {
	c, kclient, recorder := newTestController(t,
		newPVC(nil),
		newPV("vm-9999-pvc-exist", nil),
		newNode(map[string]string{migrator.AnnotationEvacuate: "pve-2"}))

	err := c.reconcileNode(context.Background(), "cluster-1-node-1")
	require.NoError(t, err)

	pvc := getPVC(t, c)
	assert.Equal(t, "pve-2", pvc.Annotations[migrator.AnnotationMigrateNode])
	assert.Empty(t, pvc.Annotations[migrator.AnnotationMigrateForce])

	node, err := kclient.CoreV1().Nodes().Get(context.Background(), "cluster-1-node-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Empty(t, node.Annotations[migrator.AnnotationEvacuate])

	event := <-recorder.Events
	assert.Contains(t, event, "EvacuationRequested")
}

func TestReconcileNodeEvacuateAuto(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	c, _, _ := newTestController(t,
		newPVC(nil),
		newPV("vm-9999-pvc-exist", nil),
		newNode(map[string]string{
			migrator.AnnotationEvacuate:      "auto",
			migrator.AnnotationEvacuateForce: "true",
		}))

	err := c.reconcileNode(context.Background(), "cluster-1-node-1")
	require.NoError(t, err)

	pvc := getPVC(t, c)
	assert.Equal(t, "pve-2", pvc.Annotations[migrator.AnnotationMigrateNode], "auto target must pick the other zone hosting the storage")
	assert.Equal(t, "true", pvc.Annotations[migrator.AnnotationMigrateForce])
}

const gib = int64(1024 * 1024 * 1024)

// newStorageClass returns a StorageClass of this driver naming the given
// Proxmox storage; isDefault marks it as the cluster default.
func newStorageClass(name, storage string, isDefault bool) *storagev1.StorageClass {
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

// registerCrossStorageCluster overrides the shared mock's cluster resources
// with the per-zone storage layout of the observed gap: storA exists only on
// the source zone pve-1, storB only on pve-2 (every zone has its own storage
// name). Exact URLs beat the shared mock's regex catch-alls.
func registerCrossStorageCluster() {
	httpmock.RegisterResponder(http.MethodGet, "https://127.0.0.1:8006/api2/json/cluster/resources?type=storage",
		httpmock.NewJsonResponderOrPanic(200, map[string]any{
			"data": proxmox.ClusterResources{
				&proxmox.ClusterResource{ID: "storage/storA", Type: "storage", PluginType: "zfspool", Node: "pve-1", Storage: "storA", Content: "images", Status: "available"},
				&proxmox.ClusterResource{ID: "storage/storB", Type: "storage", PluginType: "zfspool", Node: "pve-2", Storage: "storB", Content: "images", Status: "available"},
			},
		}))

	for _, s := range []struct{ node, storage string }{{"pve-1", "storA"}, {"pve-2", "storB"}} {
		httpmock.RegisterResponder(http.MethodGet, "https://127.0.0.1:8006/api2/json/nodes/"+s.node+"/storage/"+s.storage+"/status",
			httpmock.NewJsonResponderOrPanic(200, map[string]any{
				"data": map[string]any{"type": "zfspool", "enabled": 1, "active": 1, "content": "images", "total": 100 * gib, "used": 10 * gib, "avail": 90 * gib},
			}))
	}
}

// newCrossStoragePV returns the test PV on storage storA in the test zone.
func newCrossStoragePV() *corev1.PersistentVolume {
	pv := newPV("vm-9999-pvc-exist", nil)
	pv.Spec.CSI.VolumeHandle = testRegion + "/" + testZone + "/storA/vm-9999-pvc-exist"

	return pv
}

func TestReconcileNodeEvacuateAutoCrossStorage(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()
	registerCrossStorageCluster()

	// The volume's storage storA exists ONLY in the source zone; no other zone
	// hosts the name, so same-name targeting has nothing to offer. storB is
	// blessed by the cluster-default StorageClass and pve-2 has capacity: the
	// auto evacuation must stamp BOTH migrate-node and migrate-storage.
	c, _, _ := newTestController(t,
		newPVC(nil),
		newCrossStoragePV(),
		newNode(map[string]string{migrator.AnnotationEvacuate: "auto"}),
		newStorageClass("proxmox-a", "storA", false),
		newStorageClass("proxmox-b", "storB", true))

	err := c.reconcileNode(context.Background(), "cluster-1-node-1")
	require.NoError(t, err)

	pvc := getPVC(t, c)
	assert.Equal(t, "pve-2", pvc.Annotations[migrator.AnnotationMigrateNode], "auto target must pick the zone hosting the blessed storage")
	assert.Equal(t, "storB", pvc.Annotations[migrator.AnnotationMigrateStorage], "the migration must carry the target zone's storage")
}

// newZoneNode creates a kubernetes node in the given Proxmox zone.
func newZoneNode(name, zone string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				corev1.LabelTopologyRegion: testRegion,
				corev1.LabelTopologyZone:   zone,
			},
		},
	}
}

// newPodOn creates a pod scheduled on the given node mounting the test PVC.
// The phase is Pending: a scheduled pod whose volume cannot attach (the
// realistic state after its zone changed) stays Pending.
func newPodOn(name, nodeName string) *corev1.Pod {
	pod := newPod()
	pod.Name = name
	pod.Spec.NodeName = nodeName
	pod.Status.Phase = corev1.PodPending

	return pod
}

func TestReconcileFollowSameStorageName(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// local-lvm exists on pve-1 and pve-2 (test cluster mock); all pods have
	// settled on a node in pve-2.
	c, _, recorder := newTestController(t,
		newPVC(nil),
		newPV("vm-9999-pvc-exist", nil),
		newZoneNode("cluster-1-node-2", "pve-2"),
		newPodOn("test-0", "cluster-1-node-2"))

	err := c.reconcileFollow(context.Background(), testNS+"/"+testPVCName)
	require.NoError(t, err)

	pvc := getPVC(t, c)
	assert.Equal(t, "pve-2", pvc.Annotations[migrator.AnnotationMigrateNode])
	assert.Empty(t, pvc.Annotations[migrator.AnnotationMigrateStorage], "same storage name exists on target, no storage override")

	event := <-recorder.Events
	assert.Contains(t, event, "FollowRequested")
}

func TestReconcileFollowPrimaryStorage(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	// Pods settled on pve-3, which does NOT host local-lvm; the configured
	// primary storage for pve-3 is used instead.
	c, _, _ := newTestController(t,
		newPVC(nil),
		newPV("vm-9999-pvc-exist", nil),
		newZoneNode("cluster-1-node-3", "pve-3"),
		newPodOn("test-0", "cluster-1-node-3"))

	c.opts.PrimaryStorage = map[string]map[string]string{
		testRegion: {"pve-3": "zfs"},
	}

	err := c.reconcileFollow(context.Background(), testNS+"/"+testPVCName)
	require.NoError(t, err)

	pvc := getPVC(t, c)
	assert.Equal(t, "pve-3", pvc.Annotations[migrator.AnnotationMigrateNode])
	assert.Equal(t, "zfs", pvc.Annotations[migrator.AnnotationMigrateStorage])
}

func TestReconcileFollowNoPrimaryStorage(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	c, _, recorder := newTestController(t,
		newPVC(nil),
		newPV("vm-9999-pvc-exist", nil),
		newZoneNode("cluster-1-node-3", "pve-3"),
		newPodOn("test-0", "cluster-1-node-3"))

	err := c.reconcileFollow(context.Background(), testNS+"/"+testPVCName)
	require.NoError(t, err)

	pvc := getPVC(t, c)
	assert.Empty(t, pvc.Annotations[migrator.AnnotationMigrateNode], "no primary storage configured, no migration requested")

	event := <-recorder.Events
	assert.Contains(t, event, "FollowSkipped")
}

func TestReconcileFollowSplitZones(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	c, _, _ := newTestController(t,
		newPVC(nil),
		newPV("vm-9999-pvc-exist", nil),
		newZoneNode("cluster-1-node-1", "pve-1"),
		newZoneNode("cluster-1-node-2", "pve-2"),
		newPodOn("test-0", "cluster-1-node-2"),
		newPodOn("test-1", "cluster-1-node-1"))

	err := c.reconcileFollow(context.Background(), testNS+"/"+testPVCName)
	require.NoError(t, err)

	pvc := getPVC(t, c)
	assert.Empty(t, pvc.Annotations[migrator.AnnotationMigrateNode], "pods split across zones must not trigger a migration")
}

func TestReconcileFollowSameZone(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	c, _, _ := newTestController(t,
		newPVC(nil),
		newPV("vm-9999-pvc-exist", nil),
		newZoneNode("cluster-1-node-1", "pve-1"),
		newPodOn("test-0", "cluster-1-node-1"))

	err := c.reconcileFollow(context.Background(), testNS+"/"+testPVCName)
	require.NoError(t, err)

	pvc := getPVC(t, c)
	assert.Empty(t, pvc.Annotations[migrator.AnnotationMigrateNode], "pods in the volume's zone must not trigger a migration")
}

func TestReconcileFollowAlreadyRequested(t *testing.T) {
	c, _, _ := newTestController(t,
		newPVC(map[string]string{migrator.AnnotationMigrateNode: "pve-2"}),
		newPV("vm-9999-pvc-exist", nil),
		newZoneNode("cluster-1-node-2", "pve-2"),
		newPodOn("test-0", "cluster-1-node-2"))

	err := c.reconcileFollow(context.Background(), testNS+"/"+testPVCName)
	require.NoError(t, err)

	// No Proxmox responders are registered: reaching the storage lookup would
	// have failed, proving the early return.
	pvc := getPVC(t, c)
	assert.Equal(t, "pve-2", pvc.Annotations[migrator.AnnotationMigrateNode])
}

// newUnschedulablePod builds a Pending pod that the scheduler could not place,
// mounting the test PVC, whose PodScheduled=False/Unschedulable condition
// transitioned `age` ago. Backdating the transition time is required because
// time is real in Go tests: the grace period is measured against wall clock.
func newUnschedulablePod(age time.Duration) *corev1.Pod {
	pod := newPod()
	pod.Name = "test-0"
	pod.Spec.NodeName = ""
	pod.Status.Phase = corev1.PodPending
	pod.Status.Conditions = []corev1.PodCondition{
		{
			Type:               corev1.PodScheduled,
			Status:             corev1.ConditionFalse,
			Reason:             corev1.PodReasonUnschedulable,
			LastTransitionTime: metav1.NewTime(time.Now().Add(-age)),
		},
	}

	return pod
}

// cordonedNode returns the test PVC's zone node, cordoned.
func cordonedNode() *corev1.Node {
	node := newNode(nil)
	node.Spec.Unschedulable = true

	return node
}

func newReactiveController(t *testing.T, objects ...interface{}) (*Controller, *fake.Clientset, *record.FakeRecorder) {
	t.Helper()

	c, kclient, recorder := newTestController(t, objects...)
	c.opts.ReactiveEvacuation = true
	c.opts.ReactiveEvacuationGrace = 2 * time.Minute

	return c, kclient, recorder
}

func TestReconcileReactiveEvacuatePastGrace(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()

	c, _, recorder := newReactiveController(t,
		newPVC(nil),
		newPV("vm-9999-pvc-exist", nil),
		cordonedNode(),
		newUnschedulablePod(5*time.Minute))

	err := c.reconcileReactivePod(context.Background(), testNS+"/test-0")
	require.NoError(t, err)

	pvc := getPVC(t, c)
	assert.Equal(t, "pve-2", pvc.Annotations[migrator.AnnotationMigrateNode], "autoTarget must pick the other zone hosting the storage")
	assert.Equal(t, "true", pvc.Annotations[migrator.AnnotationMigrateForce])

	event := <-recorder.Events
	assert.Contains(t, event, "ReactiveEvacuation")
}

func TestReconcileReactiveCrossStorage(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset() //nolint: wsl_v5

	testcluster.SetupMockResponders()
	registerCrossStorageCluster()

	// Zone pve-1 (hosting the volume's storage storA) is fully cordoned and no
	// other zone hosts storA: reactive evacuation must fall back to the
	// default-StorageClass storage storB on pve-2 and stamp migrate-storage
	// alongside migrate-node — previously this failed with "no zone with N
	// bytes available" because only same-name zones were considered.
	c, _, recorder := newReactiveController(t,
		newPVC(nil),
		newCrossStoragePV(),
		cordonedNode(),
		newUnschedulablePod(5*time.Minute),
		newStorageClass("proxmox-a", "storA", false),
		newStorageClass("proxmox-b", "storB", true))

	err := c.reconcileReactivePod(context.Background(), testNS+"/test-0")
	require.NoError(t, err)

	pvc := getPVC(t, c)
	assert.Equal(t, "pve-2", pvc.Annotations[migrator.AnnotationMigrateNode], "the zone hosting the blessed storage must be the target")
	assert.Equal(t, "storB", pvc.Annotations[migrator.AnnotationMigrateStorage], "the migration must carry the target zone's storage")
	assert.Equal(t, "true", pvc.Annotations[migrator.AnnotationMigrateForce])

	event := <-recorder.Events
	assert.Contains(t, event, "ReactiveEvacuation")
	assert.Contains(t, event, "storB", "the event must name the chosen storage")
}

func TestReconcileReactiveWithinGrace(t *testing.T) {
	c, _, _ := newReactiveController(t,
		newPVC(nil),
		newPV("vm-9999-pvc-exist", nil),
		cordonedNode(),
		// Backdated to just inside the grace window: a small remaining delay so
		// the requeue lands quickly and the test stays deterministic.
		newUnschedulablePod(2*time.Minute-200*time.Millisecond))

	err := c.reconcileReactivePod(context.Background(), testNS+"/test-0")
	require.NoError(t, err)

	// Still within the grace window: nothing stamped yet.
	pvc := getPVC(t, c)
	assert.Empty(t, pvc.Annotations[migrator.AnnotationMigrateNode])

	// The pod is requeued (via AddAfter) to be re-checked once the grace period
	// elapses; the delayed item becomes ready shortly after.
	assert.Eventually(t, func() bool { return c.reactiveQueue.Len() > 0 }, 3*time.Second, 20*time.Millisecond,
		"the pod must be requeued to re-check after the grace period")
}

func TestReconcileReactiveUnrelatedReason(t *testing.T) {
	// Pod is unschedulable but the volume's zone node is schedulable (e.g. the
	// pod is stuck on insufficient resources, not a cordoned volume node).
	c, _, _ := newReactiveController(t,
		newPVC(nil),
		newPV("vm-9999-pvc-exist", nil),
		newNode(nil), // not cordoned
		newUnschedulablePod(5*time.Minute))

	err := c.reconcileReactivePod(context.Background(), testNS+"/test-0")
	require.NoError(t, err)

	pvc := getPVC(t, c)
	assert.Empty(t, pvc.Annotations[migrator.AnnotationMigrateNode], "a schedulable volume zone node must not trigger evacuation")
}

func TestReconcileReactiveNodeSchedulable(t *testing.T) {
	// The volume's zone node exists and is schedulable: no stamp.
	c, _, _ := newReactiveController(t,
		newPVC(nil),
		newPV("vm-9999-pvc-exist", nil),
		newNode(nil),
		newUnschedulablePod(5*time.Minute))

	require.NoError(t, c.reconcileReactivePod(context.Background(), testNS+"/test-0"))

	pvc := getPVC(t, c)
	assert.Empty(t, pvc.Annotations[migrator.AnnotationMigrateNode])
}

func TestReconcileReactiveDisabled(t *testing.T) {
	c, _, _ := newTestController(t, // ReactiveEvacuation left off
		newPVC(nil),
		newPV("vm-9999-pvc-exist", nil),
		cordonedNode(),
		newUnschedulablePod(5*time.Minute))

	err := c.reconcileReactivePod(context.Background(), testNS+"/test-0")
	require.NoError(t, err)

	pvc := getPVC(t, c)
	assert.Empty(t, pvc.Annotations[migrator.AnnotationMigrateNode], "feature disabled: nothing must be stamped")
}

func TestReconcileReactiveAlreadyMigrating(t *testing.T) {
	c, _, _ := newReactiveController(t,
		newPVC(map[string]string{migrator.AnnotationMigrateNode: "pve-2"}),
		newPV("vm-9999-pvc-exist", nil),
		cordonedNode(),
		newUnschedulablePod(5*time.Minute))

	err := c.reconcileReactivePod(context.Background(), testNS+"/test-0")
	require.NoError(t, err)

	// The existing request must be left untouched (no Proxmox responders are
	// registered: reaching autoTarget would have failed, proving the skip).
	pvc := getPVC(t, c)
	assert.Equal(t, "pve-2", pvc.Annotations[migrator.AnnotationMigrateNode])
}

func TestEnqueueReactiveFilter(t *testing.T) {
	c, _, _ := newReactiveController(t)

	// Running pod: not enqueued.
	c.enqueueReactivePod(newPod())
	assert.Equal(t, 0, c.reactiveQueue.Len(), "a running pod must not be enqueued")

	// Pending+Unschedulable pod with a PVC: enqueued.
	c.enqueueReactivePod(newUnschedulablePod(time.Minute))
	assert.Equal(t, 1, c.reactiveQueue.Len(), "an unschedulable pod with a PVC must be enqueued")
}

func TestEnqueueFilters(t *testing.T) {
	c, _, _ := newTestController(t)

	c.enqueuePVC(newPVC(nil))
	assert.Equal(t, 0, c.pvcQueue.Len(), "PVC without migrate-node must not be enqueued")

	c.enqueuePVC(newPVC(map[string]string{migrator.AnnotationMigrateNode: "pve-2"}))
	assert.Equal(t, 1, c.pvcQueue.Len())

	c.enqueueNode(newNode(nil))
	assert.Equal(t, 0, c.nodeQueue.Len(), "node without evacuate must not be enqueued")

	c.enqueueNode(newNode(map[string]string{migrator.AnnotationEvacuate: "auto"}))
	assert.Equal(t, 1, c.nodeQueue.Len())

	unscheduled := newPod()
	unscheduled.Spec.NodeName = ""
	c.enqueuePodPVCs(unscheduled)
	assert.Equal(t, 0, c.followQueue.Len(), "unscheduled pod must not be enqueued")

	c.enqueuePodPVCs(newPodOn("test-0", "cluster-1-node-2"))
	assert.Equal(t, 1, c.followQueue.Len(), "scheduled pod with a PVC must be enqueued")
}
