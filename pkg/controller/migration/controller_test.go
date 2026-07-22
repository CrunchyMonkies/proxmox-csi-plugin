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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/migrator"
	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	testcluster "github.com/sergelogvinov/proxmox-csi-plugin/test/cluster"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
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

	err := c.reconcilePVC(context.Background(), testNS+"/"+testPVCName)
	require.NoError(t, err)

	pv, err := kclient.CoreV1().PersistentVolumes().Get(context.Background(), testPVName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, testRegion+"/pve-2/"+testStorage+"/"+disk, pv.Spec.CSI.VolumeHandle)

	pvc := getPVC(t, c)
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
