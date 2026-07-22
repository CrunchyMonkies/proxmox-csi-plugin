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

// Package migration implements the annotation-driven volume migration
// controller. It watches PersistentVolumeClaims for migration request
// annotations and Nodes for evacuation request annotations, and executes the
// migrations strictly one at a time.
package migration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/migrator"
	tools "github.com/sergelogvinov/proxmox-csi-plugin/pkg/tools/kubernetes"
	volume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientkubernetes "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

// Options configures the migration controller.
type Options struct {
	// MaxAttempts is the number of reconcile attempts before a migration is
	// marked Failed permanently.
	MaxAttempts int
	// TaskTimeout is the Proxmox move-task timeout in seconds.
	TaskTimeout int
	// DrainTimeout bounds the wait for pods to terminate during force-drain.
	DrainTimeout time.Duration
	// DetachTimeout bounds the wait for the disk to detach from a VM.
	DetachTimeout time.Duration
	// Resync is the informer resync period.
	Resync time.Duration

	// PodFollow makes volumes follow their pods: when every pod mounting a
	// PVC is scheduled in a different zone than the volume, the volume is
	// migrated to the pods' zone automatically.
	PodFollow bool
	// PrimaryStorage maps region -> Proxmox node -> storage ID, used as the
	// migration target storage when the volume's storage name does not exist
	// on the pods' zone.
	PrimaryStorage map[string]map[string]string

	// ReactiveEvacuation makes a standard `kubectl drain` transparent for
	// zone-local volumes: when a pod cannot be scheduled because its volume is
	// pinned to a cordoned/tainted node, the controller stamps a migration so
	// the pod can schedule elsewhere. Opt-in; inert when false.
	ReactiveEvacuation bool
	// ReactiveEvacuationGrace is how long a pod must stay unschedulable on a
	// pinned volume before a reactive migration is stamped. This prevents a
	// quick maintenance cordon+reboot from triggering a large copy.
	ReactiveEvacuationGrace time.Duration
}

// DefaultReactiveEvacuationGrace is the default grace period before a reactive
// evacuation migration is stamped.
const DefaultReactiveEvacuationGrace = 2 * time.Minute

// Controller is the annotation-driven migration controller.
type Controller struct {
	kclient  clientkubernetes.Interface
	migrator *migrator.Migrator
	recorder record.EventRecorder
	opts     Options

	factory       informers.SharedInformerFactory
	pvcQueue      workqueue.TypedRateLimitingInterface[string]
	nodeQueue     workqueue.TypedRateLimitingInterface[string]
	followQueue   workqueue.TypedRateLimitingInterface[string]
	reactiveQueue workqueue.TypedRateLimitingInterface[string]
}

// New creates a migration controller.
func New(kclient clientkubernetes.Interface, m *migrator.Migrator, recorder record.EventRecorder, opts Options) *Controller {
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 5
	}

	if opts.Resync <= 0 {
		opts.Resync = 10 * time.Minute
	}

	if opts.ReactiveEvacuationGrace <= 0 {
		opts.ReactiveEvacuationGrace = DefaultReactiveEvacuationGrace
	}

	c := &Controller{
		kclient:  kclient,
		migrator: m,
		recorder: recorder,
		opts:     opts,
		factory:  informers.NewSharedInformerFactory(kclient, opts.Resync),
		pvcQueue: workqueue.NewTypedRateLimitingQueue(
			workqueue.DefaultTypedControllerRateLimiter[string]()),
		nodeQueue: workqueue.NewTypedRateLimitingQueue(
			workqueue.DefaultTypedControllerRateLimiter[string]()),
		followQueue: workqueue.NewTypedRateLimitingQueue(
			workqueue.DefaultTypedControllerRateLimiter[string]()),
		reactiveQueue: workqueue.NewTypedRateLimitingQueue(
			workqueue.DefaultTypedControllerRateLimiter[string]()),
	}

	pvcInformer := c.factory.Core().V1().PersistentVolumeClaims().Informer()
	_, _ = pvcInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{ //nolint: errcheck
		AddFunc: c.enqueuePVC,
		UpdateFunc: func(_, newObj interface{}) {
			c.enqueuePVC(newObj)
		},
	})

	nodeInformer := c.factory.Core().V1().Nodes().Informer()
	_, _ = nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{ //nolint: errcheck
		AddFunc: c.enqueueNode,
		UpdateFunc: func(_, newObj interface{}) {
			c.enqueueNode(newObj)
		},
	})

	if opts.PodFollow || opts.ReactiveEvacuation {
		podInformer := c.factory.Core().V1().Pods().Informer()

		if opts.PodFollow {
			_, _ = podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{ //nolint: errcheck
				AddFunc: c.enqueuePodPVCs,
				UpdateFunc: func(_, newObj interface{}) {
					c.enqueuePodPVCs(newObj)
				},
			})
		}

		if opts.ReactiveEvacuation {
			_, _ = podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{ //nolint: errcheck
				AddFunc: c.enqueueReactivePod,
				UpdateFunc: func(_, newObj interface{}) {
					c.enqueueReactivePod(newObj)
				},
			})
		}
	}

	return c
}

// Run starts the controller and blocks until the context is canceled.
// Migrations are executed by exactly one worker so they are strictly
// serialized: force-drain cordons every CSI node, so concurrent migrations
// must never run.
func (c *Controller) Run(ctx context.Context) {
	defer c.pvcQueue.ShutDown()
	defer c.nodeQueue.ShutDown()
	defer c.followQueue.ShutDown()
	defer c.reactiveQueue.ShutDown()

	klog.InfoS("Starting migration controller")

	c.factory.Start(ctx.Done())

	synced := []cache.InformerSynced{
		c.factory.Core().V1().PersistentVolumeClaims().Informer().HasSynced,
		c.factory.Core().V1().Nodes().Informer().HasSynced,
	}
	if c.opts.PodFollow || c.opts.ReactiveEvacuation {
		synced = append(synced, c.factory.Core().V1().Pods().Informer().HasSynced)
	}

	if !cache.WaitForCacheSync(ctx.Done(), synced...) {
		klog.ErrorS(nil, "Failed to sync informer caches")

		return
	}

	klog.InfoS("Informer caches synced")

	// Exactly one PVC worker: global migration serialization.
	go wait.UntilWithContext(ctx, c.pvcWorker, time.Second)
	// The node worker only stamps annotations, it never migrates.
	go wait.UntilWithContext(ctx, c.nodeWorker, time.Second)

	if c.opts.PodFollow {
		// The follow worker only stamps annotations, it never migrates.
		go wait.UntilWithContext(ctx, c.followWorker, time.Second)
	}

	if c.opts.ReactiveEvacuation {
		// The reactive worker only stamps annotations, it never migrates.
		go wait.UntilWithContext(ctx, c.reactiveWorker, time.Second)
	}

	<-ctx.Done()
	klog.InfoS("Stopping migration controller")
}

func (c *Controller) enqueuePVC(obj interface{}) {
	pvc, ok := obj.(*corev1.PersistentVolumeClaim)
	if !ok {
		return
	}

	if pvc.Annotations[migrator.AnnotationMigrateNode] == "" {
		return
	}

	c.pvcQueue.Add(pvc.Namespace + "/" + pvc.Name)
}

func (c *Controller) enqueueNode(obj interface{}) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return
	}

	if node.Annotations[migrator.AnnotationEvacuate] == "" {
		return
	}

	c.nodeQueue.Add(node.Name)
}

// enqueuePodPVCs enqueues every PVC referenced by a scheduled pod for
// volume-follows-pods evaluation.
func (c *Controller) enqueuePodPVCs(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}

	if pod.Spec.NodeName == "" || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return
	}

	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			c.followQueue.Add(pod.Namespace + "/" + volume.PersistentVolumeClaim.ClaimName)
		}
	}
}

// enqueueReactivePod enqueues a Pending pod that the scheduler could not place
// (PodScheduled=False, reason=Unschedulable) and that references a PVC, for
// reactive-evacuation evaluation.
func (c *Controller) enqueueReactivePod(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}

	if _, unschedulable := podUnschedulableSince(pod); !unschedulable {
		return
	}

	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim != nil {
			c.reactiveQueue.Add(pod.Namespace + "/" + pod.Name)

			return
		}
	}
}

func (c *Controller) pvcWorker(ctx context.Context) {
	for {
		key, quit := c.pvcQueue.Get()
		if quit {
			return
		}

		err := c.reconcilePVC(ctx, key)
		if err != nil {
			klog.ErrorS(err, "Migration failed, requeueing", "pvc", key)
			c.pvcQueue.AddRateLimited(key)
		} else {
			c.pvcQueue.Forget(key)
		}

		c.pvcQueue.Done(key)
	}
}

func (c *Controller) nodeWorker(ctx context.Context) {
	for {
		key, quit := c.nodeQueue.Get()
		if quit {
			return
		}

		err := c.reconcileNode(ctx, key)
		if err != nil {
			klog.ErrorS(err, "Node evacuation request failed, requeueing", "node", key)
			c.nodeQueue.AddRateLimited(key)
		} else {
			c.nodeQueue.Forget(key)
		}

		c.nodeQueue.Done(key)
	}
}

func (c *Controller) followWorker(ctx context.Context) {
	for {
		key, quit := c.followQueue.Get()
		if quit {
			return
		}

		err := c.reconcileFollow(ctx, key)
		if err != nil {
			klog.ErrorS(err, "Volume-follows-pods evaluation failed, requeueing", "pvc", key)
			c.followQueue.AddRateLimited(key)
		} else {
			c.followQueue.Forget(key)
		}

		c.followQueue.Done(key)
	}
}

func (c *Controller) reactiveWorker(ctx context.Context) {
	for {
		key, quit := c.reactiveQueue.Get()
		if quit {
			return
		}

		err := c.reconcileReactivePod(ctx, key)
		if err != nil {
			klog.ErrorS(err, "Reactive evacuation evaluation failed, requeueing", "pod", key)
			c.reactiveQueue.AddRateLimited(key)
		} else {
			c.reactiveQueue.Forget(key)
		}

		c.reactiveQueue.Done(key)
	}
}

// reconcilePVC processes one migration request. A nil return means the key is
// done (success or terminal failure); an error return means retry with backoff.
//
//nolint:gocyclo,cyclop
func (c *Controller) reconcilePVC(ctx context.Context, key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return nil //nolint:nilerr // malformed key, drop it
	}

	pvc, err := c.kclient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}

		return err
	}

	target := pvc.Annotations[migrator.AnnotationMigrateNode]
	if target == "" {
		return nil
	}

	phase := pvc.Annotations[migrator.AnnotationMigratePhase]
	if phase == migrator.PhaseFailed {
		// Terminal until a human clears or changes the request annotations.
		return nil
	}

	// Already on target? Mark completed and strip the request.
	if pvc.Spec.VolumeName != "" {
		pv, perr := c.kclient.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
		if perr == nil && pv.Spec.CSI != nil {
			if vol, verr := volume.NewVolumeFromVolumeID(pv.Spec.CSI.VolumeHandle); verr == nil && vol.Zone() == target {
				return c.patchPVCAnnotations(ctx, namespace, name, map[string]*string{
					migrator.AnnotationMigrateNode:    nil,
					migrator.AnnotationMigrateForce:   nil,
					migrator.AnnotationMigrateStorage: nil,
					migrator.AnnotationMigratePhase:   ptr(migrator.PhaseCompleted),
				})
			}
		}
	}

	force := pvc.Annotations[migrator.AnnotationMigrateForce] == "true"

	// In use without force is a terminal skip, not a retry loop.
	pods, _, err := tools.PVCPodUsage(ctx, c.kclient, namespace, name)
	if err != nil {
		return err
	}

	if len(pods) > 0 && !force {
		c.recorder.Eventf(pvc, corev1.EventTypeWarning, "MigrationSkipped",
			"PVC is in use by pods and the %s annotation is not set", migrator.AnnotationMigrateForce)

		return c.failPVC(ctx, namespace, name, fmt.Sprintf("in use by pods: %v", pods))
	}

	attempts, _ := strconv.Atoi(pvc.Annotations[migrator.AnnotationMigrateAttempts]) //nolint: errcheck
	if attempts >= c.opts.MaxAttempts {
		c.recorder.Eventf(pvc, corev1.EventTypeWarning, "MigrationFailed",
			"giving up after %d attempts", attempts)

		return c.failPVC(ctx, namespace, name, fmt.Sprintf("giving up after %d attempts", attempts))
	}

	annotations := map[string]*string{
		migrator.AnnotationMigratePhase:    ptr(migrator.PhasePending),
		migrator.AnnotationMigrateAttempts: ptr(strconv.Itoa(attempts + 1)),
		migrator.AnnotationMigrateMessage:  nil,
	}
	if pvc.Annotations[migrator.AnnotationMigrateStartedAt] == "" {
		annotations[migrator.AnnotationMigrateStartedAt] = ptr(time.Now().UTC().Format(time.RFC3339))
	}

	if err := c.patchPVCAnnotations(ctx, namespace, name, annotations); err != nil {
		return err
	}

	c.recorder.Eventf(pvc, corev1.EventTypeNormal, "MigrationStarted", "migrating to proxmox node %s", target)
	klog.InfoS("Starting migration", "pvc", key, "target", target, "force", force, "attempt", attempts+1)

	m := *c.migrator
	m.OnPhase = func(phase string) {
		// Best effort: the PVC is deleted and recreated during the rewire step.
		_ = c.patchPVCAnnotations(ctx, namespace, name, map[string]*string{ //nolint: errcheck
			migrator.AnnotationMigratePhase: ptr(phase),
		})
	}

	err = m.Migrate(ctx, migrator.Request{
		Namespace:     namespace,
		PVCName:       name,
		TargetNode:    target,
		TargetStorage: pvc.Annotations[migrator.AnnotationMigrateStorage],
		Force:         force,
		TaskTimeout:   c.opts.TaskTimeout,
		DrainTimeout:  c.opts.DrainTimeout,
		DetachTimeout: c.opts.DetachTimeout,
	})
	if err != nil {
		// Invalid requests are terminal: retrying cannot fix a bad target.
		if errors.Is(err, migrator.ErrInvalidTarget) || errors.Is(err, migrator.ErrInUse) {
			c.recorder.Event(pvc, corev1.EventTypeWarning, "MigrationFailed", err.Error())

			return c.failPVC(ctx, namespace, name, err.Error())
		}

		if errors.Is(err, migrator.ErrAlreadyOnTarget) {
			return c.patchPVCAnnotations(ctx, namespace, name, map[string]*string{
				migrator.AnnotationMigrateNode:    nil,
				migrator.AnnotationMigrateForce:   nil,
				migrator.AnnotationMigrateStorage: nil,
				migrator.AnnotationMigratePhase:   ptr(migrator.PhaseCompleted),
			})
		}

		// Shared storage: the disk is reachable from the target already, no
		// move needed (and running one would interrupt). Terminal skip.
		if errors.Is(err, migrator.ErrSharedStorage) {
			c.recorder.Event(pvc, corev1.EventTypeNormal, "MigrationSkipped", err.Error())

			return c.patchPVCAnnotations(ctx, namespace, name, map[string]*string{
				migrator.AnnotationMigrateNode:    nil,
				migrator.AnnotationMigrateForce:   nil,
				migrator.AnnotationMigrateStorage: nil,
				migrator.AnnotationMigratePhase:   ptr(migrator.PhaseSkipped),
				migrator.AnnotationMigrateMessage: ptr(err.Error()),
			})
		}

		c.recorder.Event(pvc, corev1.EventTypeWarning, "MigrationError", err.Error())

		// Record the error, then retry with backoff.
		_ = c.patchPVCAnnotations(ctx, namespace, name, map[string]*string{ //nolint: errcheck
			migrator.AnnotationMigrateMessage: ptr(err.Error()),
		})

		return err
	}

	klog.InfoS("Migration completed", "pvc", key, "target", target)

	// The PVC was recreated without migration annotations; mark the fresh
	// object Completed (best effort).
	_ = c.patchPVCAnnotations(ctx, namespace, name, map[string]*string{ //nolint: errcheck
		migrator.AnnotationMigratePhase: ptr(migrator.PhaseCompleted),
	})

	return nil
}

// reconcileNode expands a node evacuation request into per-PVC migration
// request annotations.
func (c *Controller) reconcileNode(ctx context.Context, name string) error {
	node, err := c.kclient.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}

		return err
	}

	target := node.Annotations[migrator.AnnotationEvacuate]
	if target == "" {
		return nil
	}

	region, zone := csi.GetNodeTopology(node.Labels)
	if region == "" || zone == "" {
		c.recorder.Event(node, corev1.EventTypeWarning, "EvacuationFailed", "node has no region/zone topology labels")

		return c.patchNodeAnnotations(ctx, name, map[string]*string{migrator.AnnotationEvacuate: nil})
	}

	force := node.Annotations[migrator.AnnotationEvacuateForce] == "true"

	pvs, err := tools.PVsInZone(ctx, c.kclient, csi.DriverName, region, zone)
	if err != nil {
		return err
	}

	stamped := 0

	for i := range pvs {
		pv := &pvs[i]

		if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Kind != "PersistentVolumeClaim" {
			continue
		}

		pvcNS, pvcName := pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name

		pvc, perr := c.kclient.CoreV1().PersistentVolumeClaims(pvcNS).Get(ctx, pvcName, metav1.GetOptions{})
		if perr != nil || pvc.Annotations[migrator.AnnotationMigrateNode] != "" {
			continue
		}

		pvcTarget := target
		if target == "auto" {
			pvcTarget, perr = c.autoTarget(ctx, pv, zone)
			if perr != nil {
				klog.ErrorS(perr, "Failed to select evacuation target", "pv", pv.Name, "zone", zone)
				c.recorder.Eventf(node, corev1.EventTypeWarning, "EvacuationFailed",
					"no target for %s/%s: %v", pvcNS, pvcName, perr)

				continue
			}
		}

		annotations := map[string]*string{migrator.AnnotationMigrateNode: ptr(pvcTarget)}
		if force {
			annotations[migrator.AnnotationMigrateForce] = ptr("true")
		}

		if perr := c.patchPVCAnnotations(ctx, pvcNS, pvcName, annotations); perr != nil {
			return perr
		}

		stamped++
	}

	klog.InfoS("Evacuation requested", "node", name, "zone", zone, "pvcs", stamped)
	c.recorder.Eventf(node, corev1.EventTypeNormal, "EvacuationRequested",
		"requested migration of %d PVCs out of zone %s", stamped, zone)

	// Consume the request annotation.
	return c.patchNodeAnnotations(ctx, name, map[string]*string{
		migrator.AnnotationEvacuate:      nil,
		migrator.AnnotationEvacuateForce: nil,
	})
}

// reconcileFollow evaluates volume-follows-pods for one PVC: when every pod
// mounting the PVC is scheduled in one zone that differs from the volume's
// zone, a migration request is stamped. The target storage is the volume's
// storage name if the pods' zone hosts it, otherwise the configured primary
// storage for that zone.
//
//nolint:gocyclo,cyclop
func (c *Controller) reconcileFollow(ctx context.Context, key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return nil //nolint:nilerr // malformed key, drop it
	}

	pvc, err := c.kclient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}

		return err
	}

	// A migration is already requested, running, or terminally failed.
	if pvc.Annotations[migrator.AnnotationMigrateNode] != "" || pvc.Spec.VolumeName == "" {
		return nil
	}

	pv, err := c.kclient.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}

		return err
	}

	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csi.DriverName {
		return nil
	}

	vol, err := volume.NewVolumeFromVolumeID(pv.Spec.CSI.VolumeHandle)
	if err != nil {
		return nil //nolint:nilerr // not our volume format
	}

	// Shared storage is reachable from every zone, nothing to follow.
	if vol.Zone() == "" {
		return nil
	}

	placement, err := tools.PVCPodPlacement(ctx, c.kclient, namespace, name)
	if err != nil {
		return err
	}

	if len(placement) == 0 {
		return nil
	}

	// Every pod must be settled on a node in the same zone, in the volume's
	// region, and that zone must differ from the volume's.
	podZone := ""

	for _, nodeName := range placement {
		node, nerr := c.kclient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if nerr != nil {
			if apierrors.IsNotFound(nerr) {
				return nil
			}

			return nerr
		}

		region, zone := csi.GetNodeTopology(node.Labels)
		if zone == "" || region != vol.Region() {
			return nil
		}

		if podZone == "" {
			podZone = zone
		} else if podZone != zone {
			// Pods are split across zones; a single-attach volume cannot
			// follow them all.
			return nil
		}
	}

	if podZone == vol.Zone() {
		return nil
	}

	// Match the storage by name on the pods' zone, otherwise fall back to the
	// configured primary storage for that zone.
	cluster, err := c.migrator.PClient.GetProxmoxCluster(vol.Region())
	if err != nil {
		return err
	}

	zones, err := cluster.GetNodesForStorage(ctx, vol.Storage())
	if err != nil {
		return err
	}

	targetStorage := ""

	if !slices.Contains(zones, podZone) {
		targetStorage = c.opts.PrimaryStorage[vol.Region()][podZone]
		if targetStorage == "" {
			c.recorder.Eventf(pvc, corev1.EventTypeWarning, "FollowSkipped",
				"pods moved to zone %s which has neither storage %s nor a configured primary storage", podZone, vol.Storage())

			return nil
		}
	}

	klog.InfoS("Volume follows pods", "pvc", key, "from", vol.Zone(), "to", podZone, "storage", targetStorage)
	c.recorder.Eventf(pvc, corev1.EventTypeNormal, "FollowRequested",
		"all pods moved to zone %s, requesting volume migration", podZone)

	annotations := map[string]*string{migrator.AnnotationMigrateNode: ptr(podZone)}
	if targetStorage != "" {
		annotations[migrator.AnnotationMigrateStorage] = ptr(targetStorage)
	}

	return c.patchPVCAnnotations(ctx, namespace, name, annotations)
}

// reconcileReactivePod reacts to a Pending pod that the scheduler could not
// place. If the pod is blocked because a zone-local proxmox-csi volume is
// pinned to a cordoned/tainted node, and the pod has stayed unschedulable past
// the grace period, it stamps a migration on the PVC so the volume (and the
// pod) can move to a schedulable zone. It reuses the annotation-stamping path
// of reconcileNode; it never calls the migrator directly.
func (c *Controller) reconcileReactivePod(ctx context.Context, key string) error {
	if !c.opts.ReactiveEvacuation {
		return nil
	}

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return nil //nolint:nilerr // malformed key, drop it
	}

	pod, err := c.kclient.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}

		return err
	}

	// Confirm the scheduler actually failed to place the pod.
	since, unschedulable := podUnschedulableSince(pod)
	if !unschedulable {
		return nil
	}

	// Grace period: do not react to a quick maintenance cordon+reboot. Requeue
	// the remaining time and re-check; only proceed once the pod has been stuck
	// long enough that this is a real drain, not a transient reschedule.
	if remaining := c.opts.ReactiveEvacuationGrace - time.Since(since); remaining > 0 {
		c.reactiveQueue.AddAfter(key, remaining)

		return nil
	}

	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim == nil {
			continue
		}

		if err := c.evaluateReactivePVC(ctx, pod, namespace, vol.PersistentVolumeClaim.ClaimName); err != nil {
			return err
		}
	}

	return nil
}

// evaluateReactivePVC decides whether one PVC of an unschedulable pod is
// blocked by a cordoned/tainted zone node and, if so, stamps a migration.
func (c *Controller) evaluateReactivePVC(ctx context.Context, pod *corev1.Pod, namespace, pvcName string) error {
	pvc, err := c.kclient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}

		return err
	}

	// Already requested, running, or terminally decided: do not re-stamp.
	if migrationInFlight(pvc) || pvc.Spec.VolumeName == "" {
		return nil
	}

	pv, err := c.kclient.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}

		return err
	}

	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csi.DriverName {
		return nil
	}

	vol, err := volume.NewVolumeFromVolumeID(pv.Spec.CSI.VolumeHandle)
	if err != nil {
		return nil //nolint:nilerr // not our volume format
	}

	// Shared storage is reachable from every zone; the pod is not blocked on us.
	if vol.Zone() == "" {
		return nil
	}

	blocked, err := c.zoneBlocked(ctx, vol.Region(), vol.Zone())
	if err != nil {
		return err
	}

	// A schedulable node still satisfies the volume's zone: the pod is stuck
	// for some other reason, not a cordoned volume node. Avoid false positives.
	if !blocked {
		return nil
	}

	target, err := c.autoTarget(ctx, pv, vol.Zone())
	if err != nil {
		c.recorder.Eventf(pvc, corev1.EventTypeWarning, "ReactiveEvacuation",
			"pod %s/%s unschedulable: volume pinned to cordoned zone %s but no target zone has capacity: %v",
			pod.Namespace, pod.Name, vol.Zone(), err)

		return nil
	}

	c.recorder.Eventf(pvc, corev1.EventTypeNormal, "ReactiveEvacuation",
		"pod %s/%s unschedulable: volume pinned to cordoned node in zone %s, migrating to %s",
		pod.Namespace, pod.Name, vol.Zone(), target)
	klog.InfoS("Reactive evacuation", "pod", pod.Namespace+"/"+pod.Name, "pvc", namespace+"/"+pvcName,
		"from", vol.Zone(), "to", target)

	return c.patchPVCAnnotations(ctx, namespace, pvcName, map[string]*string{
		migrator.AnnotationMigrateNode:  ptr(target),
		migrator.AnnotationMigrateForce: ptr("true"),
	})
}

// zoneBlocked reports whether every node in the given region/zone is
// unschedulable (cordoned or NoSchedule/NoExecute tainted) while at least one
// such node exists. When a schedulable node still satisfies the zone, the pod
// is not blocked on the volume's placement.
func (c *Controller) zoneBlocked(ctx context.Context, region, zone string) (bool, error) {
	nodes, err := c.kclient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}

	found, blocked := false, false

	for i := range nodes.Items {
		node := &nodes.Items[i]

		nodeRegion, nodeZone := csi.GetNodeTopology(node.Labels)
		if nodeRegion != region || nodeZone != zone {
			continue
		}

		found = true

		if nodeSchedulable(node) {
			return false, nil
		}

		blocked = true
	}

	return found && blocked, nil
}

// migrationInFlight reports whether a migration is already requested or running
// for the PVC (so a reactive trigger must not re-stamp it).
func migrationInFlight(pvc *corev1.PersistentVolumeClaim) bool {
	if pvc.Annotations[migrator.AnnotationMigrateNode] != "" {
		return true
	}

	switch pvc.Annotations[migrator.AnnotationMigratePhase] {
	case migrator.PhasePending, migrator.PhaseDraining, migrator.PhaseMoving, migrator.PhaseRewiring:
		return true
	default:
		return false
	}
}

// nodeSchedulable reports whether the scheduler may place new pods on the node:
// it must not be cordoned nor carry a NoSchedule/NoExecute taint.
func nodeSchedulable(node *corev1.Node) bool {
	if node.Spec.Unschedulable {
		return false
	}

	for _, taint := range node.Spec.Taints {
		if taint.Effect == corev1.TaintEffectNoSchedule || taint.Effect == corev1.TaintEffectNoExecute {
			return false
		}
	}

	return true
}

// podUnschedulableSince returns the time the pod entered the
// PodScheduled=False / Unschedulable state and true when it is currently in it.
// It falls back to the pod's creation time for a freshly recreated pod whose
// condition carries no transition time.
func podUnschedulableSince(pod *corev1.Pod) (time.Time, bool) {
	if pod.Status.Phase != corev1.PodPending {
		return time.Time{}, false
	}

	for _, cond := range pod.Status.Conditions {
		if cond.Type != corev1.PodScheduled {
			continue
		}

		if cond.Status != corev1.ConditionFalse || cond.Reason != corev1.PodReasonUnschedulable {
			return time.Time{}, false
		}

		since := cond.LastTransitionTime.Time
		if since.IsZero() {
			since = pod.CreationTimestamp.Time
		}

		return since, true
	}

	return time.Time{}, false
}

// autoTarget picks the zone with the most free space for the PV's storage.
func (c *Controller) autoTarget(ctx context.Context, pv *corev1.PersistentVolume, sourceZone string) (string, error) {
	vol, err := volume.NewVolumeFromVolumeID(pv.Spec.CSI.VolumeHandle)
	if err != nil {
		return "", err
	}

	cluster, err := c.migrator.PClient.GetProxmoxCluster(vol.Region())
	if err != nil {
		return "", err
	}

	zones, err := migrator.ZoneCapacities(ctx, cluster, vol.Storage())
	if err != nil {
		return "", err
	}

	size := pv.Spec.Capacity[corev1.ResourceStorage]

	return migrator.SelectTarget(zones, []string{sourceZone}, size.Value(), migrator.DefaultHeadroom)
}

func (c *Controller) failPVC(ctx context.Context, namespace, name, message string) error {
	return c.patchPVCAnnotations(ctx, namespace, name, map[string]*string{
		migrator.AnnotationMigratePhase:   ptr(migrator.PhaseFailed),
		migrator.AnnotationMigrateMessage: ptr(message),
	})
}

// patchPVCAnnotations merge-patches annotations on a PVC; nil values delete keys.
func (c *Controller) patchPVCAnnotations(ctx context.Context, namespace, name string, annotations map[string]*string) error {
	patch, err := annotationsPatch(annotations)
	if err != nil {
		return err
	}

	_, err = c.kclient.CoreV1().PersistentVolumeClaims(namespace).Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	return err
}

// patchNodeAnnotations merge-patches annotations on a Node; nil values delete keys.
func (c *Controller) patchNodeAnnotations(ctx context.Context, name string, annotations map[string]*string) error {
	patch, err := annotationsPatch(annotations)
	if err != nil {
		return err
	}

	_, err = c.kclient.CoreV1().Nodes().Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	return err
}

func annotationsPatch(annotations map[string]*string) ([]byte, error) {
	type meta struct {
		Annotations map[string]*string `json:"annotations"`
	}

	return json.Marshal(struct {
		Metadata meta `json:"metadata"`
	}{Metadata: meta{Annotations: annotations}})
}

func ptr(s string) *string {
	return &s
}
