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

// Package storage publishes the Proxmox storage catalog into the management
// cluster as ProxmoxStorage objects.
//
// This is what keeps GetCapacity off the hot path. csi-provisioner calls it very
// frequently, and a tenant that served it with a live request to this cluster
// would turn every provisioning decision into a round trip across a cluster
// boundary. Tenants read these objects from a watch-backed cache instead, which
// is also why a management outage degrades capacity reporting to "stale" rather
// than to "failed".
package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DefaultSyncPeriod is how often the catalog is refreshed.
//
// The figure it replaces is the driver's one-minute in-process capacity cache,
// so this is not a regression in freshness as long as it stays near it. Going
// much lower buys nothing: Proxmox's own numbers move as volumes are written to,
// not as they are provisioned.
const DefaultSyncPeriod = time.Minute

// DefaultStaleAfter is how long a failing sync is tolerated before the operator
// reports itself unready.
//
// Deliberately several periods rather than one. A single failed poll of a
// hypervisor API is normal; reporting unready on it would make the operator
// flap, and an operator that restarts under load is worse than one serving
// capacity figures a few minutes old.
const DefaultStaleAfter = 5 * time.Minute

// Publisher keeps ProxmoxStorage objects in step with Proxmox.
//
// It is a Runnable rather than a Reconciler because nothing in the cluster
// changes when a storage fills up -- there is no event to watch, only a
// hypervisor to ask.
type Publisher struct {
	// Client is the management cluster client.
	Client client.Client
	// Proxmox reads the catalog. Read-only by type.
	Proxmox proxmox.StorageReader
	// SyncPeriod defaults to DefaultSyncPeriod.
	SyncPeriod time.Duration
	// StaleAfter defaults to DefaultStaleAfter.
	StaleAfter time.Duration
	// Now defaults to time.Now. Injected so tests do not sleep.
	Now func() time.Time

	mu         sync.Mutex
	lastSync   time.Time
	lastErr    error
	syncedOnce bool
}

var (
	_ manager.Runnable               = (*Publisher)(nil)
	_ manager.LeaderElectionRunnable = (*Publisher)(nil)
)

// The permissions the publisher needs, and no more. The chart's ClusterRole is
// generated from these markers in milestone 5b rather than hand-written, so a
// reviewer checking what the single credential-holding process can do reads the
// same list the code declares.
//
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxstorages,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxstorages/status,verbs=get;update;patch

// NeedLeaderElection reports that only the leader publishes.
//
// Two operators writing the same objects would not corrupt anything -- the
// catalog is derived, so the loser of a race simply writes the same numbers --
// but it doubles the load on the Proxmox API for nothing.
func (p *Publisher) NeedLeaderElection() bool { return true }

// Start syncs immediately and then on a ticker until the context is canceled.
func (p *Publisher) Start(ctx context.Context) error {
	period := p.SyncPeriod
	if period <= 0 {
		period = DefaultSyncPeriod
	}

	logger := log.FromContext(ctx).WithName("storage-publisher")
	ctx = log.IntoContext(ctx, logger)

	logger.Info("starting", "syncPeriod", period)

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		// A sync error is logged and retried on the next tick rather than
		// returned: returning would stop the manager, and a hypervisor that is
		// briefly unreachable is not a reason to take the operator down.
		if err := p.Sync(ctx); err != nil {
			logger.Error(err, "sync failed")
		}

		select {
		case <-ctx.Done():
			logger.Info("stopping")

			return nil
		case <-ticker.C:
		}
	}
}

// Check reports the publisher's health, for manager.AddReadyzCheck.
//
// Unready means "the catalog I am serving may be wrong", which is the honest
// signal to give a tenant reading capacity from it.
func (p *Publisher) Check(_ *http.Request) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.syncedOnce {
		return errors.New("storage catalog has not synced yet")
	}

	staleAfter := p.StaleAfter
	if staleAfter <= 0 {
		staleAfter = DefaultStaleAfter
	}

	if age := p.now().Sub(p.lastSync); age > staleAfter {
		return fmt.Errorf("storage catalog last synced %s ago: %w", age.Truncate(time.Second), p.lastErr)
	}

	return nil
}

// Sync brings the published catalog in line with Proxmox once.
//
// Regions are independent: a region that cannot be read leaves its published
// objects exactly as they were, and the others still update. That is the whole
// reason the loop does not bail on the first error.
func (p *Publisher) Sync(ctx context.Context) error {
	logger := log.FromContext(ctx)

	var errs []error

	for _, region := range p.Proxmox.Regions() {
		if err := p.syncRegion(ctx, region); err != nil {
			errs = append(errs, err)

			logger.Error(err, "region not synced", "region", region)
		}
	}

	err := errors.Join(errs...)

	p.mu.Lock()
	defer p.mu.Unlock()

	p.lastErr = err
	// A region that failed leaves the last good figures published, so a partial
	// sync still counts as a sync for staleness. What it must not do is count as
	// a *successful* one, which is why lastErr is recorded either way and Check
	// reports it.
	p.lastSync = p.now()
	p.syncedOnce = true

	return err
}

// syncRegion upserts one region's storages and removes the ones that are gone.
func (p *Publisher) syncRegion(ctx context.Context, region string) error {
	storages, err := p.Proxmox.ListStorages(ctx, region)
	if err != nil {
		// No pruning on this path, and that is the point. An error means "I do
		// not know what exists", which is not the same as "nothing exists" --
		// treating them alike would delete the whole catalog for a region every
		// time its hypervisor hiccuped.
		return err
	}

	published := map[string]struct{}{}

	var errs []error

	for _, storage := range storages {
		name := ObjectName(storage.Region, storage.Zone, storage.Name)
		published[name] = struct{}{}

		if err := p.upsert(ctx, name, storage); err != nil {
			errs = append(errs, fmt.Errorf("publishing %s: %w", name, err))
		}
	}

	if err := errors.Join(errs...); err != nil {
		// Pruning is skipped after a write failure too: the published set is
		// only trustworthy as a survivor list if every survivor was written.
		return err
	}

	return p.prune(ctx, region, published)
}

// upsert writes one storage's spec and status.
func (p *Publisher) upsert(ctx context.Context, name string, storage proxmox.Storage) error {
	obj := &v1alpha1.ProxmoxStorage{}
	obj.Name = name

	if _, err := controllerutil.CreateOrUpdate(ctx, p.Client, obj, func() error {
		if obj.Labels == nil {
			obj.Labels = map[string]string{}
		}

		obj.Labels[v1alpha1.LabelRegion] = labelValue(storage.Region)
		obj.Labels[v1alpha1.LabelStorage] = labelValue(storage.Name)

		obj.Spec.Region = storage.Region
		obj.Spec.Zone = storage.Zone
		obj.Spec.Storage = storage.Name
		obj.Spec.Shared = storage.Shared
		obj.Spec.PluginType = storage.PluginType
		obj.Spec.Types = storage.Types

		return nil
	}); err != nil {
		return err
	}

	now := metav1.NewTime(p.now())

	obj.Status.Active = storage.Active
	obj.Status.TotalBytes = storage.TotalBytes
	obj.Status.AvailableBytes = storage.AvailableBytes
	obj.Status.UsedBytes = storage.UsedBytes
	obj.Status.LastSyncTime = &now

	condition := metav1.Condition{
		Type:               v1alpha1.ConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             v1alpha1.ReasonAccepted,
		Message:            "storage is active",
		LastTransitionTime: now,
		ObservedGeneration: obj.Generation,
	}

	switch {
	case storage.StatusErr != nil:
		condition.Status = metav1.ConditionFalse
		condition.Reason = v1alpha1.ReasonProxmoxError
		condition.Message = storage.StatusErr.Error()
	case !storage.Active:
		condition.Status = metav1.ConditionFalse
		condition.Reason = v1alpha1.ReasonProxmoxError
		condition.Message = "storage is not active on this node"
	}

	apimeta.SetStatusCondition(&obj.Status.Conditions, condition)

	return p.Client.Status().Update(ctx, obj)
}

// prune deletes published storages a region no longer reports.
func (p *Publisher) prune(ctx context.Context, region string, published map[string]struct{}) error {
	list := &v1alpha1.ProxmoxStorageList{}
	if err := p.Client.List(ctx, list, client.MatchingLabels{v1alpha1.LabelRegion: labelValue(region)}); err != nil {
		return fmt.Errorf("listing published storages for %s: %w", region, err)
	}

	logger := log.FromContext(ctx)

	var errs []error

	for i := range list.Items {
		obj := &list.Items[i]

		// The label is a selector, not proof. Match on the spec so a hand-edited
		// label cannot make this delete another region's objects.
		if obj.Spec.Region != region {
			continue
		}

		if _, ok := published[obj.Name]; ok {
			continue
		}

		// Deleting the catalog entry says the storage is gone from Proxmox. It
		// destroys no data and no volume record: ProxmoxVolume objects are a
		// separate kind with their own finalizers, and one referencing a storage
		// that has disappeared is exactly the drift the detector is for.
		if err := p.Client.Delete(ctx, obj, client.Preconditions{UID: &obj.UID}); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("removing %s: %w", obj.Name, err))

			continue
		}

		logger.Info("storage no longer published by Proxmox, removed", "name", obj.Name, "region", region)
	}

	return errors.Join(errs...)
}

func (p *Publisher) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}

	return time.Now()
}
