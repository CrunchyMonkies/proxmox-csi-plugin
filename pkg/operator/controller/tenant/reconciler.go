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

// Package tenant reconciles TenantCluster registrations.
//
// It answers two questions for every other reconciler in the operator: which
// VMIDs a tenant is allowed to attach to, and how much of its quota it has
// already spent. Both answers are derived rather than remembered -- the VMID set
// is re-resolved from live pool membership on a timer, and usage is rebuilt by
// listing the ledger -- which is what makes a recycled VMID lose access without
// anyone editing an object, and a crash mid-reservation self-heal instead of
// leaking budget.
//
// Nothing here writes to Proxmox. The reader it is handed cannot.
package tenant

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DefaultResolvePeriod is how often pool membership is re-read.
//
// This is a security window, not a refresh interval: a VM removed from a
// tenant's pool keeps its access until the next resolution lands. It is set to
// match the storage publisher's period because the underlying call is the same
// order of cost -- one /cluster/resources read per region -- and because a
// number a reader has already seen elsewhere in this operator is one less thing
// to reason about.
const DefaultResolvePeriod = time.Minute

// Reconciler keeps TenantCluster status in step with Proxmox and with the
// ledger.
type Reconciler struct {
	// Client is the management cluster client.
	Client client.Client
	// Proxmox resolves pool membership. Read-only by type.
	Proxmox proxmox.VMReader
	// ResolvePeriod defaults to DefaultResolvePeriod.
	ResolvePeriod time.Duration
	// Now defaults to time.Now. Injected so tests do not sleep.
	Now func() time.Time
}

// The permissions this reconciler needs, and no more.
//
// Note what is absent: no create and no delete on tenantclusters, because
// registering or removing a tenant is an operational act and never the
// operator's own; and only reads on proxmoxvolumes, because accounting is
// derived from the ledger rather than written into it. Together they are why
// the operator as it stands today cannot bring a volume record into existence.
//
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=tenantclusters,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=tenantclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=tenantclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxvolumes,verbs=get;list;watch

// SetupWithManager registers the reconciler.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("tenantcluster").
		For(&v1alpha1.TenantCluster{}).
		Watches(&v1alpha1.ProxmoxVolume{}, handler.EnqueueRequestsFromMapFunc(r.tenantsForVolume)).
		Complete(r)
}

// Reconcile resolves one tenant's VMIDs and rebuilds its usage.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	tenant := &v1alpha1.TenantCluster{}
	if err := r.Client.Get(ctx, req.NamespacedName, tenant); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !tenant.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, tenant)
	}

	if controllerutil.AddFinalizer(tenant, v1alpha1.TenantFinalizer) {
		if err := r.Client.Update(ctx, tenant); err != nil {
			return ctrl.Result{}, fmt.Errorf("adding the finalizer to %s: %w", tenant.Name, err)
		}
	}

	return r.reconcileNormal(ctx, tenant)
}

// tenantsForVolume maps a volume event back to the registration that accounts
// for it.
//
// A list rather than a field index: TenantClusters number in the single digits
// and are served from the manager's cache, so an index would buy nothing and
// cost a second thing to keep correct.
func (r *Reconciler) tenantsForVolume(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &v1alpha1.TenantClusterList{}
	if err := r.Client.List(ctx, list); err != nil {
		log.FromContext(ctx).Error(err, "listing tenants for a volume event")

		return nil
	}

	var requests []reconcile.Request

	for i := range list.Items {
		if list.Items[i].Spec.Namespace == obj.GetNamespace() {
			requests = append(requests, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(&list.Items[i]),
			})
		}
	}

	return requests
}

// reconcileNormal is the steady-state path.
func (r *Reconciler) reconcileNormal(ctx context.Context, tenant *v1alpha1.TenantCluster) (ctrl.Result, error) {
	// Usage first, and regardless of whether the registration is admitted
	// below. It is derived from objects in this cluster alone, so it stays
	// truthful even for a refused registration -- and an operator untangling a
	// duplicate needs to see which of the two is actually holding volumes.
	if err := r.aggregate(ctx, tenant); err != nil {
		return ctrl.Result{}, err
	}

	conflict, err := r.conflict(ctx, tenant)
	if err != nil {
		return ctrl.Result{}, err
	}

	var resolveErr error

	switch {
	case conflict != "":
		// Fail closed: a registration that lost a uniqueness contest must not
		// keep an authorization set that another registration also claims.
		tenant.Status.ResolvedVMIDs = nil
		tenant.Status.VMIDsObservedAt = nil

		r.setCondition(tenant, v1alpha1.ConditionAdmitted, metav1.ConditionFalse, v1alpha1.ReasonInvalidSpec, conflict)
		r.setCondition(tenant, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonInvalidSpec, conflict)
	default:
		r.setCondition(tenant, v1alpha1.ConditionAdmitted, metav1.ConditionTrue, v1alpha1.ReasonAccepted,
			fmt.Sprintf("registered for namespace %s in region %s", tenant.Spec.Namespace, tenant.Spec.Region))

		resolveErr = r.resolveVMIDs(ctx, tenant)
		r.setReady(tenant, resolveErr)
	}

	if err := r.updateStatus(ctx, tenant); err != nil {
		return ctrl.Result{}, err
	}

	// The Proxmox error is returned only after status is written, so the
	// staleness the next reconcile will act on is already published. Returning
	// it is what gets the exponential backoff; the timer below is the ceiling.
	return ctrl.Result{RequeueAfter: r.resolvePeriod()}, resolveErr
}

// setReady reports whether the registration can currently authorize an attach.
func (r *Reconciler) setReady(tenant *v1alpha1.TenantCluster, resolveErr error) {
	switch {
	case resolveErr != nil:
		// status.resolvedVmids keeps its previous contents and
		// status.vmidsObservedAt is deliberately not restamped, so the staleness
		// the attach path checks goes on accruing. An unreachable hypervisor
		// must expire authorization, not freeze it.
		r.setCondition(tenant, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonProxmoxError,
			resolveErr.Error())
	case len(tenant.Status.ResolvedVMIDs) == 0:
		// Not an error, but not usable either, and silence here is how an
		// onboarding mistake turns into a 2am debugging session: every attach
		// this tenant asks for will be refused for a reason that looks like
		// authorization rather than like an empty registration.
		r.setCondition(tenant, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonInvalidSpec,
			"no VMIDs resolved: set spec.pool, spec.vmids, or both")
	default:
		r.setCondition(tenant, v1alpha1.ConditionReady, metav1.ConditionTrue, v1alpha1.ReasonAccepted,
			fmt.Sprintf("%d VMID(s) resolved, %d volume(s) accounted", len(tenant.Status.ResolvedVMIDs), tenant.Status.Used.Volumes))
	}
}

// reconcileDelete releases the finalizer, but only once the ledger is empty.
func (r *Reconciler) reconcileDelete(ctx context.Context, tenant *v1alpha1.TenantCluster) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(tenant, v1alpha1.TenantFinalizer) {
		return ctrl.Result{}, nil
	}

	volumes, err := r.volumes(ctx, tenant)
	if err != nil {
		return ctrl.Result{}, err
	}

	if len(volumes) > 0 {
		// Refused, with no deadline and no escape hatch. Releasing here would
		// leave real disks allocated on Proxmox with nothing in the cluster
		// recording whose they are -- the precise state this ledger exists to
		// prevent. Deleting the volumes runs their own finalizers, and this
		// releases on its own once they are gone.
		message := fmt.Sprintf("%d ProxmoxVolume(s) remain in namespace %s", len(volumes), tenant.Spec.Namespace)

		log.FromContext(ctx).Info("deletion blocked by the ledger", "namespace", tenant.Spec.Namespace, "volumes", len(volumes))

		r.setCondition(tenant, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonReconciling, message)

		return ctrl.Result{RequeueAfter: r.resolvePeriod()}, r.updateStatus(ctx, tenant)
	}

	controllerutil.RemoveFinalizer(tenant, v1alpha1.TenantFinalizer)

	if err := r.Client.Update(ctx, tenant); err != nil {
		return ctrl.Result{}, fmt.Errorf("releasing the finalizer on %s: %w", tenant.Name, err)
	}

	return ctrl.Result{}, nil
}

// resolveVMIDs recomputes status.resolvedVmids as the union of the explicit
// allowlist and the tenant's PVE pool.
//
// On failure the previous resolution is left exactly as it was and the
// observation timestamp is not touched, so the caller can tell "the pool is
// empty" from "the pool could not be read" -- and so authorization ages out on
// the second rather than being silently renewed.
func (r *Reconciler) resolveVMIDs(ctx context.Context, tenant *v1alpha1.TenantCluster) error {
	ids := map[int32]struct{}{}

	for _, id := range tenant.Spec.VMIDs {
		ids[id] = struct{}{}
	}

	// No pool means no hypervisor call at all, which is worth preserving: a
	// tenant registered with an explicit allowlist keeps working while Proxmox
	// is unreachable, because nothing about its answer depends on Proxmox.
	if tenant.Spec.Pool != "" {
		vms, err := r.Proxmox.ListVMs(ctx, tenant.Spec.Region)
		if err != nil {
			return fmt.Errorf("resolving pool %s in region %s: %w", tenant.Spec.Pool, tenant.Spec.Region, err)
		}

		for _, vm := range vms {
			if vm.Pool == tenant.Spec.Pool {
				ids[vm.VMID] = struct{}{}
			}
		}
	}

	resolved := make([]int32, 0, len(ids))
	for id := range ids {
		resolved = append(resolved, id)
	}

	slices.Sort(resolved)

	now := metav1.NewTime(r.now())

	tenant.Status.ResolvedVMIDs = resolved
	tenant.Status.VMIDsObservedAt = &now

	return nil
}

// aggregate rebuilds status.used and status.namespaceUsed from the ledger.
func (r *Reconciler) aggregate(ctx context.Context, tenant *v1alpha1.TenantCluster) error {
	volumes, err := r.volumes(ctx, tenant)
	if err != nil {
		return err
	}

	used := v1alpha1.QuotaUsage{}

	// Only namespaces with a configured quota are accounted, which keeps the
	// size of this status bounded by the spec rather than by tenant behavior. A
	// tenant that provisions into ten thousand namespaces would otherwise make
	// the operator write ten thousand entries on every resync, for numbers
	// nothing is enforcing.
	perNamespace := make(map[string]*v1alpha1.NamespaceUsage, len(tenant.Spec.NamespaceQuotas))
	for _, quota := range tenant.Spec.NamespaceQuotas {
		perNamespace[quota.Namespace] = &v1alpha1.NamespaceUsage{Namespace: quota.Namespace}
	}

	for i := range volumes {
		volume := &volumes[i]
		bytes := countedBytes(volume)

		used.Volumes++
		addCapacity(&used.Capacity, volume.Spec.Storage, bytes)

		if entry, ok := perNamespace[volume.Spec.ClaimRef.Namespace]; ok {
			entry.Volumes++
			addCapacity(&entry.Capacity, volume.Spec.Storage, bytes)
		}
	}

	namespaced := make([]v1alpha1.NamespaceUsage, 0, len(tenant.Spec.NamespaceQuotas))
	// Spec order, so a resync that changed nothing produces a byte-identical
	// status and no write.
	for _, quota := range tenant.Spec.NamespaceQuotas {
		namespaced = append(namespaced, *perNamespace[quota.Namespace])
	}

	tenant.Status.Used = used
	tenant.Status.NamespaceUsed = namespaced

	return nil
}

// volumes lists the tenant's ledger.
func (r *Reconciler) volumes(ctx context.Context, tenant *v1alpha1.TenantCluster) ([]v1alpha1.ProxmoxVolume, error) {
	if tenant.Spec.Namespace == "" {
		// Schema-impossible, but listing with an empty namespace selector would
		// silently mean "every namespace", and accounting one tenant for
		// another's volumes is not a failure mode worth leaving open.
		return nil, nil
	}

	list := &v1alpha1.ProxmoxVolumeList{}
	if err := r.Client.List(ctx, list, client.InNamespace(tenant.Spec.Namespace)); err != nil {
		return nil, fmt.Errorf("listing volumes in %s: %w", tenant.Spec.Namespace, err)
	}

	return list.Items, nil
}

// conflict reports why this registration is refused, or "" if it is not.
//
// spec.namespace, spec.placeholderVmid and spec.subject each have to be unique
// across TenantClusters, and no schema rule can say so -- CEL validation sees
// one object at a time. Sharing a namespace means sharing a ledger; sharing a
// subject means one tenant's token works as another; sharing a placeholder puts
// indistinguishable names on two tenants' unattached disks, which is exactly the
// condition this project exists to remove.
//
// The older registration wins, by creation timestamp with the name as the
// tiebreak. Which way the tie breaks matters less than that it is stable: a rule
// depending on reconcile order would flip both objects' status on every resync.
func (r *Reconciler) conflict(ctx context.Context, tenant *v1alpha1.TenantCluster) (string, error) {
	list := &v1alpha1.TenantClusterList{}
	if err := r.Client.List(ctx, list); err != nil {
		return "", fmt.Errorf("listing tenants: %w", err)
	}

	var reasons []string

	for i := range list.Items {
		other := &list.Items[i]

		// A registration being deleted still holds its namespace until its
		// finalizer releases, so it still conflicts. A replacement that came up
		// early would share a live ledger with the one on its way out.
		if other.Name == tenant.Name || !older(other, tenant) {
			continue
		}

		if other.Spec.Namespace == tenant.Spec.Namespace {
			reasons = append(reasons, fmt.Sprintf("namespace %s is already registered by %s", tenant.Spec.Namespace, other.Name))
		}

		if other.Spec.PlaceholderVMID == tenant.Spec.PlaceholderVMID {
			reasons = append(reasons, fmt.Sprintf("placeholderVmid %d is already registered by %s", tenant.Spec.PlaceholderVMID, other.Name))
		}

		if other.Spec.Subject == tenant.Spec.Subject {
			reasons = append(reasons, fmt.Sprintf("subject %s is already registered by %s", tenant.Spec.Subject, other.Name))
		}
	}

	sort.Strings(reasons)

	return strings.Join(reasons, "; "), nil
}

// older reports whether a was registered before b.
func older(a, b *v1alpha1.TenantCluster) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}

	return a.Name < b.Name
}

// countedBytes is what one volume costs its tenant's quota.
//
// The larger of what was asked for and what Proxmox reported. A volume still
// being created has no status yet and must still be counted, or two concurrent
// creates would each see the other's budget as free; a finished one may have
// been rounded up, and charging less than the disk occupies would let the ledger
// drift below reality. A volume being deleted is counted too -- its finalizer is
// what says the disk is gone, and until then it is.
func countedBytes(volume *v1alpha1.ProxmoxVolume) int64 {
	if volume.Status.CapacityBytes > volume.Spec.CapacityBytes {
		return volume.Status.CapacityBytes
	}

	return volume.Spec.CapacityBytes
}

// addCapacity adds bytes to a per-storage tally.
func addCapacity(into *map[string]resource.Quantity, storage string, bytes int64) {
	if storage == "" || bytes <= 0 {
		return
	}

	if *into == nil {
		*into = map[string]resource.Quantity{}
	}

	total, ok := (*into)[storage]
	if !ok {
		total = *resource.NewQuantity(0, resource.BinarySI)
	}

	total.Add(*resource.NewQuantity(bytes, resource.BinarySI))
	(*into)[storage] = total
}

// setCondition records one condition with this reconciler's clock.
func (r *Reconciler) setCondition(tenant *v1alpha1.TenantCluster, conditionType string, status metav1.ConditionStatus, reason, message string) {
	apimeta.SetStatusCondition(&tenant.Status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.NewTime(r.now()),
		ObservedGeneration: tenant.Generation,
	})
}

// updateStatus writes the status subresource.
func (r *Reconciler) updateStatus(ctx context.Context, tenant *v1alpha1.TenantCluster) error {
	tenant.Status.ObservedGeneration = tenant.Generation

	if err := r.Client.Status().Update(ctx, tenant); err != nil {
		return fmt.Errorf("updating %s status: %w", tenant.Name, err)
	}

	return nil
}

func (r *Reconciler) resolvePeriod() time.Duration {
	if r.ResolvePeriod > 0 {
		return r.ResolvePeriod
	}

	return DefaultResolvePeriod
}

func (r *Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}

	return time.Now()
}
