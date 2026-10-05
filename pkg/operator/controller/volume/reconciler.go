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

// Package volume reconciles ProxmoxVolume records against Proxmox.
//
// This version serves adoption and nothing else. Adoption is how ~86 live
// PersistentVolumes acquire an owner without being recreated, and it is pure
// bookkeeping: it confirms a disk is there, records what Proxmox says about it,
// and publishes the tenant's existing handle back unchanged. It issues no create,
// no move, no rename and no attach -- the reader it is handed has no method that
// could -- which is what makes the whole migration reversible with one helm
// rollback.
//
// The second thing it does is refuse a handle another record already claims. That
// check is the reason the runbook adopts every cluster before enforcing any: if
// two clusters have been sharing a disk all along, this is where it surfaces,
// while nothing is yet acting on the ledger.
package volume

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"
	pvevolume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DefaultSyncPeriod is how often an adopted volume is re-confirmed against
// Proxmox.
//
// Slower than the tenant reconciler's resolve period on purpose. Nothing about a
// disk expires: this is a drift check, not an authorization window, and the
// hypervisor cost is a content listing per storage rather than one call per
// region.
const DefaultSyncPeriod = 5 * time.Minute

// ClaimIndex indexes the Proxmox handle a record claims, across every namespace.
//
// Cluster-wide uniqueness of a volume handle is the one ownership rule no tenant
// can be trusted to keep and no admission policy can express, since a policy sees
// one object in one namespace at a time.
const ClaimIndex = "volumeClaim"

// Reconciler keeps ProxmoxVolume records in step with Proxmox.
type Reconciler struct {
	// Client is the management cluster client.
	Client client.Client
	// Proxmox reads storage content. Read-only by type.
	Proxmox proxmox.DiskReader
	// Writer mutates Proxmox disks. Nil when provisioning is not enabled.
	Writer proxmox.Writer
	// SyncPeriod defaults to DefaultSyncPeriod.
	SyncPeriod time.Duration
	// Now defaults to time.Now. Injected so tests do not sleep.
	Now func() time.Time
}

// The permissions this reconciler needs, and no more.
//
// Neither create nor delete on proxmoxvolumes, which is not an oversight: the
// records are a tenant's own objects in a tenant's own namespace, and an operator
// that could conjure one could grant itself a disk in someone else's ledger. The
// operator writes status and holds a finalizer; that is the whole of its authority
// over the ledger.
//
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxvolumes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxvolumes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=proxmoxvolumes/finalizers,verbs=update
// +kubebuilder:rbac:groups=csi.crunchymonkies.com,resources=tenantclusters,verbs=get;list;watch

// SetupWithManager registers the reconciler and its uniqueness index.
func (r *Reconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	if err := IndexClaims(ctx, mgr.GetFieldIndexer()); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		Named("proxmoxvolume").
		For(&v1alpha1.ProxmoxVolume{}).
		Watches(&v1alpha1.TenantCluster{}, handler.EnqueueRequestsFromMapFunc(r.volumesForTenant)).
		Complete(r)
}

// IndexClaims registers the cluster-wide claim index.
//
// Exported so tests build the fake client with the same index production uses. An
// index that exists only in one of the two is a duplicate-detection test that
// proves nothing about the operator.
func IndexClaims(ctx context.Context, indexer client.FieldIndexer) error {
	if err := indexer.IndexField(ctx, &v1alpha1.ProxmoxVolume{}, ClaimIndex, IndexClaim); err != nil {
		return fmt.Errorf("indexing volume claims: %w", err)
	}

	return nil
}

// IndexClaim is the index function behind ClaimIndex.
func IndexClaim(obj client.Object) []string {
	vol, ok := obj.(*v1alpha1.ProxmoxVolume)
	if !ok {
		return nil
	}

	if claim := claim(vol); claim != "" {
		return []string{claim}
	}

	return nil
}

// claim is the Proxmox handle a record claims: the one it has been granted, or
// failing that the one it is asking for.
//
// Indexing the request and not just the grant is what closes the race the whole
// check exists for. Two adoptions of the same disk, run seconds apart from two
// tenant clusters, would otherwise both read an index containing neither of them
// and both publish -- the second one's read racing the first one's status write
// through a cache. Claimed-from-creation removes the window entirely.
//
// For provisioned (non-adopt) volumes the handle is not knowable before the
// operator creates the disk, because the disk name embeds the tenant's
// placeholderVMID (vm-<placeholderVmid>-<pvName>) and the claim function runs
// without reading the TenantCluster. The volume joins duplicate detection once
// status.volumeID is set by the provisioning reconciler. The window between
// creation and the first status write is bounded by a single reconcile: the
// provisioner writes the status in the same pass that creates the disk, and the
// resourceVersion CAS on the status update is what serializes two racing
// provisions of the same name.
func claim(vol *v1alpha1.ProxmoxVolume) string {
	if vol.Status.VolumeID != "" {
		return vol.Status.VolumeID
	}

	return vol.Spec.AdoptVolumeID
}

// Reconcile brings one volume record in step with Proxmox.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	vol := &v1alpha1.ProxmoxVolume{}
	if err := r.Client.Get(ctx, req.NamespacedName, vol); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !vol.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, vol)
	}

	if !vol.Spec.Adopt {
		return r.reconcileProvision(ctx, vol)
	}

	return r.reconcileAdopt(ctx, vol)
}

// volumesForTenant maps a registration event onto the records it governs.
//
// Without this a volume adopted before its TenantCluster was admitted would sit
// rejected until something else touched it: every refusal below is a terminal
// state with no timer, precisely so a refused record does not poll the hypervisor
// forever.
func (r *Reconciler) volumesForTenant(ctx context.Context, obj client.Object) []reconcile.Request {
	tenant, ok := obj.(*v1alpha1.TenantCluster)
	if !ok || tenant.Spec.Namespace == "" {
		return nil
	}

	list := &v1alpha1.ProxmoxVolumeList{}
	if err := r.Client.List(ctx, list, client.InNamespace(tenant.Spec.Namespace)); err != nil {
		log.FromContext(ctx).Error(err, "listing volumes for a tenant event", "namespace", tenant.Spec.Namespace)

		return nil
	}

	requests := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])})
	}

	return requests
}

// reconcileAdopt records an existing disk, or says why it will not.
//
// Deliberately a flat sequence of independent refusals. Splitting it would hide
// the one property worth reading here: every check runs before Proxmox is asked
// for anything, and none of them can write.
//
//nolint:cyclop // See above.
func (r *Reconciler) reconcileAdopt(ctx context.Context, vol *v1alpha1.ProxmoxVolume) (ctrl.Result, error) {
	tenant, err := r.tenant(ctx, vol.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}

	switch {
	case tenant == nil:
		return r.reject(ctx, vol, v1alpha1.ReasonTenantUnknown,
			fmt.Sprintf("no admitted TenantCluster registers namespace %s", vol.Namespace))
	case tenant.Spec.Mode == v1alpha1.TenantModeSuspended:
		return r.reject(ctx, vol, v1alpha1.ReasonTenantSuspended,
			fmt.Sprintf("tenant %s is suspended", tenant.Name))
	case vol.Spec.AdoptVolumeID == "":
		return r.reject(ctx, vol, v1alpha1.ReasonInvalidSpec, "spec.adopt is set without spec.adoptVolumeID")
	}

	handle, err := pvevolume.NewVolumeFromVolumeID(vol.Spec.AdoptVolumeID)
	if err != nil {
		return r.reject(ctx, vol, v1alpha1.ReasonInvalidSpec,
			fmt.Sprintf("spec.adoptVolumeID %q is not a volume handle: %v", vol.Spec.AdoptVolumeID, err))
	}

	if mismatch := describes(vol, handle); mismatch != "" {
		// The spec has to describe the handle it claims. They are redundant on
		// purpose, and the redundancy is only worth carrying if a disagreement is
		// refused: quota is charged against spec.storage, so a record whose spec
		// says one storage while its handle names another charges one budget and
		// occupies another.
		return r.reject(ctx, vol, v1alpha1.ReasonVolumeIDMismatch, mismatch)
	}

	if handle.Region() != tenant.Spec.Region {
		return r.reject(ctx, vol, v1alpha1.ReasonInvalidSpec,
			fmt.Sprintf("handle names region %s, tenant %s is registered in %s", handle.Region(), tenant.Name, tenant.Spec.Region))
	}

	// Checked against the name the tenant supplied, before anything is read, so a
	// handle naming a VM this tenant does not own is refused without the operator
	// so much as looking the disk up.
	if vmid, ok := ownedVMID(tenant, handle.VMID()); !ok {
		return r.reject(ctx, vol, v1alpha1.ReasonVMIDNotOwned,
			fmt.Sprintf("handle names vmid %s, which is neither tenant %s's placeholder nor one of its VMs", vmid, tenant.Name))
	}

	if other, err := r.duplicate(ctx, vol); err != nil {
		return ctrl.Result{}, err
	} else if other != nil {
		return r.rejectDuplicate(ctx, vol, other)
	}

	return r.adopt(ctx, vol, tenant, handle)
}

// adopt reads the disk and records what Proxmox says about it.
func (r *Reconciler) adopt(
	ctx context.Context,
	vol *v1alpha1.ProxmoxVolume,
	tenant *v1alpha1.TenantCluster,
	handle *pvevolume.Volume,
) (ctrl.Result, error) {
	disks, err := r.Proxmox.ListDisks(ctx, handle.Region(), handle.Zone(), handle.Storage())
	if err != nil {
		// Reported, requeued and returned, with status.volumeID untouched. A
		// hypervisor that cannot be read is not evidence about a disk, and a
		// tenant whose PV is already bound must not have its handle retracted
		// because a listing failed.
		r.setCondition(vol, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonProxmoxError, err.Error())

		return ctrl.Result{}, errors2(r.updateStatus(ctx, vol), err)
	}

	disk := findDisk(disks, handle)
	if disk == nil {
		// Never deleted, never cleared. A disk that is not where the ledger says
		// is the case where a confident repair does the damage, so this reports
		// and retries and does nothing else.
		r.setCondition(vol, v1alpha1.ConditionAdmitted, metav1.ConditionFalse, v1alpha1.ReasonAdoptDiskNotFound,
			fmt.Sprintf("no disk matching %s on storage %s", handle.Disk(), handle.Storage()))
		r.setCondition(vol, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonAdoptDiskNotFound,
			fmt.Sprintf("no disk matching %s on storage %s", handle.Disk(), handle.Storage()))

		vol.Status.Phase = v1alpha1.VolumePhaseFailed

		return ctrl.Result{RequeueAfter: r.syncPeriod()}, r.updateStatus(ctx, vol)
	}

	// The disk may have been found under another VM's name -- an attached volume
	// carries the VMID of the VM it is attached to, while the PV's immutable
	// handle still carries the placeholder. That VM has to belong to this tenant
	// too, or adopting the record would be adopting someone else's attachment.
	found := pvevolume.NewVolume(handle.Region(), handle.Zone(), handle.Storage(), disk.Name)
	if vmid, ok := ownedVMID(tenant, found.VMID()); !ok {
		return r.reject(ctx, vol, v1alpha1.ReasonVMIDNotOwned,
			fmt.Sprintf("disk %s is named for vmid %s, which is neither tenant %s's placeholder nor one of its VMs",
				disk.Name, vmid, tenant.Name))
	}

	if err := r.hold(ctx, vol, tenant); err != nil {
		return ctrl.Result{}, err
	}

	now := metav1.NewTime(r.now())

	// Byte-identical to what the tenant already has in spec.csi.volumeHandle.
	// Republishing the handle unchanged is the property the node plugin's
	// ignorance of this whole migration rests on.
	vol.Status.VolumeID = vol.Spec.AdoptVolumeID
	vol.Status.DiskName = disk.Name
	vol.Status.OwnerVMID = embeddedVMID(found)
	vol.Status.CapacityBytes = disk.SizeBytes
	vol.Status.Adopted = true
	vol.Status.Phase = v1alpha1.VolumePhaseReady
	vol.Status.LastSyncTime = &now

	r.setCondition(vol, v1alpha1.ConditionAdmitted, metav1.ConditionTrue, v1alpha1.ReasonAccepted,
		fmt.Sprintf("adopted by tenant %s", tenant.Name))
	r.setCondition(vol, v1alpha1.ConditionReady, metav1.ConditionTrue, v1alpha1.ReasonAccepted,
		fmt.Sprintf("disk %s on %s, %d bytes", disk.Name, disk.Storage, disk.SizeBytes))

	// Adopted regardless, and flagged. A pre-existing disk on a storage the
	// tenant may not provision on is a fact about the estate, not a reason to
	// leave it unowned -- refusing here would keep the one disk nobody has a
	// record for out of the ledger.
	if slices.Contains(tenant.Spec.AllowedStorages, handle.Storage()) {
		r.setCondition(vol, v1alpha1.ConditionDegraded, metav1.ConditionFalse, v1alpha1.ReasonAccepted,
			"ledger and Proxmox agree")
	} else {
		r.setCondition(vol, v1alpha1.ConditionDegraded, metav1.ConditionTrue, v1alpha1.ReasonStorageNotAllowed,
			fmt.Sprintf("storage %s is not in tenant %s's allowedStorages", handle.Storage(), tenant.Name))
	}

	return ctrl.Result{RequeueAfter: r.syncPeriod()}, r.updateStatus(ctx, vol)
}

// hold labels the record and takes the finalizer.
//
// Before the handle is published, not after: the finalizer is what stops a
// tenant's delete from removing the only record of a disk this operator has just
// decided it owns.
func (r *Reconciler) hold(ctx context.Context, vol *v1alpha1.ProxmoxVolume, tenant *v1alpha1.TenantCluster) error {
	patched := controllerutil.AddFinalizer(vol, v1alpha1.VolumeFinalizer)

	labels := map[string]string{
		v1alpha1.LabelTenant:  tenant.Name,
		v1alpha1.LabelRegion:  tenant.Spec.Region,
		v1alpha1.LabelStorage: vol.Spec.Storage,
	}

	for key, value := range labels {
		if vol.Labels[key] == value {
			continue
		}

		if vol.Labels == nil {
			vol.Labels = map[string]string{}
		}

		vol.Labels[key] = value
		patched = true
	}

	if !patched {
		return nil
	}

	if err := r.Client.Update(ctx, vol); err != nil {
		return fmt.Errorf("holding %s/%s: %w", vol.Namespace, vol.Name, err)
	}

	return nil
}

// reconcileDelete releases the record.
func (r *Reconciler) reconcileDelete(ctx context.Context, vol *v1alpha1.ProxmoxVolume) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(vol, v1alpha1.VolumeFinalizer) {
		return ctrl.Result{}, nil
	}

	// Provisioned (non-adopted) volumes: delete the disk, then release.
	if !vol.Status.Adopted && !vol.Spec.Adopt && r.Writer != nil {
		tenant, err := r.tenant(ctx, vol.Namespace)
		if err != nil {
			return ctrl.Result{}, err
		}

		if tenant != nil && tenant.Spec.Mode == v1alpha1.TenantModeDecommission {
			return r.reconcileDeleteProvisioned(ctx, vol)
		}

		// In Enforce mode, provisioned volumes are deleted.
		return r.reconcileDeleteProvisioned(ctx, vol)
	}

	tenant, err := r.tenant(ctx, vol.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}

	if tenant != nil && tenant.Spec.Mode == v1alpha1.TenantModeDecommission {
		// Blocked rather than released, because releasing would be a lie. In
		// Decommission mode the record's disappearance is supposed to mean the
		// disk was destroyed, and this operator cannot destroy one. Reporting the
		// gap is recoverable -- move the tenant out of Decommission and the
		// record releases; silently leaving an allocated disk behind while
		// telling the tenant it was reclaimed is not.
		message := fmt.Sprintf("tenant %s is decommissioning, and this operator cannot delete disks", tenant.Name)

		log.FromContext(ctx).Info("deletion blocked", "volume", vol.Name, "namespace", vol.Namespace, "reason", message)

		r.setCondition(vol, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonReconciling, message)

		vol.Status.Phase = v1alpha1.VolumePhaseDeleting

		return ctrl.Result{RequeueAfter: r.syncPeriod()}, r.updateStatus(ctx, vol)
	}

	if vol.Status.Adopted {
		// Said out loud, because it is the one thing an operator watching a
		// teardown could otherwise get wrong: the disk is still on the
		// hypervisor, and nothing in this cluster records it any more.
		log.FromContext(ctx).Info("releasing an adopted volume, the disk is left in place",
			"volume", vol.Name, "namespace", vol.Namespace, "volumeID", vol.Status.VolumeID, "diskName", vol.Status.DiskName)
	}

	controllerutil.RemoveFinalizer(vol, v1alpha1.VolumeFinalizer)

	if err := r.Client.Update(ctx, vol); err != nil {
		return ctrl.Result{}, fmt.Errorf("releasing %s/%s: %w", vol.Namespace, vol.Name, err)
	}

	return ctrl.Result{}, nil
}

// tenant returns the admitted registration for a namespace, or nil.
//
// Admitted is the tenant reconciler's verdict on the uniqueness contest, and
// reading it rather than re-deciding it is deliberate: two controllers applying
// the same older-wins rule to the same objects would eventually disagree during
// the window where one has reconciled and the other has not.
func (r *Reconciler) tenant(ctx context.Context, namespace string) (*v1alpha1.TenantCluster, error) {
	list := &v1alpha1.TenantClusterList{}
	if err := r.Client.List(ctx, list); err != nil {
		return nil, fmt.Errorf("listing tenants: %w", err)
	}

	for i := range list.Items {
		tenant := &list.Items[i]

		if tenant.Spec.Namespace != namespace {
			continue
		}

		if apimeta.IsStatusConditionTrue(tenant.Status.Conditions, v1alpha1.ConditionAdmitted) {
			return tenant, nil
		}
	}

	return nil, nil //nolint:nilnil // No tenant is an ordinary answer, not a failure.
}

// duplicate returns the record that already claims this one's handle, or nil.
func (r *Reconciler) duplicate(ctx context.Context, vol *v1alpha1.ProxmoxVolume) (*v1alpha1.ProxmoxVolume, error) {
	handle := claim(vol)
	if handle == "" {
		return nil, nil //nolint:nilnil // Nothing claimed, nothing to collide with.
	}

	list := &v1alpha1.ProxmoxVolumeList{}
	if err := r.Client.List(ctx, list, client.MatchingFields{ClaimIndex: handle}); err != nil {
		return nil, fmt.Errorf("listing claims on %s: %w", handle, err)
	}

	for i := range list.Items {
		other := &list.Items[i]

		if other.Namespace == vol.Namespace && other.Name == vol.Name {
			continue
		}

		// Older wins, by creation timestamp with the namespaced name as the
		// tiebreak. As with tenant registrations, which way the tie breaks
		// matters far less than that it is the same way on every resync: a rule
		// that depended on reconcile order would flip both records' status back
		// and forth and make the alert on this condition useless.
		if olderClaim(other, vol) {
			return other, nil
		}
	}

	return nil, nil //nolint:nilnil // No collision is an ordinary answer.
}

// rejectDuplicate refuses a handle another record already claims.
func (r *Reconciler) rejectDuplicate(ctx context.Context, vol, other *v1alpha1.ProxmoxVolume) (ctrl.Result, error) {
	message := fmt.Sprintf("%s is already claimed by %s/%s", claim(vol), other.Namespace, other.Name)

	// Degraded as well as refused, and this is the one condition in the operator
	// worth alerting on before anything is enforced: two records claiming one
	// disk means two clusters have been sharing it, which no amount of policy
	// applied from here can undo.
	r.setCondition(vol, v1alpha1.ConditionDegraded, metav1.ConditionTrue, v1alpha1.ReasonDuplicateClaim, message)

	log.FromContext(ctx).Info("duplicate claim refused", "volume", vol.Name, "namespace", vol.Namespace,
		"claim", claim(vol), "heldBy", other.Namespace+"/"+other.Name)

	return r.reject(ctx, vol, v1alpha1.ReasonDuplicateClaim, message)
}

// reject records a terminal refusal.
//
// status.volumeID is deliberately left alone. A record can become refused long
// after it was granted -- its registration deleted, its VM moved out of the pool
// -- and retracting a handle the tenant has already bound a PersistentVolume to
// would break a running workload to make a status field tidier. The refusal is
// what other reconcilers read; the handle is history.
func (r *Reconciler) reject(ctx context.Context, vol *v1alpha1.ProxmoxVolume, reason, message string) (ctrl.Result, error) {
	r.setCondition(vol, v1alpha1.ConditionAdmitted, metav1.ConditionFalse, reason, message)
	r.setCondition(vol, v1alpha1.ConditionReady, metav1.ConditionFalse, reason, message)

	vol.Status.Phase = v1alpha1.VolumePhaseRejected

	// No requeue. Every refusal here is a statement about the spec or about
	// another object, and both changing are events this controller already
	// watches; a timer would only poll a hypervisor on behalf of a record that
	// cannot be admitted anyway.
	return ctrl.Result{}, r.updateStatus(ctx, vol)
}

// describes reports how the spec disagrees with the handle it claims, or "".
func describes(vol *v1alpha1.ProxmoxVolume, handle *pvevolume.Volume) string {
	switch {
	case vol.Spec.Region != handle.Region():
		return fmt.Sprintf("spec.region %q does not match adoptVolumeID region %q", vol.Spec.Region, handle.Region())
	case vol.Spec.Zone != handle.Zone():
		return fmt.Sprintf("spec.zone %q does not match adoptVolumeID zone %q", vol.Spec.Zone, handle.Zone())
	case vol.Spec.Storage != handle.Storage():
		return fmt.Sprintf("spec.storage %q does not match adoptVolumeID storage %q", vol.Spec.Storage, handle.Storage())
	}

	return ""
}

// ownedVMID reports whether a VMID embedded in a disk name belongs to a tenant.
//
// Three sources, which are ownership layers 3 and 2 meeting: the tenant's own
// placeholder, the legacy placeholders its pre-existing disks still carry, and
// the VMs live pool membership currently resolves to. The last is why this is
// worth checking at all -- it is the only one of the three a compromised tenant
// cannot choose for itself.
func ownedVMID(tenant *v1alpha1.TenantCluster, vmid string) (string, bool) {
	if vmid == "" {
		// A disk not in Proxmox's vm-<vmid>-<name> form. It cannot be attributed
		// by name, so the naming layer abstains rather than guessing.
		return vmid, true
	}

	id, err := strconv.ParseInt(vmid, 10, 32)
	if err != nil {
		return vmid, false
	}

	owned := int32(id) == tenant.Spec.PlaceholderVMID ||
		slices.Contains(tenant.Spec.LegacyControllerVMIDs, int32(id)) ||
		slices.Contains(tenant.Status.ResolvedVMIDs, int32(id))

	return vmid, owned
}

// findDisk locates the handle's disk in a storage listing, tolerating a rename.
//
// Exact name first, then the part of the name a reassignment does not change.
// Without the second pass adoption would fail for every volume that happens to be
// attached: with features.reassignVolumeOnAttach on, an attached disk is named
// for the VM holding it while the PersistentVolume's immutable handle still
// carries the placeholder, so the two names disagree for exactly the volumes a
// live cluster has in use.
func findDisk(disks []proxmox.Disk, handle *pvevolume.Volume) *proxmox.Disk {
	for i := range disks {
		if disks[i].Name == handle.Disk() {
			return &disks[i]
		}
	}

	suffix := handle.DiskSuffix()
	if suffix == "" {
		return nil
	}

	for i := range disks {
		candidate := pvevolume.NewVolume(handle.Region(), handle.Zone(), handle.Storage(), disks[i].Name)
		if candidate.DiskSuffix() == suffix {
			return &disks[i]
		}
	}

	return nil
}

// embeddedVMID is the VMID a disk's name carries, or zero if it carries none.
//
// The name rather than the vmid Proxmox reports alongside it. The two disagree
// only while a reassignment is half applied, and the name is the one the driver's
// own lookups go by, so recording it keeps this status field answering the same
// question the rest of the system asks.
func embeddedVMID(vol *pvevolume.Volume) int32 {
	id, err := strconv.ParseInt(vol.VMID(), 10, 32)
	if err != nil {
		return 0
	}

	return int32(id)
}

// olderClaim reports whether a claimed its handle before b.
func olderClaim(a, b *v1alpha1.ProxmoxVolume) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}

	if a.Namespace != b.Namespace {
		return a.Namespace < b.Namespace
	}

	return a.Name < b.Name
}

// errors2 joins a status-write failure with the cause that produced it, keeping
// whichever is not nil.
func errors2(first, second error) error {
	switch {
	case first == nil:
		return second
	case second == nil:
		return first
	}

	return fmt.Errorf("%w (while reporting: %w)", second, first)
}

// setCondition records one condition with this reconciler's clock.
func (r *Reconciler) setCondition(vol *v1alpha1.ProxmoxVolume, conditionType string, status metav1.ConditionStatus, reason, message string) {
	apimeta.SetStatusCondition(&vol.Status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.NewTime(r.now()),
		ObservedGeneration: vol.Generation,
	})
}

// updateStatus writes the status subresource.
func (r *Reconciler) updateStatus(ctx context.Context, vol *v1alpha1.ProxmoxVolume) error {
	vol.Status.ObservedGeneration = vol.Generation

	if err := r.Client.Status().Update(ctx, vol); err != nil {
		return fmt.Errorf("updating %s/%s status: %w", vol.Namespace, vol.Name, err)
	}

	return nil
}

func (r *Reconciler) syncPeriod() time.Duration {
	if r.SyncPeriod > 0 {
		return r.SyncPeriod
	}

	return DefaultSyncPeriod
}

func (r *Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}

	return time.Now()
}
