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

package tenant_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/controller/tenant"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const gib = 1024 * 1024 * 1024

// at is the reconciler's frozen clock.
var at = time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC) //nolint:gochecknoglobals

// fakeVMs stands in for Proxmox. Per-region inventory and per-region errors, so
// a test can make the hypervisor unreachable without making it disappear.
type fakeVMs struct {
	vms   map[string][]proxmox.VM
	errs  map[string]error
	calls int
}

func (f *fakeVMs) ListVMs(_ context.Context, region string) ([]proxmox.VM, error) {
	f.calls++

	if err, ok := f.errs[region]; ok {
		return nil, err
	}

	return f.vms[region], nil
}

func newClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	return fake.NewClientBuilder().
		WithScheme(scheme).
		// Without this the fake lets an Update clobber status, which would make
		// every assertion below pass for the wrong reason.
		WithStatusSubresource(&v1alpha1.TenantCluster{}).
		WithObjects(objects...).
		Build()
}

func newReconciler(t *testing.T, vms *fakeVMs, objects ...client.Object) (*tenant.Reconciler, client.Client) {
	t.Helper()

	c := newClient(t, objects...)

	return &tenant.Reconciler{
		Client:  c,
		Proxmox: vms,
		Now:     func() time.Time { return at },
	}, c
}

// tenantCluster builds a registration with the fields every test needs set.
func tenantCluster(name string, mutators ...func(*v1alpha1.TenantCluster)) *v1alpha1.TenantCluster {
	obj := &v1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(at.Add(-time.Hour)),
		},
		Spec: v1alpha1.TenantClusterSpec{
			Namespace:       "tenant-" + name,
			Region:          "bne",
			Subject:         "pvx:" + name,
			PlaceholderVMID: 9991,
			Mode:            v1alpha1.TenantModeObserve,
		},
	}

	for _, mutate := range mutators {
		mutate(obj)
	}

	return obj
}

// volume builds a ledger entry.
func volume(name, namespace, storage string, bytes int64, mutators ...func(*v1alpha1.ProxmoxVolume)) *v1alpha1.ProxmoxVolume {
	obj := &v1alpha1.ProxmoxVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1alpha1.ProxmoxVolumeSpec{
			Region:        "bne",
			Storage:       storage,
			CapacityBytes: bytes,
		},
	}

	for _, mutate := range mutators {
		mutate(obj)
	}

	return obj
}

func reconcile(t *testing.T, r *tenant.Reconciler, name string) ctrl.Result {
	t.Helper()

	result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKey{Name: name}})
	require.NoError(t, err)

	return result
}

func get(t *testing.T, c client.Client, name string) *v1alpha1.TenantCluster {
	t.Helper()

	obj := &v1alpha1.TenantCluster{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Name: name}, obj))

	return obj
}

func condition(t *testing.T, obj *v1alpha1.TenantCluster, conditionType string) *metav1.Condition {
	t.Helper()

	found := apimeta.FindStatusCondition(obj.Status.Conditions, conditionType)
	require.NotNil(t, found, "condition %s should be set", conditionType)

	return found
}

// capacities renders a per-storage tally for comparison.
//
// By string rather than by struct: resource.Quantity carries a cached rendering
// that a round trip through the API populates, so two quantities holding the
// same number are not necessarily equal to reflect.DeepEqual.
func capacities(t *testing.T, in map[string]resource.Quantity) map[string]string {
	t.Helper()

	out := make(map[string]string, len(in))
	for storage, quantity := range in {
		out[storage] = quantity.String()
	}

	return out
}

func TestUsageIsZeroOnAnEmptyLedger(t *testing.T) {
	// The milestone's own acceptance criterion. A registration with no volumes
	// must report no usage -- not an absent field a quota check would then have
	// to guess about, and not a leftover figure from a previous resync.
	r, c := newReconciler(t, &fakeVMs{}, tenantCluster("bne1-cluster1", func(o *v1alpha1.TenantCluster) {
		o.Spec.VMIDs = []int32{100}
	}))

	reconcile(t, r, "bne1-cluster1")

	obj := get(t, c, "bne1-cluster1")

	assert.Zero(t, obj.Status.Used.Volumes)
	assert.Empty(t, obj.Status.Used.Capacity)
	assert.Empty(t, obj.Status.NamespaceUsed)
	assert.Equal(t, metav1.ConditionTrue, condition(t, obj, v1alpha1.ConditionReady).Status)
}

func TestResolvedVMIDsAreTheUnionOfTheAllowlistAndThePool(t *testing.T) {
	vms := &fakeVMs{vms: map[string][]proxmox.VM{"bne": {
		{Region: "bne", VMID: 100, Node: "pve-1", Pool: "bne1-cluster1"},
		{Region: "bne", VMID: 101, Node: "pve-2", Pool: "bne1-cluster1"},
		// In the region but another tenant's, which is the whole point of
		// resolving membership rather than trusting the region.
		{Region: "bne", VMID: 200, Node: "pve-1", Pool: "syd1-mgt1"},
		// In no pool at all.
		{Region: "bne", VMID: 300, Node: "pve-3"},
	}}}

	r, c := newReconciler(t, vms, tenantCluster("bne1-cluster1", func(o *v1alpha1.TenantCluster) {
		o.Spec.Pool = "bne1-cluster1"
		// Overlaps the pool on 101, so the union has to dedupe.
		o.Spec.VMIDs = []int32{101, 105}
	}))

	reconcile(t, r, "bne1-cluster1")

	obj := get(t, c, "bne1-cluster1")

	assert.Equal(t, []int32{100, 101, 105}, obj.Status.ResolvedVMIDs)
	require.NotNil(t, obj.Status.VMIDsObservedAt)
	assert.Equal(t, at, obj.Status.VMIDsObservedAt.Time.UTC())
}

func TestNoPoolMeansNoHypervisorCall(t *testing.T) {
	// A tenant registered with an explicit allowlist must keep resolving while
	// Proxmox is unreachable, because nothing about its answer depends on
	// Proxmox. The fake would error if it were asked.
	vms := &fakeVMs{errs: map[string]error{"bne": errors.New("hypervisor is down")}}

	r, c := newReconciler(t, vms, tenantCluster("bne1-cluster1", func(o *v1alpha1.TenantCluster) {
		o.Spec.VMIDs = []int32{100, 101}
	}))

	reconcile(t, r, "bne1-cluster1")

	assert.Zero(t, vms.calls)
	assert.Equal(t, []int32{100, 101}, get(t, c, "bne1-cluster1").Status.ResolvedVMIDs)
}

func TestAReadFailureKeepsTheVMIDsAndLetsThemGoStale(t *testing.T) {
	observed := metav1.NewTime(at.Add(-30 * time.Minute))

	obj := tenantCluster("bne1-cluster1", func(o *v1alpha1.TenantCluster) {
		o.Spec.Pool = "bne1-cluster1"
		o.Status.ResolvedVMIDs = []int32{100, 101}
		o.Status.VMIDsObservedAt = &observed
	})

	vms := &fakeVMs{errs: map[string]error{"bne": errors.New("hypervisor is down")}}
	r, c := newReconciler(t, vms, obj)

	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKey{Name: "bne1-cluster1"}})
	require.Error(t, err, "the read failure must reach the manager so it backs off")

	after := get(t, c, "bne1-cluster1")

	// The previous resolution survives, so an attach in flight is not refused
	// for a hypervisor hiccup...
	assert.Equal(t, []int32{100, 101}, after.Status.ResolvedVMIDs)
	// ...but the observation is not restamped, so the staleness the attach path
	// checks goes on accruing and authorization eventually expires.
	require.NotNil(t, after.Status.VMIDsObservedAt)
	assert.Equal(t, observed.Time.UTC(), after.Status.VMIDsObservedAt.Time.UTC())

	ready := condition(t, after, v1alpha1.ConditionReady)
	assert.Equal(t, metav1.ConditionFalse, ready.Status)
	assert.Equal(t, v1alpha1.ReasonProxmoxError, ready.Reason)

	// The registration itself is still admitted: the hypervisor being
	// unreachable says nothing about whether the spec is acceptable.
	assert.Equal(t, metav1.ConditionTrue, condition(t, after, v1alpha1.ConditionAdmitted).Status)
}

func TestAnEmptyResolutionIsReportedRatherThanFailingClosedInSilence(t *testing.T) {
	r, c := newReconciler(t, &fakeVMs{}, tenantCluster("bne1-cluster1"))

	reconcile(t, r, "bne1-cluster1")

	ready := condition(t, get(t, c, "bne1-cluster1"), v1alpha1.ConditionReady)

	assert.Equal(t, metav1.ConditionFalse, ready.Status)
	assert.Equal(t, v1alpha1.ReasonInvalidSpec, ready.Reason)
	assert.Contains(t, ready.Message, "no VMIDs resolved")
}

func TestUsageIsAggregatedPerStorageAndPerNamespace(t *testing.T) {
	obj := tenantCluster("bne1-cluster1", func(o *v1alpha1.TenantCluster) {
		o.Spec.VMIDs = []int32{100}
		o.Spec.NamespaceQuotas = []v1alpha1.NamespaceQuota{
			{Namespace: "apps"},
			{Namespace: "databases"},
		}
	})

	r, c := newReconciler(t, &fakeVMs{}, obj,
		volume("pvc-1", "tenant-bne1-cluster1", "local-lvm", 10*gib, inNamespace("apps")),
		volume("pvc-2", "tenant-bne1-cluster1", "local-lvm", 5*gib, inNamespace("apps")),
		volume("pvc-3", "tenant-bne1-cluster1", "rbd", 20*gib, inNamespace("databases")),
		// No configured quota, so it counts against the tenant but is not
		// reported per namespace -- status stays bounded by the spec.
		volume("pvc-4", "tenant-bne1-cluster1", "rbd", 1*gib, inNamespace("scratch")),
		// Another tenant's ledger, in another namespace.
		volume("pvc-5", "tenant-syd1-mgt1", "local-lvm", 100*gib),
	)

	reconcile(t, r, "bne1-cluster1")

	after := get(t, c, "bne1-cluster1")

	assert.Equal(t, int32(4), after.Status.Used.Volumes)
	assert.Equal(t, map[string]string{"local-lvm": "15Gi", "rbd": "21Gi"}, capacities(t, after.Status.Used.Capacity))

	// Spec order, not observation order, so a resync that changed nothing
	// produces the same bytes and no write.
	require.Len(t, after.Status.NamespaceUsed, 2)

	assert.Equal(t, "apps", after.Status.NamespaceUsed[0].Namespace)
	assert.Equal(t, int32(2), after.Status.NamespaceUsed[0].Volumes)
	assert.Equal(t, map[string]string{"local-lvm": "15Gi"}, capacities(t, after.Status.NamespaceUsed[0].Capacity))

	assert.Equal(t, "databases", after.Status.NamespaceUsed[1].Namespace)
	assert.Equal(t, int32(1), after.Status.NamespaceUsed[1].Volumes)
	assert.Equal(t, map[string]string{"rbd": "20Gi"}, capacities(t, after.Status.NamespaceUsed[1].Capacity))
}

func TestAVolumeIsChargedTheLargerOfItsRequestAndItsRealSize(t *testing.T) {
	obj := tenantCluster("bne1-cluster1", func(o *v1alpha1.TenantCluster) {
		o.Spec.VMIDs = []int32{100}
	})

	deleting := metav1.NewTime(at)

	r, c := newReconciler(t, &fakeVMs{}, obj,
		// Requested 10Gi, Proxmox rounded up to 11Gi. Charging the request
		// would let the ledger drift below what the storage actually holds.
		volume("pvc-1", "tenant-bne1-cluster1", "local-lvm", 10*gib, func(v *v1alpha1.ProxmoxVolume) {
			v.Status.CapacityBytes = 11 * gib
		}),
		// Still being created, so no status at all. Counting it is what stops
		// two concurrent creates each seeing the other's budget as free.
		volume("pvc-2", "tenant-bne1-cluster1", "local-lvm", 4*gib),
		// Being deleted, but its finalizer has not released: the disk is still
		// allocated, so the tenant is still paying for it.
		volume("pvc-3", "tenant-bne1-cluster1", "local-lvm", 1*gib, func(v *v1alpha1.ProxmoxVolume) {
			v.DeletionTimestamp = &deleting
			v.Finalizers = []string{v1alpha1.VolumeFinalizer}
		}),
	)

	reconcile(t, r, "bne1-cluster1")

	after := get(t, c, "bne1-cluster1")

	assert.Equal(t, int32(3), after.Status.Used.Volumes)
	assert.Equal(t, map[string]string{"local-lvm": "16Gi"}, capacities(t, after.Status.Used.Capacity))
}

func TestTheOlderRegistrationWinsAUniquenessContest(t *testing.T) {
	first := tenantCluster("bne1-cluster1", func(o *v1alpha1.TenantCluster) {
		o.CreationTimestamp = metav1.NewTime(at.Add(-2 * time.Hour))
		o.Spec.VMIDs = []int32{100}
	})

	// Registered later against the same namespace, the same placeholder and the
	// same subject -- the copy-paste mistake the CRD schema cannot catch,
	// because a CEL rule only ever sees one object.
	second := tenantCluster("bne1-cluster1-copy", func(o *v1alpha1.TenantCluster) {
		o.CreationTimestamp = metav1.NewTime(at.Add(-time.Hour))
		o.Spec.Namespace = first.Spec.Namespace
		o.Spec.Subject = first.Spec.Subject
		o.Spec.VMIDs = []int32{200}
	})

	r, c := newReconciler(t, &fakeVMs{}, first, second)

	reconcile(t, r, "bne1-cluster1")
	reconcile(t, r, "bne1-cluster1-copy")

	winner := get(t, c, "bne1-cluster1")
	assert.Equal(t, metav1.ConditionTrue, condition(t, winner, v1alpha1.ConditionAdmitted).Status)
	assert.Equal(t, []int32{100}, winner.Status.ResolvedVMIDs)

	loser := get(t, c, "bne1-cluster1-copy")
	admitted := condition(t, loser, v1alpha1.ConditionAdmitted)
	assert.Equal(t, metav1.ConditionFalse, admitted.Status)
	assert.Equal(t, v1alpha1.ReasonInvalidSpec, admitted.Reason)
	assert.Contains(t, admitted.Message, "namespace tenant-bne1-cluster1 is already registered by bne1-cluster1")
	assert.Contains(t, admitted.Message, "placeholderVmid 9991 is already registered by bne1-cluster1")
	assert.Contains(t, admitted.Message, "subject pvx:bne1-cluster1 is already registered by bne1-cluster1")

	// Fail closed: the loser must not keep an authorization set that the winner
	// also claims.
	assert.Empty(t, loser.Status.ResolvedVMIDs)
	assert.Nil(t, loser.Status.VMIDsObservedAt)
}

func TestUniquenessIsDecidedTheSameWayOnEveryResync(t *testing.T) {
	// Same creation timestamp, so the name breaks the tie. What matters is not
	// which one wins but that reconciling in either order gives the same
	// answer: a rule that depended on reconcile order would flip both objects'
	// status forever.
	first := tenantCluster("aaa", func(o *v1alpha1.TenantCluster) { o.Spec.Namespace = "shared" })
	second := tenantCluster("bbb", func(o *v1alpha1.TenantCluster) { o.Spec.Namespace = "shared" })

	r, c := newReconciler(t, &fakeVMs{}, first, second)

	reconcile(t, r, "bbb")
	reconcile(t, r, "aaa")

	assert.Equal(t, metav1.ConditionTrue, condition(t, get(t, c, "aaa"), v1alpha1.ConditionAdmitted).Status)
	assert.Equal(t, metav1.ConditionFalse, condition(t, get(t, c, "bbb"), v1alpha1.ConditionAdmitted).Status)
}

func TestTheFinalizerIsHeldFromTheFirstReconcile(t *testing.T) {
	r, c := newReconciler(t, &fakeVMs{}, tenantCluster("bne1-cluster1"))

	reconcile(t, r, "bne1-cluster1")

	assert.Contains(t, get(t, c, "bne1-cluster1").Finalizers, v1alpha1.TenantFinalizer)
}

func TestDeletionIsBlockedWhileTheLedgerIsNotEmpty(t *testing.T) {
	deleting := metav1.NewTime(at)

	obj := tenantCluster("bne1-cluster1", func(o *v1alpha1.TenantCluster) {
		o.DeletionTimestamp = &deleting
		o.Finalizers = []string{v1alpha1.TenantFinalizer}
	})

	r, c := newReconciler(t, &fakeVMs{}, obj,
		volume("pvc-1", "tenant-bne1-cluster1", "local-lvm", 10*gib))

	result := reconcile(t, r, "bne1-cluster1")

	// Releasing here would leave a real disk allocated with nothing recording
	// whose it is, which is the state the ledger exists to prevent.
	after := get(t, c, "bne1-cluster1")
	assert.Contains(t, after.Finalizers, v1alpha1.TenantFinalizer)
	assert.Positive(t, result.RequeueAfter)

	ready := condition(t, after, v1alpha1.ConditionReady)
	assert.Equal(t, metav1.ConditionFalse, ready.Status)
	assert.Contains(t, ready.Message, "1 ProxmoxVolume(s) remain in namespace tenant-bne1-cluster1")
}

func TestDeletionReleasesOnceTheLedgerIsEmpty(t *testing.T) {
	deleting := metav1.NewTime(at)

	obj := tenantCluster("bne1-cluster1", func(o *v1alpha1.TenantCluster) {
		o.DeletionTimestamp = &deleting
		o.Finalizers = []string{v1alpha1.TenantFinalizer}
	})

	r, c := newReconciler(t, &fakeVMs{}, obj)

	reconcile(t, r, "bne1-cluster1")

	err := c.Get(t.Context(), client.ObjectKey{Name: "bne1-cluster1"}, &v1alpha1.TenantCluster{})
	require.Error(t, err, "the object should be gone once its last finalizer is released")
}

func TestAMissingTenantIsNotAnError(t *testing.T) {
	r, _ := newReconciler(t, &fakeVMs{})

	result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKey{Name: "gone"}})

	require.NoError(t, err)
	assert.Zero(t, result.RequeueAfter)
}

// inNamespace sets the tenant-side PVC namespace a volume is accounted to.
func inNamespace(namespace string) func(*v1alpha1.ProxmoxVolume) {
	return func(v *v1alpha1.ProxmoxVolume) {
		v.Spec.ClaimRef = v1alpha1.TenantClaimRef{Namespace: namespace, Name: v.Name, PVName: v.Name}
	}
}
