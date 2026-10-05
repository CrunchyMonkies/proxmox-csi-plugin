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

package storage_test

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/controller/storage"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const gib = 1024 * 1024 * 1024

// fakeReader stands in for Proxmox. Per-region results and per-region errors so
// a test can make one region fail while another succeeds -- the case the
// publisher's error handling exists for.
type fakeReader struct {
	regions  []string
	storages map[string][]proxmox.Storage
	errs     map[string]error
	calls    int
}

func (f *fakeReader) Regions() []string { return f.regions }

func (f *fakeReader) ListStorages(_ context.Context, region string) ([]proxmox.Storage, error) {
	f.calls++

	if err, ok := f.errs[region]; ok {
		return nil, err
	}

	return f.storages[region], nil
}

func newClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	return fake.NewClientBuilder().
		WithScheme(scheme).
		// Without this the fake lets an Update clobber status, which would make
		// every assertion below pass for the wrong reason.
		WithStatusSubresource(&v1alpha1.ProxmoxStorage{}).
		WithObjects(objects...).
		Build()
}

func newPublisher(t *testing.T, reader *fakeReader, objects ...client.Object) *storage.Publisher {
	t.Helper()

	at := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)

	return &storage.Publisher{
		Client:  newClient(t, objects...),
		Proxmox: reader,
		Now:     func() time.Time { return at },
	}
}

func names(t *testing.T, c client.Client) []string {
	t.Helper()

	list := &v1alpha1.ProxmoxStorageList{}
	require.NoError(t, c.List(t.Context(), list))

	out := make([]string, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, list.Items[i].Name)
	}

	sort.Strings(out)

	return out
}

func get(t *testing.T, c client.Client, name string) *v1alpha1.ProxmoxStorage {
	t.Helper()

	obj := &v1alpha1.ProxmoxStorage{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Name: name}, obj))

	return obj
}

func TestSyncPublishesTheCatalog(t *testing.T) {
	p := newPublisher(t, &fakeReader{
		regions: []string{"bne"},
		storages: map[string][]proxmox.Storage{
			"bne": {
				{
					Region: "bne", Zone: "", Name: "rbd", Shared: true,
					Types: []string{"images"}, Active: true,
					TotalBytes: 100 * gib, AvailableBytes: 40 * gib, UsedBytes: 60 * gib,
				},
				{
					Region: "bne", Zone: "pve-1", Name: "local-lvm",
					Types: []string{"images"}, Active: true,
					TotalBytes: 200 * gib, AvailableBytes: 150 * gib, UsedBytes: 50 * gib,
				},
			},
		},
	})

	require.NoError(t, p.Sync(t.Context()))

	assert.Equal(t, []string{"bne.pve-1.local-lvm", "bne.rbd"}, names(t, p.Client))

	shared := get(t, p.Client, "bne.rbd")
	assert.Equal(t, "bne", shared.Spec.Region)
	assert.Empty(t, shared.Spec.Zone)
	assert.True(t, shared.Spec.Shared)
	assert.Equal(t, int64(40*gib), shared.Status.AvailableBytes)
	assert.True(t, apimeta.IsStatusConditionTrue(shared.Status.Conditions, v1alpha1.ConditionReady))

	local := get(t, p.Client, "bne.pve-1.local-lvm")
	assert.Equal(t, "pve-1", local.Spec.Zone)
	assert.False(t, local.Spec.Shared)
	assert.Equal(t, int64(200*gib), local.Status.TotalBytes)

	// The labels a tenant and the prune step select on.
	assert.Equal(t, "bne", local.Labels[v1alpha1.LabelRegion])
	assert.Equal(t, "local-lvm", local.Labels[v1alpha1.LabelStorage])
}

func TestSyncUpdatesAndPrunes(t *testing.T) {
	reader := &fakeReader{
		regions: []string{"bne"},
		storages: map[string][]proxmox.Storage{
			"bne": {
				{Region: "bne", Zone: "pve-1", Name: "old", Active: true, AvailableBytes: 10 * gib},
				{Region: "bne", Zone: "pve-1", Name: "kept", Active: true, AvailableBytes: 10 * gib},
			},
		},
	}

	p := newPublisher(t, reader)
	require.NoError(t, p.Sync(t.Context()))
	require.Equal(t, []string{"bne.pve-1.kept", "bne.pve-1.old"}, names(t, p.Client))

	// "old" is gone from Proxmox and "kept" has filled up.
	reader.storages["bne"] = []proxmox.Storage{
		{Region: "bne", Zone: "pve-1", Name: "kept", Active: true, AvailableBytes: 1 * gib},
	}

	require.NoError(t, p.Sync(t.Context()))

	assert.Equal(t, []string{"bne.pve-1.kept"}, names(t, p.Client))
	assert.Equal(t, int64(1*gib), get(t, p.Client, "bne.pve-1.kept").Status.AvailableBytes)
}

func TestSyncDoesNotPruneOnReadError(t *testing.T) {
	// The property the whole design leans on: a hypervisor that cannot be asked
	// must not be read as a hypervisor with no storage. If this test ever fails,
	// a Proxmox outage empties the catalog and every tenant stops provisioning.
	reader := &fakeReader{
		regions: []string{"bne"},
		storages: map[string][]proxmox.Storage{
			"bne": {{Region: "bne", Zone: "pve-1", Name: "kept", Active: true}},
		},
	}

	p := newPublisher(t, reader)
	require.NoError(t, p.Sync(t.Context()))
	require.Equal(t, []string{"bne.pve-1.kept"}, names(t, p.Client))

	reader.errs = map[string]error{"bne": errors.New("connection refused")}

	require.Error(t, p.Sync(t.Context()))
	assert.Equal(t, []string{"bne.pve-1.kept"}, names(t, p.Client))
}

func TestSyncKeepsGoingAfterAFailedRegion(t *testing.T) {
	reader := &fakeReader{
		regions: []string{"bne", "syd"},
		errs:    map[string]error{"bne": errors.New("connection refused")},
		storages: map[string][]proxmox.Storage{
			"syd": {{Region: "syd", Zone: "pve-9", Name: "local-lvm", Active: true}},
		},
	}

	p := newPublisher(t, reader)

	err := p.Sync(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")

	// One region's outage does not cost the other its catalog.
	assert.Equal(t, []string{"syd.pve-9.local-lvm"}, names(t, p.Client))
	assert.Equal(t, 2, reader.calls, "both regions should have been attempted")
}

func TestPruneLeavesOtherRegionsAlone(t *testing.T) {
	// A hand-edited or stale label must not let one region's sync delete
	// another's objects, which is why prune re-checks spec.region.
	mislabelled := &v1alpha1.ProxmoxStorage{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "syd.pve-9.local-lvm",
			Labels: map[string]string{v1alpha1.LabelRegion: "bne"},
		},
		Spec: v1alpha1.ProxmoxStorageSpec{Region: "syd", Zone: "pve-9", Storage: "local-lvm"},
	}

	p := newPublisher(t, &fakeReader{
		regions:  []string{"bne"},
		storages: map[string][]proxmox.Storage{"bne": {}},
	}, mislabelled)

	require.NoError(t, p.Sync(t.Context()))

	assert.Equal(t, []string{"syd.pve-9.local-lvm"}, names(t, p.Client))
}

func TestUnmeasurableStorageIsPublishedNotReady(t *testing.T) {
	// A storage PVE lists but will not answer for stays in the catalog. Dropping
	// it would not stop a tenant provisioning, it would send the tenant
	// somewhere else silently; publishing it unready is the answer that reaches
	// the PVC as a reason.
	p := newPublisher(t, &fakeReader{
		regions: []string{"bne"},
		storages: map[string][]proxmox.Storage{
			"bne": {{
				Region: "bne", Name: "smb", Shared: true,
				StatusErr: errors.New("Parameter verification failed"),
			}},
		},
	})

	require.NoError(t, p.Sync(t.Context()))

	obj := get(t, p.Client, "bne.smb")
	assert.False(t, obj.Status.Active)
	assert.Zero(t, obj.Status.TotalBytes)

	ready := apimeta.FindStatusCondition(obj.Status.Conditions, v1alpha1.ConditionReady)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionFalse, ready.Status)
	assert.Equal(t, v1alpha1.ReasonProxmoxError, ready.Reason)
	assert.Contains(t, ready.Message, "Parameter verification failed")
}

func TestCheckIsUnreadyUntilSynced(t *testing.T) {
	now := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)

	p := newPublisher(t, &fakeReader{regions: []string{"bne"}})
	p.Now = func() time.Time { return now }

	// Nothing has synced, so the catalog this replica would serve is empty and
	// it should not be read from.
	require.Error(t, p.Check(nil))

	require.NoError(t, p.Sync(t.Context()))
	assert.NoError(t, p.Check(nil))

	// Well inside the staleness budget: a single failed poll is normal, and
	// flapping ready would restart an operator that is working fine.
	now = now.Add(storage.DefaultStaleAfter - time.Second)

	assert.NoError(t, p.Check(nil))

	now = now.Add(2 * time.Second)

	assert.Error(t, p.Check(nil))
}

func TestStartSyncsThenStops(t *testing.T) {
	reader := &fakeReader{
		regions:  []string{"bne"},
		storages: map[string][]proxmox.Storage{"bne": {{Region: "bne", Name: "rbd", Shared: true, Active: true}}},
	}

	p := newPublisher(t, reader)
	p.SyncPeriod = time.Hour

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// A canceled context still gets one sync in before the loop exits: the
	// first pass is deliberately not gated on the ticker, because a fresh leader
	// must publish immediately rather than a period later.
	require.NoError(t, p.Start(ctx))
	assert.Equal(t, 1, reader.calls)
	assert.Equal(t, []string{"bne.rbd"}, names(t, p.Client))
}

func TestNeedLeaderElection(t *testing.T) {
	assert.True(t, (&storage.Publisher{}).NeedLeaderElection())
}
