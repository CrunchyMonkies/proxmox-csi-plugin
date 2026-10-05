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

package drift_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/controller/drift"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const gib = 1024 * 1024 * 1024

var errProxmoxDown = errors.New("connection refused")

// fakeDisks stands in for Proxmox, keyed "region/storage".
type fakeDisks struct {
	disks map[string][]proxmox.Disk
	errs  map[string]error
}

func (f *fakeDisks) ListDisks(_ context.Context, region, _, storage string) ([]proxmox.Disk, error) {
	key := region + "/" + storage

	if err, ok := f.errs[key]; ok {
		return nil, err
	}

	return f.disks[key], nil
}

func disk(storage, name string) proxmox.Disk {
	return proxmox.Disk{Region: "bne", Node: "pve-1", Storage: storage, Name: name, VMID: 9999, SizeBytes: gib}
}

func newDetector(t *testing.T, disks *fakeDisks, objects ...client.Object) *drift.Detector {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	return &drift.Detector{
		Client:  fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(),
		Proxmox: disks,
	}
}

func tenant(storages ...string) *v1alpha1.TenantCluster {
	return &v1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "bne1-cluster1"},
		Spec: v1alpha1.TenantClusterSpec{
			Namespace: "tenant-bne1-cluster1", Region: "bne",
			PlaceholderVMID: 9991, AllowedStorages: storages,
		},
	}
}

// recorded builds a ledger entry that has been granted a handle.
func recorded(name, storage, volumeID string) *v1alpha1.ProxmoxVolume {
	return &v1alpha1.ProxmoxVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tenant-bne1-cluster1"},
		Spec: v1alpha1.ProxmoxVolumeSpec{
			Region: "bne", Zone: "pve-1", Storage: storage, CapacityBytes: gib,
		},
		Status: v1alpha1.ProxmoxVolumeStatus{VolumeID: volumeID},
	}
}

func TestSweepReportsBothDirections(t *testing.T) {
	disks := &fakeDisks{disks: map[string][]proxmox.Disk{"bne/local-lvm": {
		disk("local-lvm", "vm-9999-pvc-claimed"),
		// On the hypervisor, in nobody's ledger. Before cut-over this means a
		// cluster is provisioning outside the control plane; after it, a leak.
		disk("local-lvm", "vm-9999-pvc-orphan"),
	}}}

	detector := newDetector(t, disks,
		tenant("local-lvm"),
		recorded("pvc-claimed", "local-lvm", "bne/pve-1/local-lvm/vm-9999-pvc-claimed"),
		// In the ledger, not on the hypervisor.
		recorded("pvc-gone", "local-lvm", "bne/pve-1/local-lvm/vm-9999-pvc-gone"))

	report, err := detector.Sweep(t.Context())
	require.NoError(t, err)

	assert.Equal(t, []drift.Orphan{{
		Region: "bne", Storage: "local-lvm", Name: "vm-9999-pvc-orphan", VMID: 9999, SizeBytes: gib,
	}}, report.Orphans)

	assert.Equal(t, []drift.Missing{{
		Namespace: "tenant-bne1-cluster1", Name: "pvc-gone", VolumeID: "bne/pve-1/local-lvm/vm-9999-pvc-gone",
	}}, report.Missing)

	assert.Empty(t, report.Unreadable)
}

func TestSweepIgnoresDisksThisDriverCouldNotHaveCreated(t *testing.T) {
	// A Proxmox storage is full of things that are supposed to be there. Counting
	// them would produce a permanently non-zero orphan number, and a number that
	// is never zero is a number nobody reads.
	disks := &fakeDisks{disks: map[string][]proxmox.Disk{"bne/local-lvm": {
		disk("local-lvm", "vm-100-disk-0"),      // a real VM's root disk
		disk("local-lvm", "base-9000-disk-0"),   // a template's, in no vm- form at all
		disk("local-lvm", "vm-100-cloudinit"),   // a generated config drive
		disk("local-lvm", "vm-9999-pvc-orphan"), // and the one that is genuinely ours
	}}}

	report, err := newDetector(t, disks, tenant("local-lvm")).Sweep(t.Context())
	require.NoError(t, err)

	require.Len(t, report.Orphans, 1)
	assert.Equal(t, "vm-9999-pvc-orphan", report.Orphans[0].Name)
}

func TestSweepFollowsADiskThroughARename(t *testing.T) {
	// The disk is attached to VM 101 and named for it, while the ledger still
	// holds the placeholder handle. Matching on the full name would report this
	// one volume twice -- as an orphan under the name it has and as missing
	// under the name the ledger recorded -- for every volume in use.
	disks := &fakeDisks{disks: map[string][]proxmox.Disk{"bne/local-lvm": {
		{Region: "bne", Node: "pve-1", Storage: "local-lvm", Name: "vm-101-pvc-abc", VMID: 101, SizeBytes: gib},
	}}}

	detector := newDetector(t, disks, tenant("local-lvm"),
		recorded("pvc-abc", "local-lvm", "bne/pve-1/local-lvm/vm-9999-pvc-abc"))

	report, err := detector.Sweep(t.Context())
	require.NoError(t, err)

	assert.Empty(t, report.Orphans)
	assert.Empty(t, report.Missing)
}

func TestAnUnreadableStorageSuppressesItsMissingCount(t *testing.T) {
	// The one mistake that would send someone looking for a disk that is fine.
	// A storage that could not be listed is no evidence either way, and every
	// volume on it would otherwise be reported as vanished the moment a node
	// went down.
	disks := &fakeDisks{errs: map[string]error{"bne/local-lvm": errProxmoxDown}}

	detector := newDetector(t, disks, tenant("local-lvm"),
		recorded("pvc-abc", "local-lvm", "bne/pve-1/local-lvm/vm-9999-pvc-abc"))

	report, err := detector.Sweep(t.Context())
	require.NoError(t, err)

	assert.Empty(t, report.Missing)
	assert.Empty(t, report.Orphans)
	assert.Equal(t, []string{"bne/local-lvm"}, report.Unreadable)
}

func TestADeletedStorageReportsItsVolumesMissing(t *testing.T) {
	// The other half of the pair above, and the reason the distinction is drawn
	// at all. Proxmox answering "that storage does not exist" is evidence, not a
	// silence: the volumes recorded on it really are gone. Treating it like an
	// unreadable storage would suppress the count permanently -- a deleted
	// storage stays deleted -- so the one situation this detector exists for is
	// the one it would say nothing about.
	//
	// A typo in allowedStorages arrives here identically, and should.
	disks := &fakeDisks{errs: map[string]error{
		"bne/local-lvm": fmt.Errorf("region bne: locating storage local-lvm: %w", proxmox.ErrStorageNotFound),
	}}

	detector := newDetector(t, disks, tenant("local-lvm"),
		recorded("pvc-abc", "local-lvm", "bne/pve-1/local-lvm/vm-9999-pvc-abc"))

	report, err := detector.Sweep(t.Context())
	require.NoError(t, err)

	assert.Equal(t, []drift.Missing{{
		Namespace: "tenant-bne1-cluster1", Name: "pvc-abc", VolumeID: "bne/pve-1/local-lvm/vm-9999-pvc-abc",
	}}, report.Missing)

	// Not unreadable: it was read, and the answer was that it is not there.
	assert.Empty(t, report.Unreadable)
	assert.Empty(t, report.Orphans)
}

func TestSweepCoversStoragesTheLedgerUsesAfterPolicyStopsAllowingThem(t *testing.T) {
	// Removing a storage from allowedStorages stops new volumes landing on it.
	// It does not move the ones already there, and a sweep scoped to policy
	// would quietly stop watching exactly the disks nobody will think about
	// again.
	disks := &fakeDisks{disks: map[string][]proxmox.Disk{"bne/rbd": {
		disk("rbd", "vm-9999-pvc-orphan"),
	}}}

	detector := newDetector(t, disks, tenant("local-lvm"),
		recorded("pvc-abc", "rbd", "bne/pve-1/rbd/vm-9999-pvc-abc"))

	report, err := detector.Sweep(t.Context())
	require.NoError(t, err)

	require.Len(t, report.Orphans, 1)
	assert.Equal(t, "rbd", report.Orphans[0].Storage)
	require.Len(t, report.Missing, 1)
	assert.Equal(t, "pvc-abc", report.Missing[0].Name)
}

func TestTheSameSuffixOnTwoStoragesDoesNotVouchForItself(t *testing.T) {
	// A clone or a half-finished storage migration genuinely leaves the same
	// pvc-<uuid> on two storages. Keying claims globally would let the copy on
	// one storage account for the ledger entry on the other, hiding both the
	// orphan and the missing volume.
	disks := &fakeDisks{disks: map[string][]proxmox.Disk{
		"bne/local-lvm": {disk("local-lvm", "vm-9999-pvc-abc")},
		"bne/rbd":       {},
	}}

	detector := newDetector(t, disks, tenant("local-lvm", "rbd"),
		recorded("pvc-abc", "rbd", "bne/pve-1/rbd/vm-9999-pvc-abc"))

	report, err := detector.Sweep(t.Context())
	require.NoError(t, err)

	require.Len(t, report.Orphans, 1)
	assert.Equal(t, "local-lvm", report.Orphans[0].Storage)
	require.Len(t, report.Missing, 1)
	assert.Equal(t, "pvc-abc", report.Missing[0].Name)
}
