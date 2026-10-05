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

// Package drift compares the ledger against Proxmox and reports the difference.
//
// It reports and stops. It does not delete an orphan, does not recreate a
// missing disk, and does not write to the custom resources at all -- the reader
// it holds cannot write to Proxmox, and it is given no writing client for the
// management cluster either. That is deliberate rather than incremental: a
// detector confident enough to act is a detector that deletes a tenant's data
// the first time it misreads a transient hypervisor error, and the two directions
// it reports have opposite correct responses that only a human knows.
//
// A sweep answers one question in each direction:
//
//   - Which disks that look like this driver's are on a storage with nothing in
//     the ledger claiming them? Before cut-over this is the signal that some
//     cluster has been provisioning outside the control plane. After cut-over it
//     is a leak.
//   - Which recorded volumes have no disk behind them? Almost always a storage
//     that could not be read, which is why an unreadable storage suppresses this
//     direction entirely rather than reporting every volume on it as vanished.
package drift

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"
	pvevolume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"
)

// DefaultInterval is how often the estate is swept.
//
// Deliberately slow. A sweep lists the content of every storage every tenant is
// allowed to use, which is the most expensive thing this operator does to the
// hypervisor, and nothing it finds is urgent: drift is a condition to notice
// within the hour, not a race to win.
const DefaultInterval = 30 * time.Minute

// driverDiskPrefix is the disk-name suffix every volume this driver creates
// carries, because the CSI provisioner names volumes after the PV.
//
// The filter is what keeps the orphan count meaningful. A Proxmox storage is
// full of things that are supposed to be there -- real VM disks, ISOs, backups,
// linked-clone bases -- and reporting them would produce a permanently non-zero
// number that teaches everyone to ignore the alert, which is worse than not
// having it.
const driverDiskPrefix = "pvc-"

// Detector sweeps the estate on a timer and reports what disagrees.
type Detector struct {
	// Client reads the ledger. Reads only; nothing here writes a resource.
	Client client.Reader
	// Proxmox reads storage content. Read-only by type.
	Proxmox proxmox.DiskReader
	// Interval defaults to DefaultInterval.
	Interval time.Duration
	// Now defaults to time.Now.
	Now func() time.Time
}

// Orphan is a disk on a storage that no ledger entry claims.
type Orphan struct {
	// Region, Storage and Name locate the disk.
	Region  string
	Storage string
	Name    string
	// VMID is the owner Proxmox reports, which is the first thing a human
	// investigating an orphan wants: it usually names the cluster that created
	// it, via that cluster's placeholder.
	VMID int32
	// SizeBytes is the size Proxmox reports.
	SizeBytes int64
}

// Missing is a ledger entry with no disk behind it.
type Missing struct {
	// Namespace and Name locate the ProxmoxVolume.
	Namespace string
	Name      string
	// VolumeID is the handle the tenant holds, which is what a
	// PersistentVolume would still be pointing at.
	VolumeID string
}

// Report is the outcome of one sweep.
type Report struct {
	// Orphans and Missing are sorted, so two consecutive reports are diffable.
	Orphans []Orphan
	Missing []Missing
	// Unreadable names the storages the sweep could not list, as "region/storage".
	//
	// Reported as a first-class result rather than logged and forgotten: a sweep
	// that could not read half the estate found far fewer orphans than a sweep
	// that read all of it, and a reader comparing two reports without this
	// number would read that as an improvement.
	Unreadable []string
}

// Metrics. Named for the question they answer rather than for the code that
// produces them, since the alert on them will outlive this package.
var (
	labels = []string{"region", "storage"}

	orphanedDisks = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "proxmox_csi_drift_orphaned_disks",
		Help: "Disks named like a CSI volume that no ProxmoxVolume claims.",
	}, labels)

	missingDisks = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "proxmox_csi_drift_missing_disks",
		Help: "ProxmoxVolumes whose disk was not found on a storage that was read successfully.",
	}, labels)

	unreadableStorages = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "proxmox_csi_drift_unreadable_storages",
		Help: "Storages the last sweep could not list. Suppresses the missing-disk count for those storages.",
	}, labels)

	lastSweep = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "proxmox_csi_drift_last_sweep_timestamp_seconds",
		Help: "When the last sweep completed. Alert on this going stale, not only on the counts.",
	})
)

func init() {
	metrics.Registry.MustRegister(orphanedDisks, missingDisks, unreadableStorages, lastSweep)
}

// NeedLeaderElection keeps the sweep on one replica.
//
// Not for correctness -- nothing here writes anything -- but because the gauges
// are per-process and two replicas publishing the same estate would make the
// alert depend on which one Prometheus scraped.
func (d *Detector) NeedLeaderElection() bool {
	return true
}

// Start runs sweeps until the context is canceled.
func (d *Detector) Start(ctx context.Context) error {
	// Swept immediately, because the first useful moment for this is the one
	// right after the operator is installed and before anything is enforced --
	// runbook step 6 -- and waiting half an hour for it is friction in exactly
	// the place the runbook needs none.
	d.sweepAndReport(ctx)

	ticker := time.NewTicker(d.interval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			d.sweepAndReport(ctx)
		}
	}
}

// Sweep compares the ledger against Proxmox once.
//
// Exported so the sweep is testable without a clock, and so a future
// `pvecsictl drift` can run exactly what the operator runs rather than a second
// implementation that agrees with it only most of the time.
func (d *Detector) Sweep(ctx context.Context) (Report, error) {
	volumes := &v1alpha1.ProxmoxVolumeList{}
	if err := d.Client.List(ctx, volumes); err != nil {
		return Report{}, fmt.Errorf("listing volumes: %w", err)
	}

	tenants := &v1alpha1.TenantClusterList{}
	if err := d.Client.List(ctx, tenants); err != nil {
		return Report{}, fmt.Errorf("listing tenants: %w", err)
	}

	claimed, ledger := index(volumes.Items)
	report := Report{}

	for _, target := range targets(tenants.Items, volumes.Items) {
		// Zone is empty on purpose: every node currently serving the storage.
		// A per-zone sweep would report a volume as missing the moment its disk
		// moved node, which is a normal migration rather than drift.
		disks, err := d.Proxmox.ListDisks(ctx, target.region, "", target.storage)
		if err != nil {
			if !errors.Is(err, proxmox.ErrStorageNotFound) {
				log.FromContext(ctx).Error(err, "sweeping a storage", "region", target.region, "storage", target.storage)

				report.Unreadable = append(report.Unreadable, target.String())
				unreadableStorages.WithLabelValues(target.region, target.storage).Set(1)

				continue
			}

			// A storage Proxmox answers does not exist is evidence, not the
			// absence of it: every volume the ledger holds there really is gone.
			// Swept as an empty storage rather than suppressed, because
			// suppressing here would mean that deleting a storage -- or a typo in
			// allowedStorages -- permanently hides exactly the volumes someone
			// needs to be told about, in the one situation this detector exists
			// for.
			log.FromContext(ctx).Info("storage no longer exists; reporting its recorded volumes as missing",
				"region", target.region, "storage", target.storage)

			disks = nil
		}

		unreadableStorages.WithLabelValues(target.region, target.storage).Set(0)

		orphans := orphansIn(disks, claimed)
		report.Orphans = append(report.Orphans, orphans...)
		orphanedDisks.WithLabelValues(target.region, target.storage).Set(float64(len(orphans)))

		// Only for a storage that was read. Whether a volume is missing is a
		// statement about a listing this sweep actually obtained; on a storage
		// it could not read it has no evidence either way, and saying "missing"
		// there is the one mistake that would send someone looking for a disk
		// that is fine.
		missing := missingIn(disks, ledger[target])
		report.Missing = append(report.Missing, missing...)
		missingDisks.WithLabelValues(target.region, target.storage).Set(float64(len(missing)))
	}

	sortReport(&report)

	return report, nil
}

func (d *Detector) sweepAndReport(ctx context.Context) {
	logger := log.FromContext(ctx).WithName("drift")

	report, err := d.Sweep(ctx)
	if err != nil {
		logger.Error(err, "sweep failed")

		return
	}

	lastSweep.Set(float64(d.now().Unix()))

	// Logged individually rather than as counts. A count tells someone there is
	// a problem; the runbook step that says "explain every orphan" needs the
	// names, and an operator log is where they will look first.
	for _, orphan := range report.Orphans {
		logger.Info("orphaned disk", "region", orphan.Region, "storage", orphan.Storage,
			"disk", orphan.Name, "vmid", orphan.VMID, "sizeBytes", orphan.SizeBytes)
	}

	for _, missing := range report.Missing {
		logger.Info("recorded volume with no disk", "namespace", missing.Namespace,
			"volume", missing.Name, "volumeID", missing.VolumeID)
	}

	logger.Info("sweep complete", "orphans", len(report.Orphans),
		"missing", len(report.Missing), "unreadable", report.Unreadable)
}

func (d *Detector) interval() time.Duration {
	if d.Interval > 0 {
		return d.Interval
	}

	return DefaultInterval
}

func (d *Detector) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}

	return time.Now()
}

// target is one storage in one region.
type target struct {
	region  string
	storage string
}

func (t target) String() string {
	return t.region + "/" + t.storage
}

// targets is every storage worth sweeping.
//
// The union of what tenants are allowed to use and what the ledger already
// records, and the second half is what makes the sweep honest: a storage removed
// from allowedStorages still holds the volumes that were provisioned while it
// was allowed, and a sweep scoped to policy would quietly stop watching exactly
// the disks nobody is going to think about again.
func targets(tenants []v1alpha1.TenantCluster, volumes []v1alpha1.ProxmoxVolume) []target {
	seen := make(map[target]struct{})

	for i := range tenants {
		for _, storage := range tenants[i].Spec.AllowedStorages {
			seen[target{region: tenants[i].Spec.Region, storage: storage}] = struct{}{}
		}
	}

	for i := range volumes {
		if volumes[i].Spec.Region == "" || volumes[i].Spec.Storage == "" {
			continue
		}

		seen[target{region: volumes[i].Spec.Region, storage: volumes[i].Spec.Storage}] = struct{}{}
	}

	targets := make([]target, 0, len(seen))
	for t := range seen {
		targets = append(targets, t)
	}

	sort.Slice(targets, func(i, j int) bool {
		if targets[i].region != targets[j].region {
			return targets[i].region < targets[j].region
		}

		return targets[i].storage < targets[j].storage
	})

	return targets
}

// index builds the two lookups a sweep needs: which disks are claimed, and which
// volumes each storage is expected to hold.
//
// Both are keyed on the rename-stable suffix rather than the disk name. Matching
// on the full name would report every attached volume twice over -- once as an
// orphan under the name it has, once as missing under the name the ledger
// recorded -- because reassign-on-attach renames a disk for the VM holding it.
func index(volumes []v1alpha1.ProxmoxVolume) (map[string]struct{}, map[target][]Missing) {
	claimed := make(map[string]struct{}, len(volumes))
	ledger := make(map[target][]Missing)

	for i := range volumes {
		vol := &volumes[i]

		if vol.Status.VolumeID == "" {
			continue
		}

		handle, err := pvevolume.NewVolumeFromVolumeID(vol.Status.VolumeID)
		if err != nil {
			continue
		}

		suffix := handle.DiskSuffix()
		if suffix == "" {
			continue
		}

		key := target{region: handle.Region(), storage: handle.Storage()}

		claimed[claimKey(key, suffix)] = struct{}{}
		ledger[key] = append(ledger[key], Missing{
			Namespace: vol.Namespace,
			Name:      vol.Name,
			VolumeID:  vol.Status.VolumeID,
		})
	}

	return claimed, ledger
}

// claimKey scopes a suffix to its storage.
//
// Not global: the same pvc-<uuid> suffix genuinely appears on two storages after
// a clone or a storage migration whose source was never cleaned up, and a global
// key would let a disk on one storage vouch for a volume on another.
func claimKey(t target, suffix string) string {
	return t.String() + "/" + suffix
}

// orphansIn returns the driver-shaped disks in a listing that nothing claims.
func orphansIn(disks []proxmox.Disk, claimed map[string]struct{}) []Orphan {
	orphans := []Orphan{}

	for i := range disks {
		disk := &disks[i]

		suffix := pvevolume.NewVolume(disk.Region, "", disk.Storage, disk.Name).DiskSuffix()
		if !strings.HasPrefix(suffix, driverDiskPrefix) {
			continue
		}

		key := claimKey(target{region: disk.Region, storage: disk.Storage}, suffix)
		if _, ok := claimed[key]; ok {
			continue
		}

		orphans = append(orphans, Orphan{
			Region:    disk.Region,
			Storage:   disk.Storage,
			Name:      disk.Name,
			VMID:      disk.VMID,
			SizeBytes: disk.SizeBytes,
		})
	}

	return orphans
}

// missingIn returns the expected volumes a listing does not account for.
func missingIn(disks []proxmox.Disk, expected []Missing) []Missing {
	present := make(map[string]struct{}, len(disks))

	for i := range disks {
		suffix := pvevolume.NewVolume(disks[i].Region, "", disks[i].Storage, disks[i].Name).DiskSuffix()
		if suffix != "" {
			present[suffix] = struct{}{}
		}
	}

	missing := []Missing{}

	for _, volume := range expected {
		handle, err := pvevolume.NewVolumeFromVolumeID(volume.VolumeID)
		if err != nil {
			continue
		}

		if _, ok := present[handle.DiskSuffix()]; !ok {
			missing = append(missing, volume)
		}
	}

	return missing
}

func sortReport(report *Report) {
	sort.Slice(report.Orphans, func(i, j int) bool {
		if report.Orphans[i].Region != report.Orphans[j].Region {
			return report.Orphans[i].Region < report.Orphans[j].Region
		}

		if report.Orphans[i].Storage != report.Orphans[j].Storage {
			return report.Orphans[i].Storage < report.Orphans[j].Storage
		}

		return report.Orphans[i].Name < report.Orphans[j].Name
	})

	sort.Slice(report.Missing, func(i, j int) bool {
		if report.Missing[i].Namespace != report.Missing[j].Namespace {
			return report.Missing[i].Namespace < report.Missing[j].Namespace
		}

		return report.Missing[i].Name < report.Missing[j].Name
	})

	sort.Strings(report.Unreadable)
}
