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

// Package migrator implements the volume migration orchestration shared by
// the pvecsictl CLI and the annotation-driven migration controller.
package migrator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	goproxmox "github.com/sergelogvinov/go-proxmox"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	tools "github.com/sergelogvinov/proxmox-csi-plugin/pkg/tools/kubernetes"
	toolsproxmox "github.com/sergelogvinov/proxmox-csi-plugin/pkg/tools/proxmox"
	provider "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/provider"
	volume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientkubernetes "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
)

// Annotation keys of the migration protocol. Request annotations are set by
// operators or producer tools (evacuate/rebalance), status annotations are
// written by the migration controller, and the state annotation is stamped
// on the PV to make a crashed migration resumable.
const (
	// AnnotationMigrate is the legacy explicit enable flag.
	AnnotationMigrate = csi.DriverName + "/migrate"
	// AnnotationMigrateNode requests a migration to the given Proxmox node (zone).
	AnnotationMigrateNode = csi.DriverName + "/migrate-node"
	// AnnotationMigrateForce opts the PVC in to force-drain (cordon + pod delete).
	AnnotationMigrateForce = csi.DriverName + "/migrate-force"
	// AnnotationMigrateStorage optionally selects a different storage on the
	// target node (used when the source storage name does not exist there).
	AnnotationMigrateStorage = csi.DriverName + "/migrate-storage"

	// AnnotationMigratePhase is the current phase of the migration.
	AnnotationMigratePhase = csi.DriverName + "/migrate-phase"
	// AnnotationMigrateMessage is the last error or progress message.
	AnnotationMigrateMessage = csi.DriverName + "/migrate-message"
	// AnnotationMigrateAttempts counts reconcile attempts.
	AnnotationMigrateAttempts = csi.DriverName + "/migrate-attempts"
	// AnnotationMigrateStartedAt is the RFC3339 time the migration started.
	AnnotationMigrateStartedAt = csi.DriverName + "/migrate-started-at"

	// AnnotationMigrateState is stamped on the PV before the disk move so an
	// interrupted migration can resume at the rewire step (PV survives PVC recreate).
	AnnotationMigrateState = csi.DriverName + "/migrate-state"

	// AnnotationEvacuate on a Kubernetes Node requests evacuation of all CSI
	// volumes from the node's Proxmox zone. Value is a target zone or "auto".
	AnnotationEvacuate = csi.DriverName + "/evacuate"
	// AnnotationEvacuateForce opts evacuated PVCs in to force-drain.
	AnnotationEvacuateForce = csi.DriverName + "/evacuate-force"
)

// Migration phases reported via AnnotationMigratePhase.
const (
	PhasePending   = "Pending"
	PhaseDraining  = "Draining"
	PhaseMoving    = "Moving"
	PhaseRewiring  = "Rewiring"
	PhaseCompleted = "Completed"
	PhaseFailed    = "Failed"
	PhaseSkipped   = "Skipped"
)

// DefaultTaskTimeout is the default Proxmox move-task timeout in seconds.
const DefaultTaskTimeout = 10800

// DefaultHelperVMID is the default VM ID of the transient helper VM used to
// convert qcow2/vmdk volumes to raw before migration. It must NOT be the
// volume owner VM ID: destroying the helper then can only ever free the
// helper's own disposable conversion copy, never the original volume.
const DefaultHelperVMID = 9998

// Sentinel errors that callers (the migration controller) can classify as
// terminal rather than retryable.
var (
	// ErrInvalidTarget means the target zone does not exist or does not host the volume's storage.
	ErrInvalidTarget = errors.New("invalid migration target")
	// ErrAlreadyOnTarget means the volume is already on the requested zone.
	ErrAlreadyOnTarget = errors.New("volume already on target node")
	// ErrInUse means the volume is used by pods and force was not requested.
	ErrInUse = errors.New("volume is in use")
	// ErrSharedStorage means the disk is already visible on the target node's
	// storage: the storage is shared between the nodes and no move is needed
	// (running one would interrupt, and on a shared file self-overwrite).
	ErrSharedStorage = errors.New("storage is shared with the target node")
)

// Migrator migrates the backing Proxmox disk of a PVC to another Proxmox node.
type Migrator struct {
	KClient clientkubernetes.Interface
	PClient *pxpool.ProxmoxPool

	// Recorder emits Kubernetes events on the PVC. Optional.
	Recorder record.EventRecorder
	// Logger is the CLI-style logger. Optional.
	Logger *log.Entry
	// OnPhase is called when the migration enters a new phase. Optional.
	OnPhase func(phase string)

	// HelperVMID is the VM ID of the transient helper VM used to convert
	// qcow2/vmdk volumes (default DefaultHelperVMID).
	HelperVMID int

	// TokenCopyEndpoint routes the volume copy through the permission-gated
	// endpoint from hack/pve-token-copy (POST .../storage/{storage}/csi-copy)
	// instead of PVE's built-in root@pam-only "copy" method, so the migrator can
	// run with a scoped API token. Requires the pve-csi-copy package (>= 0.2.0)
	// on the Proxmox nodes. Default false preserves the built-in (root@pam)
	// behavior.
	TokenCopyEndpoint bool
}

// Request describes one volume migration.
type Request struct {
	Namespace  string
	PVCName    string
	TargetNode string

	// TargetStorage moves the disk into a different storage on the target
	// node. Empty keeps the volume's current storage name.
	TargetStorage string

	// Force allows disrupting pods that use the PVC (cordon CSI nodes + delete pods).
	Force bool

	// TaskTimeout is the Proxmox move-task timeout in seconds (default 10800).
	TaskTimeout int
	// DrainTimeout bounds the wait for pods to terminate. Zero means unbounded.
	DrainTimeout time.Duration
	// DetachTimeout bounds the wait for the disk to detach from the VM. Zero means unbounded.
	DetachTimeout time.Duration
}

// Migrate moves the PVC's backing disk to the target Proxmox node and rewires
// the PV/PVC topology. It is resumable: if a previous attempt crashed after
// the disk move, the move step is skipped.
//
// The disk is copied (not moved) to the target and the rewire forces the source
// PV to Retain, so once the new PV is verified and bound Migrate reclaims the
// leftover source disk copy to recover the space (a reclaim failure is warned
// about but does not fail the already-succeeded migration).
//
//nolint:gocyclo,cyclop
func (m *Migrator) Migrate(ctx context.Context, req Request) error {
	taskTimeout := req.TaskTimeout
	if taskTimeout <= 0 {
		taskTimeout = DefaultTaskTimeout
	}

	kubePVC, kubePV, err := tools.PVCResources(ctx, m.KClient, req.Namespace, req.PVCName)
	if err != nil {
		return fmt.Errorf("failed to get resources: %v", err)
	}

	if kubePV.Spec.CSI == nil || kubePV.Spec.CSI.Driver != csi.DriverName {
		return fmt.Errorf("persistentvolume %s is not provisioned by Proxmox CSI driver", kubePV.Name)
	}

	vol, err := volume.NewVolumeFromVolumeID(kubePV.Spec.CSI.VolumeHandle)
	if err != nil {
		return fmt.Errorf("failed to parse volume ID: %v", err)
	}

	if vol.Node() == "" {
		return fmt.Errorf("%w: persistentvolumeclaims %s is on shared storage %s, no migration needed", ErrInvalidTarget, req.PVCName, vol.Storage())
	}

	// The storage the disk will live on after the migration: the volume's
	// current storage name unless a different target storage was requested.
	targetStorage := req.TargetStorage
	if targetStorage == "" {
		targetStorage = vol.Storage()
	}

	// A same-node request is a no-op only when the storage stays the same too;
	// with a different target storage it is a same-node storage move.
	if vol.Node() == req.TargetNode && targetStorage == vol.Storage() {
		return fmt.Errorf("%w: persistentvolumeclaims %s is already on proxmox node %s (storage %s)", ErrAlreadyOnTarget, req.PVCName, req.TargetNode, targetStorage)
	}

	cluster, err := m.PClient.GetProxmoxCluster(vol.Cluster())
	if err != nil {
		return fmt.Errorf("failed to get Proxmox cluster: %v", err)
	}

	// Pre-flight: the target node must host the target storage. Never cordon
	// or drain for an invalid target.
	zones, err := cluster.GetNodesForStorage(ctx, targetStorage)
	if err != nil {
		if errors.Is(err, goproxmox.ErrNotFound) {
			return fmt.Errorf("%w: storage %s does not exist", ErrInvalidTarget, targetStorage)
		}

		return fmt.Errorf("failed to get nodes for storage %s: %v", targetStorage, err)
	}

	if !slices.Contains(zones, req.TargetNode) {
		return fmt.Errorf("%w: proxmox node %s does not have storage %s (nodes: %s)", ErrInvalidTarget, req.TargetNode, targetStorage, strings.Join(zones, ","))
	}

	// qcow2/vmdk volumes cannot be streamed by the copy-volume endpoint (PVE
	// exports them via qemu-img convert to a pipe, which fails); they are
	// first converted to a raw copy through a transient helper VM (file-to-
	// file conversion), and the raw copy is then moved with the standard
	// copy endpoint. The volume therefore becomes raw on the target.
	convertFirst := strings.HasSuffix(vol.Disk(), ".qcow2") || strings.HasSuffix(vol.Disk(), ".vmdk")

	// The volume as it will exist on the target (used for resume detection
	// and the topology rewire): same name, with qcow2/vmdk extensions
	// becoming .raw.
	targetVol := volume.NewVolume(vol.Region(), req.TargetNode, targetStorage, targetDiskName(vol.Disk()))

	// The disk is fully transferred only when it is at least as large as the
	// volume's capacity; anything smaller is a partial file left by an
	// interrupted move.
	expectedSize := kubePV.Spec.Capacity.Storage().Value()

	// Resume detection: a previous attempt stamped the PV and already moved the
	// disk, but crashed before rewiring the PV/PVC topology.
	skipMove := false

	if kubePV.Annotations[AnnotationMigrateState] == req.TargetNode {
		onTarget, size, derr := toolsproxmox.DiskOnNode(ctx, cluster, targetVol, req.TargetNode)

		switch {
		case derr == nil && onTarget && size >= expectedSize:
			m.logf("disk %s already on proxmox node %s, resuming interrupted migration", targetVol.Disk(), req.TargetNode)

			skipMove = true
		case derr == nil && onTarget:
			// A previous move attempt left a partial file: remove it so the
			// move can be retried cleanly (the import refuses to overwrite).
			m.logf("partial disk %s on proxmox node %s (%d of %d bytes), deleting before retrying the move", targetVol.Disk(), req.TargetNode, size, expectedSize)

			if derr = toolsproxmox.DeleteDisk(ctx, cluster, targetVol, req.TargetNode, 300); derr != nil {
				return fmt.Errorf("failed to delete partial disk on %s: %v", req.TargetNode, derr)
			}
		}
	} else if vol.Node() != req.TargetNode {
		// Shared-storage pre-flight: if the source disk is already visible on
		// the target node, the storage is shared between the nodes — there is
		// nothing to move, and an export/import would overwrite the file with
		// itself. Skip instead of interrupting. Meaningless for same-node
		// storage moves: the source disk is trivially present on its own node.
		shared, _, derr := toolsproxmox.DiskOnNode(ctx, cluster, vol, req.TargetNode)
		if derr == nil && shared {
			return fmt.Errorf("%w: disk %s is already present on proxmox node %s (storage %s)", ErrSharedStorage, vol.Disk(), req.TargetNode, vol.Storage())
		}
	}

	pods, vmName, err := tools.PVCPodUsage(ctx, m.KClient, req.Namespace, req.PVCName)
	if err != nil {
		return fmt.Errorf("failed to find pods using pvc: %v", err)
	}

	// Resolve the Proxmox VM ID before the force-drain path so we capture
	// the node while it is still reachable.
	var vmID int

	if vmName != "" {
		kubeNode, nodeErr := m.KClient.CoreV1().Nodes().Get(ctx, vmName, metav1.GetOptions{})
		if nodeErr != nil {
			return fmt.Errorf("failed to get kubernetes node %s: %v", vmName, nodeErr)
		}

		vmID, nodeErr = csi.ProxmoxVMIDbyNode(kubeNode)
		if nodeErr == nil {
			// The detach wait queries the volume's cluster, where a VMID from
			// another Proxmox cluster would alias an unrelated VM. The
			// instance-id annotation carries no region and is trusted as-is.
			if _, pidRegion, pidErr := provider.ParseProviderID(kubeNode.Spec.ProviderID); pidErr == nil && pidRegion != vol.Region() {
				return fmt.Errorf("node %s providerID places Proxmox VMID %d in region %s, but the volume is in region %s", vmName, vmID, pidRegion, vol.Region())
			}
		} else {
			// No proxmox:// providerID (no Proxmox CCM) and no instance-id
			// annotation: resolve the VM from the Proxmox API like the CSI
			// controller does (FindVMByNode: node-name prefix match verified
			// against the node's SMBIOS system UUID), then by system UUID
			// alone for VMs whose name does not start with the node name.
			// Both lookups verify the UUID, so a node that reports none can
			// only be resolved via the annotation.
			m.logf("no proxmox providerID or %s annotation on node %s, falling back to Proxmox VM lookup", csi.AnnotationProxmoxInstanceID, vmName)

			uuid := kubeNode.Status.NodeInfo.SystemUUID
			if uuid == "" {
				return fmt.Errorf("failed to resolve Proxmox VMID from node %s: %v (the node reports no system UUID to verify a VM lookup against; set the %s annotation)",
					vmName, nodeErr, csi.AnnotationProxmoxInstanceID)
			}

			var vmRegion string

			vmID, vmRegion, nodeErr = m.PClient.FindVMByNode(ctx, kubeNode)
			if nodeErr != nil {
				vmID, vmRegion, nodeErr = m.PClient.FindVMByUUID(ctx, uuid)
				if nodeErr != nil {
					return fmt.Errorf("failed to resolve Proxmox VMID from node %s: %v (no VM matches the node name or system UUID %s)", vmName, nodeErr, uuid)
				}
			}

			// The detach wait queries the volume's cluster, so a VM found in a
			// different Proxmox cluster would alias an unrelated VMID there.
			if vmRegion != vol.Region() {
				return fmt.Errorf("node %s resolved to Proxmox VMID %d in region %s, but the volume is in region %s", vmName, vmID, vmRegion, vol.Region())
			}
		}

		m.logf("resolved kubernetes node %s to Proxmox VMID %d", vmName, vmID)
	}

	cordonedNodes := []string{}

	// Always uncordon the nodes we cordoned, on every exit path. Uses a fresh
	// context because ctx may already be canceled when we get here.
	defer func() { //nolint: contextcheck
		if len(cordonedNodes) == 0 {
			return
		}

		m.logf("uncordoning nodes: %s", strings.Join(cordonedNodes, ","))

		uncordonCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		if uerr := tools.UncondonNodes(uncordonCtx, m.KClient, cordonedNodes); uerr != nil {
			m.logf("failed to uncordon nodes %s: %v", strings.Join(cordonedNodes, ","), uerr)
		}
	}()

	if len(pods) > 0 {
		if !req.Force {
			return fmt.Errorf("%w: persistentvolumeclaims is using by pods: %s on node %s, cannot move volume", ErrInUse, strings.Join(pods, ","), vmName)
		}

		m.phase(PhaseDraining)
		m.logf("persistentvolumeclaims is using by pods: %s on node %s, trying to force migration", strings.Join(pods, ","), vmName)
		m.event(kubePVC, corev1.EventTypeNormal, "MigrationDraining", fmt.Sprintf("draining pods %s for migration to %s", strings.Join(pods, ","), req.TargetNode))

		csiNodes, cerr := tools.CSINodes(ctx, m.KClient, kubePV.Spec.CSI.Driver)
		if cerr != nil {
			return cerr
		}

		m.logf("cordoning nodes: %s", strings.Join(csiNodes, ","))

		cordonedNodes, err = tools.CondonNodes(ctx, m.KClient, csiNodes)
		if err != nil {
			return fmt.Errorf("failed to cordon nodes: %v", err)
		}

		m.logf("terminated pods: %s", strings.Join(pods, ","))

		for _, pod := range pods {
			if err = m.KClient.CoreV1().Pods(req.Namespace).Delete(ctx, pod, metav1.DeleteOptions{}); err != nil {
				return fmt.Errorf("failed to delete pod: %v", err)
			}
		}

		if err = m.waitPodsGone(ctx, req); err != nil {
			return err
		}
	}

	detachCtx := ctx

	if req.DetachTimeout > 0 {
		var cancel context.CancelFunc

		detachCtx, cancel = context.WithTimeout(ctx, req.DetachTimeout)
		defer cancel()
	}

	if err = toolsproxmox.WaitForVolumeDetach(detachCtx, cluster, vmID, vol.Disk()); err != nil {
		return fmt.Errorf("failed to wait for volume detach: %v", err)
	}

	if !skipMove {
		// Stamp the resume marker on the PV before the disk move so a crash
		// between the move and the rewire is recoverable.
		if err = m.annotatePV(ctx, kubePV.Name, AnnotationMigrateState, req.TargetNode); err != nil {
			return fmt.Errorf("failed to annotate PV %s: %v", kubePV.Name, err)
		}

		m.phase(PhaseMoving)
		m.logf("moving disk %s to proxmox node %s (as %s)", vol.Disk(), req.TargetNode, targetVol.VolID())
		m.event(kubePVC, corev1.EventTypeNormal, "MigrationMoving", fmt.Sprintf("moving disk %s to proxmox node %s (as %s)", vol.Disk(), req.TargetNode, targetVol.VolID()))

		if convertFirst {
			if err = m.convertAndMove(ctx, cluster, vol, targetVol, req.TargetNode, taskTimeout); err != nil {
				return fmt.Errorf("failed to migrate disk: %w", err)
			}
		} else if err = toolsproxmox.MoveQemuDisk(ctx, cluster, vol, req.TargetNode, targetVol, taskTimeout, m.useTokenCopy(vol.Region())); err != nil {
			// Best effort: remove the partial target file a failed move may
			// have left behind so retries (and operators) start clean.
			if onTarget, size, derr := toolsproxmox.DiskOnNode(ctx, cluster, targetVol, req.TargetNode); derr == nil && onTarget && size < expectedSize {
				if derr = toolsproxmox.DeleteDisk(ctx, cluster, targetVol, req.TargetNode, 300); derr != nil {
					m.logf("failed to clean up partial disk on %s: %v", req.TargetNode, derr)
				}
			}

			return fmt.Errorf("failed to move disk: %v", err)
		}
	}

	// Never rewire the PV/PVC unless the disk is verifiably and FULLY on the
	// target: the subsequent PVC deletion lets the external provisioner
	// delete the disk at the OLD location, so rewiring after a failed or
	// partial move destroys the only copy of the data.
	onTarget, size, err := toolsproxmox.DiskOnNode(ctx, cluster, targetVol, req.TargetNode)
	if err != nil {
		return fmt.Errorf("failed to verify disk on target node %s: %v", req.TargetNode, err)
	}

	if !onTarget || size < expectedSize {
		return fmt.Errorf("disk %s on %s (storage %s) is missing or partial (%d of %d bytes) after move, refusing to rewire", targetVol.Disk(), req.TargetNode, targetStorage, size, expectedSize)
	}

	m.phase(PhaseRewiring)
	m.logf("replacing persistentvolume topology")

	// %w: rewire failures keep their sentinel (the quota pre-flight aborts with
	// ErrInvalidTarget so the controller can classify it as terminal).
	if err = m.replacePVTopology(ctx, req.Namespace, kubePVC, kubePV, targetVol); err != nil {
		return fmt.Errorf("failed to replace PV topology: %w", err)
	}

	// The migration is fully committed: the new PV is bound to the moved disk and
	// its reclaim policy restored. The rewire forced the OLD PV to Retain, so the
	// external provisioner will NOT reclaim the source disk copy the move left at
	// the origin — reclaim it here to recover the space. This runs only after the
	// bind is verified: every failure path above returns before this point, so the
	// source copy (the safety net) is never deleted until the target is proven good.
	m.reclaimSourceDisk(ctx, cluster, kubePVC, vol, targetVol, taskTimeout)

	m.logf("persistentvolumeclaims %s has been migrated to proxmox node %s", req.PVCName, req.TargetNode)
	m.event(kubePVC, corev1.EventTypeNormal, "MigrationCompleted", fmt.Sprintf("migrated to proxmox node %s", req.TargetNode))

	return nil
}

// convertAndMove migrates a qcow2/vmdk volume in two steps: a transient,
// diskless, stopped helper VM converts it to a raw COPY on the source node
// (move_disk runs qemu-img file-to-file, which works where the streaming
// copy endpoint does not), and the raw copy is then moved to the target with
// the standard copy endpoint.
//
// Safety by construction: the helper VM ID differs from the volume's owner
// VM ID, and Proxmox only ever frees volumes OWNED by a VM being destroyed —
// so destroying the helper (cleanup, crash recovery, any failure path) can
// only free the helper's own disposable conversion copy, never the original.
// useTokenCopy resolves the effective token-copy-endpoint setting for a region: the
// per-cluster config override if set, else the global default (TokenCopyEndpoint).
func (m *Migrator) useTokenCopy(region string) bool {
	if m.PClient == nil {
		return m.TokenCopyEndpoint
	}

	return m.PClient.TokenCopyEndpoint(region, m.TokenCopyEndpoint)
}

func (m *Migrator) convertAndMove(ctx context.Context, cluster *goproxmox.APIClient, vol, targetVol *volume.Volume, targetNode string, taskTimeout int) error {
	vmid := m.HelperVMID
	if vmid <= 0 {
		vmid = DefaultHelperVMID
	}

	if strconv.Itoa(vmid) == vol.VMID() {
		return fmt.Errorf("%w: helper VM ID %d must differ from the volume owner VM ID", ErrInvalidTarget, vmid)
	}

	// Crash recovery: a previous run may have left the helper VM behind.
	// Deleting it frees only its own conversion leftovers — but never touch
	// a VM that is not our helper.
	node, name, found, err := toolsproxmox.FindVMNode(ctx, cluster, vmid)
	if err != nil {
		return err
	}

	if found {
		if name != toolsproxmox.HelperVMName {
			return fmt.Errorf("%w: VM %d already exists (%q) — configure a free helper VM ID", ErrInvalidTarget, vmid, name)
		}

		m.logf("removing stale helper VM %d on %s", vmid, node)

		if derr := toolsproxmox.DeleteVM(ctx, cluster, node, vmid); derr != nil {
			return fmt.Errorf("failed to remove stale helper VM %d: %v", vmid, derr)
		}
	}

	if err := toolsproxmox.CreateHelperVM(ctx, cluster, vol.Node(), vmid); err != nil {
		return err
	}

	// The helper VM (and with it the disposable conversion copy it owns)
	// never outlives the migration attempt, success or failure.
	defer func() { //nolint: contextcheck
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		if derr := toolsproxmox.DeleteVM(cctx, cluster, vol.Node(), vmid); derr != nil {
			m.logf("failed to delete helper VM %d on %s: %v", vmid, vol.Node(), derr)
		}
	}()

	if err := toolsproxmox.AttachDisk(ctx, cluster, vol.Node(), vmid, vol.VolID()); err != nil {
		return err
	}

	m.logf("converting disk %s to raw on %s via helper VM %d", vol.Disk(), vol.Node(), vmid)

	rawVolid, err := toolsproxmox.ConvertDiskToRaw(ctx, cluster, vol.Node(), vmid, vol.Storage(), taskTimeout)
	if err != nil {
		return err
	}

	parts := strings.SplitN(rawVolid, ":", 2)
	if len(parts) != 2 {
		return fmt.Errorf("unexpected converted disk volid %q", rawVolid)
	}

	rawVol := volume.NewVolume(vol.Region(), vol.Node(), parts[0], parts[1])

	m.logf("moving converted disk %s to proxmox node %s (as %s)", rawVol.Disk(), targetNode, targetVol.VolID())

	return toolsproxmox.MoveQemuDisk(ctx, cluster, rawVol, targetNode, targetVol, taskTimeout, m.useTokenCopy(vol.Region()))
}

// reclaimSourceDisk deletes the source disk copy left at the migration origin once
// the moved copy is verified and bound. The migrator copies the disk to the target
// (the source file is not moved), and the claimRef pre-bind forces the source PV to
// Retain, so the external provisioner never reclaims this copy — the migrator does.
//
// It MUST be called only on a fully verified success: the source copy is the sole
// safety net until the moved disk is proven good, so it is never deleted before the
// bind is confirmed (every failure path returns earlier, leaving the source under
// Retain). It handles both cross-node moves (the source lives on the origin node)
// and same-node cross-storage moves (the source lives on the origin storage). The
// shared-storage and same-node/same-storage no-ops are short-circuited before any
// move, so the source and target are always distinct here; the guard below is a
// belt-and-braces check that never deletes the disk that was just migrated to.
//
// A failure only leaves a reclaimable disk behind — a space issue, not a correctness
// one, because the migration itself already succeeded — so it is surfaced as a
// warning (log + event) and never fails the completed migration. It returns true
// when the source was reclaimed (or there was nothing to reclaim).
func (m *Migrator) reclaimSourceDisk(
	ctx context.Context,
	cluster *goproxmox.APIClient,
	pvc *corev1.PersistentVolumeClaim,
	sourceVol, targetVol *volume.Volume,
	taskTimeout int,
) bool {
	// Never touch the target's physical location: identical node, storage and disk
	// would delete the disk the migration just landed on.
	if sourceVol.Node() == targetVol.Node() && sourceVol.Storage() == targetVol.Storage() && sourceVol.Disk() == targetVol.Disk() {
		return true
	}

	if err := toolsproxmox.DeleteDisk(ctx, cluster, sourceVol, sourceVol.Node(), taskTimeout); err != nil {
		m.logf("WARNING: failed to reclaim source disk %s on %s after migration, reclaim it manually to free the space: %v", sourceVol.Disk(), sourceVol.Node(), err)
		m.event(pvc, corev1.EventTypeWarning, "MigrationSourceReclaimFailed",
			fmt.Sprintf("failed to reclaim source disk %s on %s (leftover copy, reclaim manually): %v", sourceVol.Disk(), sourceVol.Node(), err))

		return false
	}

	m.logf("reclaimed source disk %s on %s", sourceVol.Disk(), sourceVol.Node())

	return true
}

// waitPodsGone polls until no pods use the PVC, bounded by req.DrainTimeout (zero = unbounded).
func (m *Migrator) waitPodsGone(ctx context.Context, req Request) error {
	var deadline time.Time
	if req.DrainTimeout > 0 {
		deadline = time.Now().Add(req.DrainTimeout)
	}

	for {
		p, _, err := tools.PVCPodUsage(ctx, m.KClient, req.Namespace, req.PVCName)
		if err != nil {
			return fmt.Errorf("failed to find pods using pvc: %v", err)
		}

		if len(p) == 0 {
			break
		}

		if !deadline.IsZero() && time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for pods to terminate: %s", strings.Join(p, ","))
		}

		m.logf("waiting pods: %s", strings.Join(p, " "))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}

	// Grace period for the kubelet to release the volume.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
	}

	return nil
}

// annotatePV merge-patches a single annotation onto a PV.
func (m *Migrator) annotatePV(ctx context.Context, pvName, key, value string) error {
	patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, key, value))

	_, err := m.KClient.CoreV1().PersistentVolumes().Patch(ctx, pvName, types.MergePatchType, patch, metav1.PatchOptions{})

	return err
}

// setPVReclaimPolicy merge-patches the reclaim policy of a PV. It is used to
// guard the migrated disk (force the old PV to Retain before the destructive
// rewire) and to restore the reserved PV's intended final policy after it binds.
func (m *Migrator) setPVReclaimPolicy(ctx context.Context, pvName string, policy corev1.PersistentVolumeReclaimPolicy) error {
	patch := []byte(fmt.Sprintf(`{"spec":{"persistentVolumeReclaimPolicy":%q}}`, policy))

	_, err := m.KClient.CoreV1().PersistentVolumes().Patch(ctx, pvName, types.MergePatchType, patch, metav1.PatchOptions{})

	return err
}

func (m *Migrator) logf(format string, args ...any) {
	if m.Logger != nil {
		m.Logger.Infof(format, args...)
	}
}

func (m *Migrator) event(pvc *corev1.PersistentVolumeClaim, eventType, reason, message string) {
	if m.Recorder != nil {
		m.Recorder.Event(pvc, eventType, reason, message)
	}
}

func (m *Migrator) phase(phase string) {
	if m.OnPhase != nil {
		m.OnPhase(phase)
	}
}

// targetDiskName returns the disk name to use on the migration target.
// raw+size streams cannot be imported into qcow2/vmdk-named files on
// directory storage (pve-storage Plugin.pm volume_import), so those names
// are converted to .raw; names without an image extension (LVM, ZFS) are
// returned unchanged.
func targetDiskName(disk string) string {
	for _, ext := range []string{".qcow2", ".vmdk"} {
		if strings.HasSuffix(disk, ext) {
			return strings.TrimSuffix(disk, ext) + ".raw"
		}
	}

	return disk
}
