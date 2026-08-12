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

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	cobra "github.com/spf13/cobra"

	csiconfig "github.com/sergelogvinov/proxmox-csi-plugin/pkg/config"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/migrator"
	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	tools "github.com/sergelogvinov/proxmox-csi-plugin/pkg/tools/kubernetes"
	volume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"

	rbacv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientkubernetes "k8s.io/client-go/kubernetes"
)

type evacuateCmd struct {
	pclient *pxpool.ProxmoxPool
	kclient *clientkubernetes.Clientset
	regions []string
}

// evacuateMove is one planned PVC evacuation. targetStorage is empty when the
// volume keeps its storage name on the target zone; it carries the blessed
// (StorageClass-named) storage when the target zone does not host the source
// storage name.
type evacuateMove struct {
	pv            *corev1.PersistentVolume
	pvcNS         string
	pvcName       string
	storage       string
	targetStorage string
	size          int64
	target        string
}

func buildEvacuateCmd() *cobra.Command {
	c := &evacuateCmd{}

	cmd := cobra.Command{
		Use:           "evacuate proxmox-node",
		Aliases:       []string{"e"},
		Short:         "Evacuate all CSI volumes from a Proxmox node",
		Args:          cobra.ExactArgs(1),
		PreRunE:       c.evacuateValidate,
		RunE:          c.runEvacuate,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	setEvacuateCmdFlags(&cmd)

	return &cmd
}

func setEvacuateCmdFlags(cmd *cobra.Command) {
	flags := cmd.Flags()

	flags.String("region", "", "limit evacuation to one Proxmox cluster (region)")
	flags.String("target", "", "move all volumes to this Proxmox node (default: pick per volume by capacity)")
	flags.Float64("headroom", migrator.DefaultHeadroom, "fractional free-space margin required on the target")

	flags.BoolP("force", "f", false, "request force migration for volumes that are in use by pods")
	flags.Bool("dry-run", false, "print the planned moves and exit without changing anything")
	flags.Bool("now", false, "run the migrations synchronously instead of stamping annotations for the controller")
	flags.Int("max-failures", 1, "abort after this many failed migrations (only with --now)")
	flags.Int("timeout", 10800, "task timeout in seconds (only with --now)")
	flags.Int("helper-vmid", migrator.DefaultHelperVMID, "VM ID of the transient helper VM used to convert qcow2/vmdk volumes (only with --now)")
}

//nolint:gocyclo,cyclop
func (c *evacuateCmd) runEvacuate(cmd *cobra.Command, args []string) error {
	flags := cmd.Flags()

	region, _ := flags.GetString("region")                       //nolint: errcheck
	target, _ := flags.GetString("target")                       //nolint: errcheck
	headroom, _ := flags.GetFloat64("headroom")                  //nolint: errcheck
	force, _ := flags.GetBool("force")                           //nolint: errcheck
	dryRun, _ := flags.GetBool("dry-run")                        //nolint: errcheck
	now, _ := flags.GetBool("now")                               //nolint: errcheck
	maxFailures, _ := flags.GetInt("max-failures")               //nolint: errcheck
	taskTimeout, _ := flags.GetInt("timeout")                    //nolint: errcheck
	helperVMID, _ := flags.GetInt("helper-vmid")                 //nolint: errcheck
	tokenCopyEndpoint, _ := flags.GetBool(flagTokenCopyEndpoint) //nolint: errcheck
	proxmodEndpoint, _ := flags.GetBool(flagProxmodEndpoint)     //nolint: errcheck

	ctx := context.Background()
	zone := args[0]

	regions := c.regions
	if region != "" {
		regions = []string{region}
	}

	moves := []evacuateMove{}

	for _, r := range regions {
		cluster, err := c.pclient.GetProxmoxCluster(r)
		if err != nil {
			return fmt.Errorf("failed to get Proxmox cluster %s: %v", r, err)
		}

		pvs, err := tools.PVsInZone(ctx, c.kclient, csi.DriverName, r, zone)
		if err != nil {
			return err
		}

		// Cross-storage-aware candidates per source storage: the storages named
		// by the driver's StorageClasses, so a cluster whose zones use different
		// storage names still yields a target.
		candidates := map[string]*migrator.StorageCandidates{}

		for i := range pvs {
			pv := &pvs[i]

			if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Kind != "PersistentVolumeClaim" {
				logger.Warnf("skipping PV %s: not bound to a PVC", pv.Name)

				continue
			}

			vol, err := volume.NewVolumeFromVolumeID(pv.Spec.CSI.VolumeHandle)
			if err != nil {
				continue
			}

			size := pv.Spec.Capacity[corev1.ResourceStorage]

			if _, ok := candidates[vol.Storage()]; !ok {
				cands, cerr := migrator.GatherStorageCandidates(ctx, cluster, c.kclient, vol.Storage())
				if cerr != nil {
					return fmt.Errorf("failed to get candidate capacities for storage %s: %v", vol.Storage(), cerr)
				}

				candidates[vol.Storage()] = cands
			}

			pvcTarget := target

			var pvcStorage string

			if pvcTarget == "" {
				pvcTarget, pvcStorage, err = migrator.SelectCrossStorageTarget(candidates[vol.Storage()], zone, vol.Storage(), size.Value(), headroom)
				if err != nil {
					logger.Errorf("no evacuation target for %s/%s (storage %s): %v", pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name, vol.Storage(), err)

					continue
				}
			} else {
				// Explicit target node: when it does not host the source storage
				// name, resolve the storage the same blessed-StorageClass way.
				pvcStorage, err = candidates[vol.Storage()].StorageForZone(pvcTarget, vol.Storage())
				if err != nil {
					logger.Errorf("no storage on target %s for %s/%s (storage %s): %v", pvcTarget, pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name, vol.Storage(), err)

					continue
				}
			}

			if pvcStorage == vol.Storage() {
				pvcStorage = ""
			}

			moves = append(moves, evacuateMove{
				pv:            pv,
				pvcNS:         pv.Spec.ClaimRef.Namespace,
				pvcName:       pv.Spec.ClaimRef.Name,
				storage:       vol.Storage(),
				targetStorage: pvcStorage,
				size:          size.Value(),
				target:        pvcTarget,
			})
		}
	}

	if len(moves) == 0 {
		logger.Infof("no volumes to evacuate from proxmox node %s", zone)

		return nil
	}

	if dryRun {
		fmt.Printf("%-40s %-10s %-12s %-12s %s\n", "PVC", "SIZE", "SOURCE", "TARGET", "STORAGE")

		for _, m := range moves {
			storage := m.storage
			if m.targetStorage != "" {
				storage = m.storage + " -> " + m.targetStorage
			}

			fmt.Printf("%-40s %-10d %-12s %-12s %s\n", m.pvcNS+"/"+m.pvcName, m.size, zone, m.target, storage)
		}

		return nil
	}

	if !now {
		// Stamp migration request annotations; the migration controller picks
		// them up and executes one at a time.
		for i, m := range moves {
			logger.Infof("requesting migration %d/%d: %s/%s %s -> %s", i+1, len(moves), m.pvcNS, m.pvcName, zone, m.target)

			if err := annotatePVCMigration(ctx, c.kclient, m.pvcNS, m.pvcName, m.target, m.targetStorage, force); err != nil {
				return err
			}
		}

		logger.Infof("requested migration of %d volumes from proxmox node %s, the migration controller will process them", len(moves), zone)

		return nil
	}

	// Synchronous mode: migrate one volume at a time.
	m := &migrator.Migrator{
		KClient:           c.kclient,
		PClient:           c.pclient,
		Logger:            logger,
		HelperVMID:        helperVMID,
		TokenCopyEndpoint: tokenCopyEndpoint,
		ProxmodEndpoint:   proxmodEndpoint,
	}

	failures := 0

	for i, move := range moves {
		logger.Infof("evacuating %d/%d: %s/%s %s -> %s", i+1, len(moves), move.pvcNS, move.pvcName, zone, move.target)

		err := m.Migrate(ctx, migrator.Request{
			Namespace:     move.pvcNS,
			PVCName:       move.pvcName,
			TargetNode:    move.target,
			TargetStorage: move.targetStorage,
			Force:         force,
			TaskTimeout:   taskTimeout,
		})
		if errors.Is(err, migrator.ErrSharedStorage) {
			logger.Infof("skipping %s/%s: %v", move.pvcNS, move.pvcName, err)

			continue
		}

		if err != nil {
			failures++

			logger.Errorf("failed to evacuate %s/%s: %v", move.pvcNS, move.pvcName, err)

			if failures >= maxFailures {
				return fmt.Errorf("aborting evacuation after %d failures", failures)
			}
		}
	}

	logger.Infof("evacuated %d/%d volumes from proxmox node %s", len(moves)-failures, len(moves), zone)

	return nil
}

// annotatePVCMigration stamps the migration request annotations on a PVC.
// targetStorage is optional: when set, the volume also moves into that storage
// on the target zone (cross-storage migration).
func annotatePVCMigration(ctx context.Context, kclient clientkubernetes.Interface, namespace, name, target, targetStorage string, force bool) error {
	annotations := map[string]string{
		migrator.AnnotationMigrateNode: target,
	}
	if targetStorage != "" {
		annotations[migrator.AnnotationMigrateStorage] = targetStorage
	}

	if force {
		annotations[migrator.AnnotationMigrateForce] = "true"
	}

	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": annotations}})
	if err != nil {
		return err
	}

	_, err = kclient.CoreV1().PersistentVolumeClaims(namespace).Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to annotate PVC %s/%s: %v", namespace, name, err)
	}

	return nil
}

func (c *evacuateCmd) evacuateValidate(cmd *cobra.Command, _ []string) error {
	flags := cmd.Flags()

	cfg, err := csiconfig.ReadCloudConfigFromFile(cloudconfig)
	if err != nil {
		return fmt.Errorf("failed to read config: %v", err)
	}

	// Synchronous migration moves disks through the Proxmox API and needs copy
	// credentials (root@pam, or an API token with --token-copy-endpoint or
	// --proxmod-endpoint);
	// annotation mode only needs API read access.
	if now, _ := flags.GetBool("now"); now { //nolint: errcheck
		tokenCopyEndpoint, _ := flags.GetBool(flagTokenCopyEndpoint) //nolint: errcheck
		proxmodEndpoint, _ := flags.GetBool(flagProxmodEndpoint)     //nolint: errcheck

		if err := requireMigrationCredentials(cfg.Clusters, tokenCopyEndpoint, proxmodEndpoint); err != nil {
			return err
		}
	}

	for _, cl := range cfg.Clusters {
		c.regions = append(c.regions, cl.Region)
	}

	c.pclient, err = pxpool.NewProxmoxPool(cfg.Clusters)
	if err != nil {
		return fmt.Errorf("failed to create Proxmox cluster client: %v", err)
	}

	if err = c.pclient.CheckClusters(context.TODO()); err != nil {
		return fmt.Errorf("failed to initialize Proxmox clusters: %v", err)
	}

	kclientConfig, _, err := tools.BuildConfig(kubeconfig, "")
	if err != nil {
		return fmt.Errorf("failed to create kubernetes config: %v", err)
	}

	c.kclient, err = clientkubernetes.NewForConfig(kclientConfig)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %v", err)
	}

	accessCheck := []rbacv1.ResourceAttributes{
		{Group: "", Namespace: "", Resource: "persistentvolumes", Verb: "list"},
		{Group: "", Namespace: "", Resource: "persistentvolumeclaims", Verb: "patch"},
		// Cross-storage target selection reads the driver's StorageClasses.
		{Group: "storage.k8s.io", Namespace: "", Resource: "storageclasses", Verb: "list"},
	}

	if now, _ := flags.GetBool("now"); now { //nolint: errcheck
		accessCheck = append(accessCheck,
			rbacv1.ResourceAttributes{Group: "", Namespace: "", Resource: "persistentvolumeclaims", Verb: "create"},
			rbacv1.ResourceAttributes{Group: "", Namespace: "", Resource: "persistentvolumeclaims", Verb: "delete"},
			rbacv1.ResourceAttributes{Group: "", Namespace: "", Resource: "persistentvolumes", Verb: "create"},
			rbacv1.ResourceAttributes{Group: "", Namespace: "", Resource: "persistentvolumes", Verb: "delete"},
			rbacv1.ResourceAttributes{Group: "", Namespace: "", Resource: "pods", Verb: "delete"},
			rbacv1.ResourceAttributes{Group: "", Namespace: "", Resource: "nodes", Verb: "patch"},
			// The --now path runs Migrate, whose rewire lists namespace
			// ResourceQuotas for the target-class quota pre-flight.
			rbacv1.ResourceAttributes{Group: "", Namespace: "", Resource: "resourcequotas", Verb: "list"},
		)
	}

	return checkPermissions(context.TODO(), c.kclient, accessCheck)
}
