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
	"fmt"
	"strings"
	"time"

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
	clientkubernetes "k8s.io/client-go/kubernetes"
)

type rebalanceCmd struct {
	pclient *pxpool.ProxmoxPool
	kclient *clientkubernetes.Clientset
	regions []string
}

func buildRebalanceCmd() *cobra.Command {
	c := &rebalanceCmd{}

	cmd := cobra.Command{
		Use:           "rebalance",
		Short:         "Rebalance idle CSI volumes from overloaded Proxmox nodes to emptier ones",
		Args:          cobra.ExactArgs(0),
		PreRunE:       c.rebalanceValidate,
		RunE:          c.runRebalance,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	setRebalanceCmdFlags(&cmd)

	return &cmd
}

func setRebalanceCmdFlags(cmd *cobra.Command) {
	flags := cmd.Flags()

	flags.String("region", "", "limit rebalancing to one Proxmox cluster (region)")
	flags.String("storage", "", "limit rebalancing to one Proxmox storage")

	flags.Float64("high-threshold", 0.80, "zones above this used fraction are rebalancing sources")
	flags.Float64("low-threshold", 0.60, "only zones below this used fraction are rebalancing targets")
	flags.Float64("headroom", migrator.DefaultHeadroom, "fractional free-space margin required on the target")
	flags.Int("max-migrations", 2, "maximum volumes to move per run")

	flags.String("window", "", `maintenance window "HH:MM-HH:MM"; outside the window the command exits without doing anything`)
	flags.String("window-tz", "UTC", "IANA time zone for the maintenance window")

	flags.Bool("dry-run", false, "print the planned moves and exit without changing anything")
}

//nolint:gocyclo,cyclop
func (c *rebalanceCmd) runRebalance(cmd *cobra.Command, _ []string) error {
	flags := cmd.Flags()

	region, _ := flags.GetString("region")                 //nolint: errcheck
	storageFilter, _ := flags.GetString("storage")         //nolint: errcheck
	highThreshold, _ := flags.GetFloat64("high-threshold") //nolint: errcheck
	lowThreshold, _ := flags.GetFloat64("low-threshold")   //nolint: errcheck
	headroom, _ := flags.GetFloat64("headroom")            //nolint: errcheck
	maxMigrations, _ := flags.GetInt("max-migrations")     //nolint: errcheck
	window, _ := flags.GetString("window")                 //nolint: errcheck
	windowTz, _ := flags.GetString("window-tz")            //nolint: errcheck
	dryRun, _ := flags.GetBool("dry-run")                  //nolint: errcheck

	ctx := context.Background()

	if window != "" {
		inWindow, err := withinWindow(time.Now(), window, windowTz)
		if err != nil {
			return err
		}

		if !inWindow {
			logger.Infof("outside maintenance window %s (%s), nothing to do", window, windowTz)

			return nil
		}
	}

	regions := c.regions
	if region != "" {
		regions = []string{region}
	}

	// Collect candidate volumes per (region, storage).
	pvList, err := c.kclient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list PersistentVolumes: %v", err)
	}

	type group struct {
		region  string
		storage string
	}

	candidates := map[group][]migrator.VolumeInfo{}

	for i := range pvList.Items {
		pv := &pvList.Items[i]

		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csi.DriverName {
			continue
		}

		if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Kind != "PersistentVolumeClaim" {
			continue
		}

		vol, verr := volume.NewVolumeFromVolumeID(pv.Spec.CSI.VolumeHandle)
		if verr != nil || vol.Zone() == "" {
			continue
		}

		if storageFilter != "" && vol.Storage() != storageFilter {
			continue
		}

		inRegion := false

		for _, r := range regions {
			if vol.Region() == r {
				inRegion = true

				break
			}
		}

		if !inRegion {
			continue
		}

		pods, _, perr := tools.PVCPodUsage(ctx, c.kclient, pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name)
		if perr != nil {
			return fmt.Errorf("failed to check pod usage for %s/%s: %v", pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name, perr)
		}

		size := pv.Spec.Capacity[corev1.ResourceStorage]

		key := group{region: vol.Region(), storage: vol.Storage()}
		candidates[key] = append(candidates[key], migrator.VolumeInfo{
			Namespace: pv.Spec.ClaimRef.Namespace,
			PVCName:   pv.Spec.ClaimRef.Name,
			PVName:    pv.Name,
			Zone:      vol.Zone(),
			SizeBytes: size.Value(),
			InUse:     len(pods) > 0,
		})
	}

	moves := []migrator.Move{}
	budget := maxMigrations

	for key, vols := range candidates {
		if budget <= 0 {
			break
		}

		cluster, cerr := c.pclient.GetProxmoxCluster(key.region)
		if cerr != nil {
			return fmt.Errorf("failed to get Proxmox cluster %s: %v", key.region, cerr)
		}

		zones, zerr := migrator.ZoneCapacities(ctx, cluster, key.storage)
		if zerr != nil {
			logger.Errorf("skipping storage %s in region %s: %v", key.storage, key.region, zerr)

			continue
		}

		planned := migrator.Plan(zones, vols, highThreshold, lowThreshold, headroom, budget)
		budget -= len(planned)

		moves = append(moves, planned...)
	}

	if len(moves) == 0 {
		logger.Infof("cluster is balanced, nothing to do")

		return nil
	}

	if dryRun {
		fmt.Printf("%-40s %-12s %-12s %s\n", "PVC", "SOURCE", "TARGET", "SIZE")

		for _, m := range moves {
			fmt.Printf("%-40s %-12s %-12s %d\n", m.Namespace+"/"+m.PVCName, m.Source, m.Target, m.SizeBytes)
		}

		return nil
	}

	for i, m := range moves {
		logger.Infof("requesting migration %d/%d: %s/%s %s -> %s", i+1, len(moves), m.Namespace, m.PVCName, m.Source, m.Target)

		// Rebalance never force-drains: only idle volumes are planned.
		if err := annotatePVCMigration(ctx, c.kclient, m.Namespace, m.PVCName, m.Target, false); err != nil {
			return err
		}
	}

	logger.Infof("requested %d migrations, the migration controller will process them", len(moves))

	return nil
}

// withinWindow reports whether now falls inside the "HH:MM-HH:MM" window in
// the given time zone. Windows may span midnight (e.g. "22:00-04:00").
func withinWindow(now time.Time, window, tz string) (bool, error) {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return false, fmt.Errorf("invalid --window-tz %q: %v", tz, err)
	}

	parts := strings.SplitN(window, "-", 2)
	if len(parts) != 2 {
		return false, fmt.Errorf(`invalid --window %q, expected "HH:MM-HH:MM"`, window)
	}

	start, err := time.Parse("15:04", strings.TrimSpace(parts[0]))
	if err != nil {
		return false, fmt.Errorf("invalid --window start %q: %v", parts[0], err)
	}

	end, err := time.Parse("15:04", strings.TrimSpace(parts[1]))
	if err != nil {
		return false, fmt.Errorf("invalid --window end %q: %v", parts[1], err)
	}

	local := now.In(loc)
	minutes := local.Hour()*60 + local.Minute()
	startMin := start.Hour()*60 + start.Minute()
	endMin := end.Hour()*60 + end.Minute()

	if startMin <= endMin {
		return minutes >= startMin && minutes < endMin, nil
	}

	// Window spans midnight.
	return minutes >= startMin || minutes < endMin, nil
}

func (c *rebalanceCmd) rebalanceValidate(_ *cobra.Command, _ []string) error {
	cfg, err := csiconfig.ReadCloudConfigFromFile(cloudconfig)
	if err != nil {
		return fmt.Errorf("failed to read config: %v", err)
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
		{Group: "", Namespace: "", Resource: "pods", Verb: "list"},
	}

	return checkPermissions(context.TODO(), c.kclient, accessCheck)
}
