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
	"errors"
	"fmt"

	cobra "github.com/spf13/cobra"

	csiconfig "github.com/sergelogvinov/proxmox-csi-plugin/pkg/config"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/migrator"
	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	tools "github.com/sergelogvinov/proxmox-csi-plugin/pkg/tools/kubernetes"

	rbacv1 "k8s.io/api/authorization/v1"
	clientkubernetes "k8s.io/client-go/kubernetes"
)

type migrateCmd struct {
	pclient   *pxpool.ProxmoxPool
	kclient   *clientkubernetes.Clientset
	namespace string
}

func buildMigrateCmd() *cobra.Command {
	c := &migrateCmd{}

	cmd := cobra.Command{
		Use:           "migrate pvc proxmox-node",
		Aliases:       []string{"m"},
		Short:         "Migrate data from one Proxmox node to another",
		Args:          cobra.ExactArgs(2),
		PreRunE:       c.migrationValidate,
		RunE:          c.runMigration,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	setMigrateCmdFlags(&cmd)

	return &cmd
}

func setMigrateCmdFlags(cmd *cobra.Command) {
	flags := cmd.Flags()

	flags.StringP("namespace", "n", "", "namespace of the persistentvolumeclaims")

	flags.BoolP("force", "f", false, "force migration even if the persistentvolumeclaims is in use")
	flags.Int("timeout", 10800, "task timeout in seconds")
	flags.String("storage", "", "move the disk into this storage on the target node (default: keep the current storage name)")
	flags.Int("helper-vmid", migrator.DefaultHelperVMID, "VM ID of the transient helper VM used to convert qcow2/vmdk volumes")
}

func (c *migrateCmd) runMigration(cmd *cobra.Command, args []string) error {
	flags := cmd.Flags()
	force, _ := flags.GetBool("force")                           //nolint: errcheck
	taskTimeout, _ := flags.GetInt("timeout")                    //nolint: errcheck
	targetStorage, _ := flags.GetString("storage")               //nolint: errcheck
	helperVMID, _ := flags.GetInt("helper-vmid")                 //nolint: errcheck
	tokenCopyEndpoint, _ := flags.GetBool(flagTokenCopyEndpoint) //nolint: errcheck

	m := &migrator.Migrator{
		KClient:           c.kclient,
		PClient:           c.pclient,
		Logger:            logger,
		HelperVMID:        helperVMID,
		TokenCopyEndpoint: tokenCopyEndpoint,
	}

	err := m.Migrate(context.Background(), migrator.Request{
		Namespace:     c.namespace,
		PVCName:       args[0],
		TargetNode:    args[1],
		TargetStorage: targetStorage,
		Force:         force,
		TaskTimeout:   taskTimeout,
	})
	if errors.Is(err, migrator.ErrSharedStorage) {
		logger.Infof("%v, nothing to do", err)

		return nil
	}

	return err
}

// nolint: dupl
func (c *migrateCmd) migrationValidate(cmd *cobra.Command, _ []string) error {
	flags := cmd.Flags()

	cfg, err := csiconfig.ReadCloudConfigFromFile(cloudconfig)
	if err != nil {
		return fmt.Errorf("failed to read config: %v", err)
	}

	tokenCopyEndpoint, _ := flags.GetBool(flagTokenCopyEndpoint) //nolint: errcheck
	if err := requireMigrationCredentials(cfg.Clusters, tokenCopyEndpoint); err != nil {
		return err
	}

	c.pclient, err = pxpool.NewProxmoxPool(cfg.Clusters)
	if err != nil {
		return fmt.Errorf("failed to create Proxmox cluster client: %v", err)
	}

	if err = c.pclient.CheckClusters(context.TODO()); err != nil {
		return fmt.Errorf("failed to initialize Proxmox clusters: %v", err)
	}

	namespace, _ := flags.GetString("namespace") //nolint: errcheck

	kclientConfig, namespace, err := tools.BuildConfig(kubeconfig, namespace)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes config: %v", err)
	}

	c.kclient, err = clientkubernetes.NewForConfig(kclientConfig)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %v", err)
	}

	c.namespace = namespace

	accessCheck := []rbacv1.ResourceAttributes{
		{Group: "", Namespace: "", Resource: "persistentvolumeclaims", Verb: "create"},
		{Group: "", Namespace: "", Resource: "persistentvolumeclaims", Verb: "delete"},
		{Group: "", Namespace: "", Resource: "persistentvolumes", Verb: "create"},
		{Group: "", Namespace: "", Resource: "persistentvolumes", Verb: "delete"},
		{Group: "", Namespace: "", Resource: "pods", Verb: "delete"},
		{Group: "", Namespace: "", Resource: "nodes", Verb: "get"},
		{Group: "", Namespace: "", Resource: "nodes", Verb: "patch"},
	}

	return checkPermissions(context.TODO(), c.kclient, accessCheck)
}
