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

// Proxmox PV Migrate utility
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	cobra "github.com/spf13/cobra"

	clilog "github.com/sergelogvinov/proxmox-csi-plugin/pkg/log"
	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
)

// requireMigrationCredentials verifies each cluster carries credentials capable of
// copying volumes. Which credentials those are depends on the endpoint the copy will
// go to, so it asks the cluster the same question the copy asks: the per-cluster
// token_copy_endpoint / proxmod_endpoint keys win, and the --token-copy-endpoint /
// --proxmod-endpoint flags are the fallback for clusters that set neither.
//
// Copying through PVE's built-in "copy" method needs username/password, because that
// method carries no permission check and PVE therefore restricts it to root@pam. The
// two token copy endpoints are ACL-gated instead — identical credential requirements,
// different server-side implementations — so either accepts an API token.
func requireMigrationCredentials(clusters []*pxpool.ProxmoxCluster, tokenEndpoint, proxmodEndpoint bool) error {
	for _, cl := range clusters {
		hasUserPass := cl.Username != "" && cl.Password != ""

		endpoint := cl.CopyEndpoint(tokenEndpoint, proxmodEndpoint)
		if endpoint == pxpool.CopyEndpointBuiltin {
			if !hasUserPass {
				return fmt.Errorf("cluster %s: copies through Proxmox' built-in endpoint, which requires a root account (username/password); "+
					"to use an API token instead, install a token copy endpoint and set proxmod_endpoint (or token_copy_endpoint) on the cluster, or pass --%s or --%s",
					cl.Region, flagProxmodEndpoint, flagTokenCopyEndpoint)
			}

			continue
		}

		hasToken := (cl.TokenID != "" || cl.TokenIDFile != "") && (cl.TokenSecret != "" || cl.TokenSecretFile != "")
		if !hasToken && !hasUserPass {
			return fmt.Errorf("cluster %s: the %s copy endpoint requires an API token (token_id/token_secret) or username/password in the config file", cl.Region, endpoint)
		}
	}

	return nil
}

var (
	command = "pvecsictl"
	version = "v0.0.0"
	commit  = "none"

	cloudconfig string
	kubeconfig  string

	flagLogLevel = "log-level"

	flagProxmoxConfig = "config"
	flagKubeConfig    = "kubeconfig"

	flagTokenCopyEndpoint = "token-copy-endpoint"
	flagProxmodEndpoint   = "proxmod-endpoint"

	logger *log.Entry
)

func main() {
	if exitCode := run(); exitCode != 0 {
		os.Exit(exitCode)
	}
}

func run() int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := log.New()
	l.SetOutput(os.Stdout)
	l.SetLevel(log.InfoLevel)

	logger = l.WithContext(ctx)

	cmd := cobra.Command{
		Use:     command,
		Version: fmt.Sprintf("%s (commit: %s)", version, commit),
		Short:   "A command-line utility to manipulate PersistentVolume/PersistentVolumeClaim on Proxmox VE",
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			f := cmd.Flags()
			loglvl, _ := f.GetString(flagLogLevel) //nolint: errcheck

			clilog.Configure(logger, loglvl)

			return nil
		},
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	cmd.PersistentFlags().String(flagLogLevel, clilog.LevelInfo,
		fmt.Sprintf("log level, must be one of: %s", strings.Join(clilog.Levels, ", ")))

	cmd.PersistentFlags().StringVar(&cloudconfig, flagProxmoxConfig, "", "proxmox cluster config file")
	cmd.PersistentFlags().StringVar(&kubeconfig, flagKubeConfig, "", "kubernetes config file")

	cmd.PersistentFlags().Bool(flagTokenCopyEndpoint, false,
		"use the token-authorized copy endpoint (hack/pve-token-copy) instead of the root@pam-only built-in copy; needs the pve-csi-copy package on nodes; per-cluster override: token_copy_endpoint")

	cmd.PersistentFlags().Bool(flagProxmodEndpoint, false,
		"use the proxmod extension's copy endpoint (hack/proxmod-csi-storage) instead of the root@pam-only "+
			"built-in copy; needs the proxmod and proxmox-csi-storage packages on nodes; wins over --"+
			flagTokenCopyEndpoint+"; per-cluster override: proxmod_endpoint")

	cmd.AddCommand(buildMigrateCmd())
	cmd.AddCommand(buildRenameCmd())
	cmd.AddCommand(buildSwapCmd())
	cmd.AddCommand(buildCleanCmd())
	cmd.AddCommand(buildControllerCmd())
	cmd.AddCommand(buildEvacuateCmd())
	cmd.AddCommand(buildRebalanceCmd())

	err := cmd.ExecuteContext(ctx)
	if err != nil {
		errorString := err.Error()
		if strings.Contains(errorString, "arg(s)") || strings.Contains(errorString, "flag") || strings.Contains(errorString, "command") {
			fmt.Fprintf(os.Stderr, "Error: %s\n\n", errorString)
			fmt.Fprintln(os.Stderr, cmd.UsageString())
		} else {
			logger.Errorf("Error: %s\n", errorString)
		}

		return 1
	}

	return 0
}
