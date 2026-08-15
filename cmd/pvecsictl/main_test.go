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
	"testing"

	"github.com/stretchr/testify/assert"

	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
)

func tokenCluster(region string) *pxpool.ProxmoxCluster {
	return &pxpool.ProxmoxCluster{
		URL:         "https://127.0.0.1:8006/api2/json",
		TokenID:     "kubernetes-csi@pve!csi",
		TokenSecret: "secret",
		Region:      region,
	}
}

func rootCluster(region string) *pxpool.ProxmoxCluster {
	return &pxpool.ProxmoxCluster{
		URL:      "https://127.0.0.1:8006/api2/json",
		Username: "root@pam",
		Password: "strong-password",
		Region:   region,
	}
}

func TestRequireMigrationCredentials(t *testing.T) {
	truePtr, falsePtr := true, false

	withProxmod := func(cl *pxpool.ProxmoxCluster, v *bool) *pxpool.ProxmoxCluster {
		cl.ProxmodEndpoint = v

		return cl
	}

	withTokenCopy := func(cl *pxpool.ProxmoxCluster, v *bool) *pxpool.ProxmoxCluster {
		cl.TokenCopyEndpoint = v

		return cl
	}

	tests := []struct {
		msg             string
		clusters        []*pxpool.ProxmoxCluster
		tokenEndpoint   bool
		proxmodEndpoint bool
		expectedError   string
	}{
		{
			msg:      "root credentials, no endpoint: the built-in copy is root-only and root is what it has",
			clusters: []*pxpool.ProxmoxCluster{rootCluster("cluster-1")},
		},
		{
			msg:           "token only, no endpoint anywhere: the built-in copy cannot use it",
			clusters:      []*pxpool.ProxmoxCluster{tokenCluster("cluster-1")},
			expectedError: "cluster cluster-1: copies through Proxmox' built-in endpoint, which requires a root account",
		},
		{
			// The defect this test was written for: the copy resolves the endpoint per
			// cluster, so the check has to as well, or a working config is rejected.
			msg:      "token only with proxmod_endpoint on the cluster and no flag",
			clusters: []*pxpool.ProxmoxCluster{withProxmod(tokenCluster("cluster-1"), &truePtr)},
		},
		{
			msg:      "token only with token_copy_endpoint on the cluster and no flag",
			clusters: []*pxpool.ProxmoxCluster{withTokenCopy(tokenCluster("cluster-1"), &truePtr)},
		},
		{
			msg:             "token only with the proxmod flag and no per-cluster key",
			clusters:        []*pxpool.ProxmoxCluster{tokenCluster("cluster-1")},
			proxmodEndpoint: true,
		},
		{
			// The same defect in the other direction: the flag used to make every
			// cluster look token-capable, including one that opted back out and will
			// therefore drive the root-only built-in endpoint.
			msg:             "cluster opts out of proxmod while the flag is on, and holds no root credentials",
			clusters:        []*pxpool.ProxmoxCluster{withProxmod(tokenCluster("cluster-1"), &falsePtr)},
			proxmodEndpoint: true,
			expectedError:   "cluster cluster-1: copies through Proxmox' built-in endpoint, which requires a root account",
		},
		{
			msg:             "cluster opts out of proxmod while the flag is on, but holds root credentials",
			clusters:        []*pxpool.ProxmoxCluster{withProxmod(rootCluster("cluster-1"), &falsePtr)},
			proxmodEndpoint: true,
		},
		{
			msg: "mixed fleet: one cluster on proxmod with a token, one on the built-in with root",
			clusters: []*pxpool.ProxmoxCluster{
				withProxmod(tokenCluster("cluster-1"), &truePtr),
				rootCluster("cluster-2"),
			},
		},
		{
			msg: "mixed fleet reports the cluster that is actually short of credentials",
			clusters: []*pxpool.ProxmoxCluster{
				withProxmod(tokenCluster("cluster-1"), &truePtr),
				tokenCluster("cluster-2"),
			},
			expectedError: "cluster cluster-2: copies through Proxmox' built-in endpoint, which requires a root account",
		},
		{
			msg: "an endpoint is configured but the cluster carries no credentials at all",
			clusters: []*pxpool.ProxmoxCluster{
				withProxmod(&pxpool.ProxmoxCluster{URL: "https://127.0.0.1:8006/api2/json", Region: "cluster-1"}, &truePtr),
			},
			expectedError: "cluster cluster-1: the proxmod-endpoint copy endpoint requires an API token",
		},
		{
			msg: "token supplied through files rather than inline",
			clusters: []*pxpool.ProxmoxCluster{
				withProxmod(&pxpool.ProxmoxCluster{
					URL:             "https://127.0.0.1:8006/api2/json",
					TokenIDFile:     "/etc/proxmox/token_id",
					TokenSecretFile: "/etc/proxmox/token_secret",
					Region:          "cluster-1",
				}, &truePtr),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.msg, func(t *testing.T) {
			err := requireMigrationCredentials(test.clusters, test.tokenEndpoint, test.proxmodEndpoint)

			if test.expectedError == "" {
				assert.NoError(t, err)

				return
			}

			assert.Error(t, err)
			assert.Contains(t, err.Error(), test.expectedError)
		})
	}
}
