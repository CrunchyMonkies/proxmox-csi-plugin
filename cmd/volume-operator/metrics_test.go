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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"
	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	testcluster "github.com/sergelogvinov/proxmox-csi-plugin/test/cluster"
)

func TestProxmoxAPIMetricsAreScrapeable(t *testing.T) {
	// The failure this prevents is a silent one. The driver's client pool counts
	// Proxmox API retries into component-base's legacy registry while the manager
	// serves controller-runtime's, so the counter increments inside this process
	// and nothing can read it -- which looks exactly like a healthy hypervisor.
	//
	// Asserted end to end rather than by inspecting the map, because "the handler
	// is installed" and "the number a human would alert on comes out of it" are
	// different claims and only the second one is worth anything.
	handler, ok := metricsOptions(":8080").ExtraHandlers["/metrics/proxmox"]
	require.True(t, ok, "the driver's registry has to be served somewhere")

	httpmock.Activate()

	defer httpmock.DeactivateAndReset()

	testcluster.SetupMockResponders()

	pool, err := pxpool.NewProxmoxPool([]*pxpool.ProxmoxCluster{{
		URL:         "https://127.0.0.2:8006/api2/json",
		TokenID:     "user!token-id",
		TokenSecret: "secret",
		Region:      "cluster-1",
	}})
	require.NoError(t, err)

	// Provoke the retry rather than assert on an empty registry: a counter vec
	// publishes nothing until a label combination is used, so a scrape taken
	// before any retry happened would pass whether or not the bridge worked.
	testcluster.FailNextReads(2)

	_, err = proxmox.NewPool(pool).ListVMs(t.Context(), "cluster-1")
	require.NoError(t, err)

	res := httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/metrics/proxmox", nil))

	require.Equal(t, http.StatusOK, res.Code)
	assert.Contains(t, res.Body.String(), "proxmox_api_request_retries_total",
		"the retry the transport just made has to reach the scrape")
}

func TestNoMetricsHandlersWhenTheListenerIsOff(t *testing.T) {
	// The chart passes -metrics-address=0 when metrics are disabled, which is
	// controller-runtime's "do not listen at all".
	assert.Empty(t, metricsOptions("0").ExtraHandlers)
}
