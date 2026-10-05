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

// Proxmox CSI volume control plane.
//
// The operator is the only component in the estate that holds a Proxmox
// credential. Tenant clusters ask for volumes by creating custom resources in
// this cluster; this binary reconciles them against Proxmox.
//
// It serves an optional gRPC volume API (-volume-api-address) that tenant
// clusters use to provision volumes remotely. The API runs on every replica
// (not leader-gated) and authenticates callers via OIDC bearer tokens.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/config"
	volumeapi "github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/api"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/controller/attachment"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/controller/drift"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/controller/snapshot"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/controller/storage"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/controller/tenant"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/controller/volume"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/component-base/metrics/legacyregistry"
)

// errNoCloudConfig is returned when the operator is started without one.
//
// There is no default and no discovery: a control plane whose whole point is to
// be the single holder of a Proxmox credential must be told explicitly which
// credential that is.
var errNoCloudConfig = errors.New("-cloud-config is required")

// Set by the linker; see GO_LDFLAGS in the Makefile.
var (
	version = "dev"
	commit  = "none"
)

var (
	showVersion = flag.Bool("version", false,
		"Print the version and exit.")
	cloudConfig = flag.String("cloud-config", "",
		"Path to the Proxmox cloud config. This file, and the Secret any token_ref in it names, "+
			"are the only Proxmox credentials in the estate.")
	metricsAddress = flag.String("metrics-address", ":8080",
		"TCP address for the metrics server.")
	probeAddress = flag.String("health-probe-address", ":8081",
		"TCP address for the health and readiness probes.")
	leaderElect = flag.Bool("leader-elect", true,
		"Elect a leader before running controllers. Leave this on: it is what makes the driver's "+
			"process-local VM config lock globally correct.")
	leaderElectionNamespace = flag.String("leader-election-namespace", "",
		"Namespace holding the leader election lease. Defaults to the operator's own namespace.")
	storageSyncPeriod = flag.Duration("storage-sync-period", storage.DefaultSyncPeriod,
		"How often the Proxmox storage catalog is republished.")
	tenantResolvePeriod = flag.Duration("tenant-resolve-period", tenant.DefaultResolvePeriod,
		"How often a tenant's PVE pool membership is re-resolved. This is the window in which a VM "+
			"removed from a tenant's pool still authorizes an attach, so lowering it tightens that window.")
	volumeSyncPeriod = flag.Duration("volume-sync-period", volume.DefaultSyncPeriod,
		"How often an adopted volume is re-confirmed against Proxmox.")
	driftInterval = flag.Duration("drift-interval", drift.DefaultInterval,
		"How often the ledger is swept against Proxmox. The sweep only reports.")
	driftDetector = flag.Bool("drift-detector", true,
		"Run the drift detector. It never mutates anything; turning it off only turns off the reporting.")

	volumeAPIAddress = flag.String("volume-api-address", "",
		"TCP address for the gRPC volume API (e.g. :9090). Empty disables the API. "+
			"Runs on every replica, not leader-gated.")
	volumeAPIRate = flag.Float64("volume-api-rate", 10,
		"Per-tenant requests-per-second rate limit for the volume API.")
	volumeAPIBurst = flag.Int("volume-api-burst", 20,
		"Per-tenant burst limit for the volume API.")
)

// scheme carries the CRD types plus the built-in kinds the manager needs for
// leader election and for resolving token references.
var scheme = runtime.NewScheme() //nolint:gochecknoglobals

func init() {
	mustRegister(clientscheme.AddToScheme(scheme))
	mustRegister(v1alpha1.AddToScheme(scheme))
}

func mustRegister(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	if *showVersion {
		fmt.Printf("proxmox-csi-operator %s (%s)\n", version, commit)

		return
	}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// main holds no defers of its own so that os.Exit here cannot skip the
	// teardown in run.
	os.Exit(realMain())
}

func realMain() int {
	logger := ctrl.Log.WithName("setup")
	logger.Info("starting", "version", version, "commit", commit)

	if err := run(ctrl.SetupSignalHandler()); err != nil {
		logger.Error(err, "exiting")

		return 1
	}

	logger.Info("shutdown complete")

	return 0
}

func run(ctx context.Context) error {
	logger := ctrl.Log.WithName("setup")

	// Argument validation before anything that touches the environment, so a
	// missing flag is reported as a missing flag rather than as whatever the
	// environment happened to be missing too.
	if *cloudConfig == "" {
		return errNoCloudConfig
	}

	restConfig, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("loading kubeconfig: %w", err)
	}

	pool, features, err := proxmoxPool(ctx, restConfig)
	if err != nil {
		return err
	}

	// Fail fast rather than at the first reconcile. An operator that starts,
	// wins the lease and only then discovers it cannot authenticate is an
	// operator that has taken the lease away from a replica that could.
	if err := pool.CheckClusters(ctx); err != nil {
		return fmt.Errorf("checking Proxmox clusters: %w", err)
	}

	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsOptions(*metricsAddress),
		HealthProbeBindAddress:  *probeAddress,
		LeaderElection:          *leaderElect,
		LeaderElectionID:        "proxmox-csi-operator.csi.crunchymonkies.com",
		LeaderElectionNamespace: *leaderElectionNamespace,
		// Give up the lease on a clean shutdown so a rolling update does not
		// wait out the full lease duration with nothing reconciling.
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		return fmt.Errorf("building manager: %w", err)
	}

	// One adapter, handed to each controller through the narrow interface it is
	// allowed to use: the publisher gets a StorageReader, the tenant reconciler
	// gets a VMReader, and neither surface has a method that writes.
	pve := proxmox.NewPool(pool)
	writer := proxmox.NewDiskWriter(pool)

	publisher := &storage.Publisher{
		Client:     mgr.GetClient(),
		Proxmox:    pve,
		SyncPeriod: *storageSyncPeriod,
	}

	if err := mgr.Add(publisher); err != nil {
		return fmt.Errorf("adding the storage publisher: %w", err)
	}

	tenants := &tenant.Reconciler{
		Client:        mgr.GetClient(),
		Proxmox:       pve,
		ResolvePeriod: *tenantResolvePeriod,
	}

	if err := tenants.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("adding the tenant reconciler: %w", err)
	}

	volumes := &volume.Reconciler{
		Client:     mgr.GetClient(),
		Proxmox:    pve,
		Writer:     writer,
		SyncPeriod: *volumeSyncPeriod,
	}

	if err := volumes.SetupWithManager(ctx, mgr); err != nil {
		return fmt.Errorf("adding the volume reconciler: %w", err)
	}

	attachments := &attachment.Reconciler{
		Client:                 mgr.GetClient(),
		Writer:                 writer,
		VMLock:                 proxmox.NewVMLock(),
		ReassignVolumeOnAttach: features.ReassignVolumeOnAttach,
	}

	if err := attachments.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("adding the attachment reconciler: %w", err)
	}

	snapshots := &snapshot.Reconciler{
		Client: mgr.GetClient(),
		Writer: writer,
	}

	if err := snapshots.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("adding the snapshot reconciler: %w", err)
	}

	if *driftDetector {
		// Handed the manager's cached reader rather than its client. The
		// detector has no business writing a resource, and the type it holds is
		// the cheapest way to keep it that way through every future edit.
		detector := &drift.Detector{
			Client:   mgr.GetCache(),
			Proxmox:  pve,
			Interval: *driftInterval,
		}

		if err := mgr.Add(detector); err != nil {
			return fmt.Errorf("adding the drift detector: %w", err)
		}
	}

	if *volumeAPIAddress != "" {
		if err := volumeapi.SetupIndexes(ctx, mgr); err != nil {
			return fmt.Errorf("setting up volume API indexes: %w", err)
		}

		apiServer := volumeapi.NewServer(volumeapi.ServerConfig{ //nolint:contextcheck // Stream interceptors handle context via the stream.
			Address: *volumeAPIAddress,
			Client:  mgr.GetClient(),
			VMs:     pve,
			Rate:    *volumeAPIRate,
			Burst:   *volumeAPIBurst,
		})

		if err := mgr.Add(apiServer); err != nil {
			return fmt.Errorf("adding the volume API server: %w", err)
		}

		logger.Info("volume API enabled", "address", *volumeAPIAddress)
	}

	if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		return fmt.Errorf("adding the liveness check: %w", err)
	}

	// Liveness is a ping and readiness is the catalog, deliberately. A stale
	// catalog means this replica should stop being read from; it does not mean
	// the process is wedged, and restarting it would not refresh anything.
	if err := mgr.AddReadyzCheck("storage-catalog", publisher.Check); err != nil {
		return fmt.Errorf("adding the readiness check: %w", err)
	}

	logger.Info("running", "leaderElection", *leaderElect, "regions", pool.GetRegions())

	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("running manager: %w", err)
	}

	return nil
}

// metricsOptions builds the manager's metrics server options.
//
// The extra handler is the whole point. The Proxmox client pool is the driver's,
// and the driver registers its API collectors into component-base's legacy
// registry rather than controller-runtime's. The shared transport increments
// proxmox_api_request_retries_total inside this process on every retried read --
// the early warning that the Proxmox API is answering badly before it answers
// badly enough to fail a volume operation -- and without this handler nothing
// could ever scrape it.
//
// That counter is the only one of the three that moves here.
// proxmox_api_request_duration_seconds and proxmox_api_request_errors_total are
// observed from pkg/csi, which this binary does not import, so they are exposed
// and stay at zero.
func metricsOptions(bindAddress string) server.Options {
	opts := server.Options{BindAddress: bindAddress}

	// "0" is controller-runtime's "do not listen"; the chart passes it when
	// metrics are disabled. Nothing would serve the handler in that case.
	if bindAddress == "0" {
		return opts
	}

	opts.ExtraHandlers = map[string]http.Handler{
		"/metrics/proxmox": legacyregistry.Handler(),
	}

	return opts
}

// proxmoxPool builds the Proxmox client pool from the cloud config and returns
// the cloud config features alongside it.
//
// The config format, its validation and its token_ref indirection are the
// driver's, reused rather than reimplemented: the operator and the driver must
// agree on what a cluster definition means, and the cheapest way to guarantee
// that is to share the parser.
func proxmoxPool(ctx context.Context, restConfig *rest.Config) (*proxmoxpool.ProxmoxPool, config.ClustersFeatures, error) {
	if *cloudConfig == "" {
		return nil, config.ClustersFeatures{}, errNoCloudConfig
	}

	cfg, err := config.ReadCloudConfigFromFile(*cloudConfig)
	if err != nil {
		return nil, config.ClustersFeatures{}, fmt.Errorf("reading %s: %w", *cloudConfig, err)
	}

	kclient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, config.ClustersFeatures{}, fmt.Errorf("building kubernetes client: %w", err)
	}

	namespace := *leaderElectionNamespace
	if namespace == "" {
		namespace = inClusterNamespace()
	}

	if err := proxmoxpool.ResolveTokenRefs(ctx, kclient, namespace, cfg.Clusters); err != nil {
		return nil, config.ClustersFeatures{}, fmt.Errorf("resolving Proxmox token references: %w", err)
	}

	pool, err := proxmoxpool.NewProxmoxPool(cfg.Clusters)
	if err != nil {
		return nil, config.ClustersFeatures{}, fmt.Errorf("building the Proxmox client pool: %w", err)
	}

	return pool, cfg.Features, nil
}

// serviceAccountNamespaceFile is where the projected service account token
// volume publishes the pod's namespace.
const serviceAccountNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// inClusterNamespace returns the namespace the operator is running in, or "" if
// it cannot tell -- out of cluster, for instance.
func inClusterNamespace() string {
	data, err := os.ReadFile(serviceAccountNamespaceFile)
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(data))
}
