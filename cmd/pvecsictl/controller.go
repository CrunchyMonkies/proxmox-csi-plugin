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
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	cobra "github.com/spf13/cobra"

	csiconfig "github.com/sergelogvinov/proxmox-csi-plugin/pkg/config"
	migrationcontroller "github.com/sergelogvinov/proxmox-csi-plugin/pkg/controller/migration"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/migrator"
	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	tools "github.com/sergelogvinov/proxmox-csi-plugin/pkg/tools/kubernetes"

	rbacv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientkubernetes "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/component-base/metrics/legacyregistry"
)

const leaseName = "pvecsictl-migrator"

type controllerCmd struct {
	pclient   *pxpool.ProxmoxPool
	kclient   *clientkubernetes.Clientset
	namespace string

	// primaryStorage maps region -> Proxmox node -> storage ID, from the
	// cloud config's per-cluster primary_storage maps.
	primaryStorage map[string]map[string]string
}

func buildControllerCmd() *cobra.Command {
	c := &controllerCmd{}

	cmd := cobra.Command{
		Use:           "controller",
		Aliases:       []string{"c"},
		Short:         "Run the annotation-driven volume migration controller",
		Args:          cobra.ExactArgs(0),
		PreRunE:       c.controllerValidate,
		RunE:          c.runController,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	setControllerCmdFlags(&cmd)

	return &cmd
}

func setControllerCmdFlags(cmd *cobra.Command) {
	flags := cmd.Flags()

	flags.StringP("namespace", "n", "", "namespace for the leader election lease (default: POD_NAMESPACE or kube-system)")

	flags.Bool("leader-election", true, "enable leader election")
	flags.Bool("pod-follow", false, "migrate volumes automatically when all pods mounting them have moved to another zone")
	flags.Bool("reactive-evacuation", false, "migrate a volume when its pod is unschedulable because the volume is pinned to a cordoned/tainted node (makes kubectl drain transparent)")
	flags.Duration("reactive-evacuation-grace", migrationcontroller.DefaultReactiveEvacuationGrace, "how long a pod must stay unschedulable before reactive evacuation triggers")
	flags.Int("max-attempts", 5, "maximum migration attempts per PVC before giving up")
	flags.Int("helper-vmid", migrator.DefaultHelperVMID, "VM ID of the transient helper VM used to convert qcow2/vmdk volumes (must differ from the controller VMID)")
	flags.Int("timeout", 10800, "Proxmox move-task timeout in seconds")
	flags.Duration("drain-timeout", 10*time.Minute, "maximum time to wait for pods to terminate during force-drain")
	flags.Duration("detach-timeout", 5*time.Minute, "maximum time to wait for a disk to detach from a VM")
	flags.String("metrics-address", "", "TCP address for prometheus metrics (e.g. :8080), disabled if empty")
}

func (c *controllerCmd) runController(cmd *cobra.Command, _ []string) error {
	flags := cmd.Flags()

	leaderElection, _ := flags.GetBool("leader-election")              //nolint: errcheck
	podFollow, _ := flags.GetBool("pod-follow")                        //nolint: errcheck
	reactiveEvacuation, _ := flags.GetBool("reactive-evacuation")      //nolint: errcheck
	reactiveGrace, _ := flags.GetDuration("reactive-evacuation-grace") //nolint: errcheck
	tokenCopyEndpoint, _ := flags.GetBool(flagTokenCopyEndpoint)       //nolint: errcheck
	proxmodEndpoint, _ := flags.GetBool(flagProxmodEndpoint)           //nolint: errcheck
	maxAttempts, _ := flags.GetInt("max-attempts")                     //nolint: errcheck
	helperVMID, _ := flags.GetInt("helper-vmid")                       //nolint: errcheck
	taskTimeout, _ := flags.GetInt("timeout")                          //nolint: errcheck
	drainTimeout, _ := flags.GetDuration("drain-timeout")              //nolint: errcheck
	detachTimeout, _ := flags.GetDuration("detach-timeout")            //nolint: errcheck
	metricsAddress, _ := flags.GetString("metrics-address")            //nolint: errcheck

	ctx := cmd.Context()

	if metricsAddress != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", legacyregistry.Handler())

		go func() {
			logger.Infof("metrics listening on %s", metricsAddress)

			if err := http.ListenAndServe(metricsAddress, mux); err != nil { //nolint: gosec
				logger.Errorf("failed to start metrics server: %v", err)
			}
		}()
	}

	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: c.kclient.CoreV1().Events("")})

	recorder := broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "proxmox-csi-migrator"})

	m := &migrator.Migrator{
		KClient:           c.kclient,
		PClient:           c.pclient,
		Recorder:          recorder,
		Logger:            logger,
		HelperVMID:        helperVMID,
		TokenCopyEndpoint: tokenCopyEndpoint,
		ProxmodEndpoint:   proxmodEndpoint,
	}

	controller := migrationcontroller.New(c.kclient, m, recorder, migrationcontroller.Options{
		MaxAttempts:             maxAttempts,
		TaskTimeout:             taskTimeout,
		DrainTimeout:            drainTimeout,
		DetachTimeout:           detachTimeout,
		PodFollow:               podFollow,
		PrimaryStorage:          c.primaryStorage,
		ReactiveEvacuation:      reactiveEvacuation,
		ReactiveEvacuationGrace: reactiveGrace,
	})

	if !leaderElection {
		controller.Run(ctx)

		return nil
	}

	id, err := os.Hostname()
	if err != nil {
		id = "pvecsictl-migrator"
	}

	id = fmt.Sprintf("%s_%s", id, uuid.NewString())

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: c.leaseNamespace(),
		},
		Client: c.kclient.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity:      id,
			EventRecorder: recorder,
		},
	}

	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				logger.Infof("became leader: %s", id)
				controller.Run(ctx)
			},
			OnStoppedLeading: func() {
				logger.Errorf("leader election lost: %s", id)
				os.Exit(1)
			},
		},
	})

	return nil
}

func (c *controllerCmd) leaseNamespace() string {
	if c.namespace != "" {
		return c.namespace
	}

	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}

	return "kube-system"
}

// nolint: dupl
func (c *controllerCmd) controllerValidate(cmd *cobra.Command, _ []string) error {
	flags := cmd.Flags()

	cfg, err := csiconfig.ReadCloudConfigFromFile(cloudconfig)
	if err != nil {
		return fmt.Errorf("failed to read config: %v", err)
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

	if err := pxpool.ResolveTokenRefs(context.TODO(), c.kclient, namespace, cfg.Clusters); err != nil {
		return fmt.Errorf("failed to resolve token refs: %v", err)
	}

	c.primaryStorage = map[string]map[string]string{}

	tokenCopyEndpoint, _ := flags.GetBool(flagTokenCopyEndpoint) //nolint: errcheck
	proxmodEndpoint, _ := flags.GetBool(flagProxmodEndpoint)     //nolint: errcheck

	if err := requireMigrationCredentials(cfg.Clusters, tokenCopyEndpoint, proxmodEndpoint); err != nil {
		return err
	}

	for _, cl := range cfg.Clusters {
		if len(cl.PrimaryStorage) > 0 {
			c.primaryStorage[cl.Region] = cl.PrimaryStorage
		}
	}

	c.pclient, err = pxpool.NewProxmoxPool(cfg.Clusters)
	if err != nil {
		return fmt.Errorf("failed to create Proxmox cluster client: %v", err)
	}

	if err = c.pclient.CheckClusters(context.TODO()); err != nil {
		return fmt.Errorf("failed to initialize Proxmox clusters: %v", err)
	}

	accessCheck := []rbacv1.ResourceAttributes{
		{Group: "", Namespace: "", Resource: "persistentvolumeclaims", Verb: "create"},
		{Group: "", Namespace: "", Resource: "persistentvolumeclaims", Verb: "delete"},
		{Group: "", Namespace: "", Resource: "persistentvolumes", Verb: "create"},
		{Group: "", Namespace: "", Resource: "persistentvolumes", Verb: "delete"},
		{Group: "", Namespace: "", Resource: "pods", Verb: "delete"},
		{Group: "", Namespace: "", Resource: "nodes", Verb: "get"},
		{Group: "", Namespace: "", Resource: "nodes", Verb: "patch"},
		{Group: "", Namespace: "", Resource: "events", Verb: "create"},
	}

	return checkPermissions(context.TODO(), c.kclient, accessCheck)
}
