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

// Proxmox CSI Plugin Controller
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"

	proto "github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/oauth2/clientcredentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi/remote"
	tools "github.com/sergelogvinov/proxmox-csi-plugin/pkg/tools/kubernetes"

	clientkubernetes "k8s.io/client-go/kubernetes"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"
)

var (
	version string
	commit  string

	showVersion = flag.Bool("version", false, "Print the version and exit.")
	csiEndpoint = flag.String("csi-address", "unix:///csi/csi.sock", "CSI Endpoint")

	metricsAddress = flag.String("metrics-address", "", "The TCP network address where the HTTP server for metrics, will listen (example: `:8080`). By default the server is disabled.")
	metricsPath    = flag.String("metrics-path", "/metrics", "The HTTP path where prometheus metrics will be exposed.")

	cloudconfig = flag.String("cloud-config", "", "The path to the CSI driver cloud config.")
	kubeconfig  = flag.String("kubeconfig", "", "Absolute path to the kubeconfig file. Either this or master needs to be set if the provisioner is being run out of cluster.")

	annotateNodeInstanceID = flag.Bool("annotate-node-instance-id", false,
		"Write the resolved Proxmox VMID back to the node as the "+csi.AnnotationProxmoxInstanceID+" annotation, so later lookups skip the cluster scan. "+
			"Only has an effect where the providerID carries no VMID, and requires patch on nodes.")

	remoteVolumeAPI = flag.String("remote-volume-api", "",
		"Address of the operator volume API (host:port). Enables remote mode.")
	remoteTokenURL = flag.String("remote-token-url", "",
		"OAuth2 token endpoint URL for remote mode.")
	remoteClientIDFile = flag.String("remote-client-id-file", "",
		"Path to a file containing the OAuth2 client ID.")
	remoteClientSecretFile = flag.String("remote-client-secret-file", "",
		"Path to a file containing the OAuth2 client secret.")
	remoteCAFile = flag.String("remote-ca-file", "",
		"Path to a CA certificate file for the remote volume API (optional).")
)

func main() {
	klog.InitFlags(nil)
	flag.Set("logtostderr", "true") //nolint: errcheck
	flag.Parse()

	klog.V(2).InfoS("Version", "version", csi.DriverVersion, "csiVersion", csi.DriverSpecVersion, "gitVersion", version, "gitCommit", commit)

	if *showVersion {
		klog.Infof("Driver version %v, GitVersion %s", csi.DriverVersion, version)
		os.Exit(0)
	}

	if *csiEndpoint == "" {
		klog.Error("csi-address must be provided")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	if *remoteVolumeAPI != "" && *cloudconfig != "" {
		klog.Error("--remote-volume-api and --cloud-config are mutually exclusive")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	if *remoteVolumeAPI == "" && *cloudconfig == "" {
		klog.Error("either --cloud-config or --remote-volume-api must be provided")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	scheme, addr, err := csi.ParseEndpoint(*csiEndpoint)
	if err != nil {
		klog.Error(err, "Failed to parse endpoint")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	listener, err := net.Listen(scheme, addr)
	if err != nil {
		klog.ErrorS(err, "Failed to listen", "address", *csiEndpoint)
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	logErr := func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, rpcerr := handler(ctx, req)
		if rpcerr != nil {
			klog.ErrorS(rpcerr, "GRPC error")
		}

		return resp, rpcerr
	}

	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(logErr),
	}

	// Prepare http endpoint for metrics
	mux := http.NewServeMux()
	if *metricsAddress != "" {
		mux.Handle("/metrics", legacyregistry.Handler())

		go func() {
			klog.V(2).InfoS("Metrics listening", "address", *metricsAddress, "metricsPath", *metricsPath)

			err := http.ListenAndServe(*metricsAddress, mux)
			if err != nil {
				klog.ErrorS(err, "Failed to start HTTP server at specified address and metrics path", "address", addr, "metricsPath", *metricsPath)
			}
		}()
	}

	srv := grpc.NewServer(opts...)
	identityService := csi.NewIdentityService()
	proto.RegisterIdentityServer(srv, identityService)

	if *remoteVolumeAPI != "" {
		// Remote mode: connect to the operator's volume API.
		conn, err := dialRemoteAPI(*remoteVolumeAPI, *remoteTokenURL, *remoteClientIDFile, *remoteClientSecretFile, *remoteCAFile)
		if err != nil {
			klog.ErrorS(err, "Failed to connect to remote volume API")
			klog.FlushAndExit(klog.ExitFlushTimeout, 1)
		}
		defer conn.Close() //nolint:errcheck // Best-effort cleanup on exit.

		controllerService := remote.NewControllerServer(conn)
		controllerService.StartCapacityWatch(context.Background())

		proto.RegisterControllerServer(srv, controllerService)
		klog.InfoS("Running in remote mode", "api", *remoteVolumeAPI)
	} else {
		// Direct mode: build the Proxmox client pool.
		kconfig, namespace, err := tools.BuildConfig(*kubeconfig, "")
		if err != nil {
			klog.Error(err, "Failed to build a Kubernetes config")
			klog.FlushAndExit(klog.ExitFlushTimeout, 1)
		}

		clientset, err := clientkubernetes.NewForConfig(kconfig)
		if err != nil {
			klog.Error(err, "Failed to create a Clientset")
			klog.FlushAndExit(klog.ExitFlushTimeout, 1)
		}

		controllerService, err := csi.NewControllerService(clientset, *cloudconfig, namespace)
		if err != nil {
			klog.ErrorS(err, "Failed to create controller service")
			klog.FlushAndExit(klog.ExitFlushTimeout, 1)
		}

		controllerService.AnnotateNodeInstanceID = *annotateNodeInstanceID

		proto.RegisterControllerServer(srv, controllerService)
	}

	klog.InfoS("Listening for connection on address", "address", listener.Addr())

	if err := srv.Serve(listener); err != nil {
		klog.ErrorS(err, "Failed to run driver")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}
}

// dialRemoteAPI establishes a gRPC connection to the operator's volume API
// with OAuth2 client credentials.
func dialRemoteAPI(address, tokenURL, clientIDFile, clientSecretFile, caFile string) (*grpc.ClientConn, error) {
	clientID, err := readFileContent(clientIDFile)
	if err != nil {
		return nil, fmt.Errorf("reading client ID file: %w", err)
	}

	clientSecret, err := readFileContent(clientSecretFile)
	if err != nil {
		return nil, fmt.Errorf("reading client secret file: %w", err)
	}

	ccConfig := clientcredentials.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     tokenURL,
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("reading CA file: %w", err)
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}

		tlsConfig.RootCAs = pool
	}

	tokenSource := ccConfig.TokenSource(context.Background())

	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		grpc.WithPerRPCCredentials(oauth.TokenSource{TokenSource: tokenSource}),
	)
	if err != nil {
		return nil, fmt.Errorf("dialing %s: %w", address, err)
	}

	return conn, nil
}

// readFileContent reads a file and returns its trimmed content.
func readFileContent(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("file path is empty")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(data)), nil
}
