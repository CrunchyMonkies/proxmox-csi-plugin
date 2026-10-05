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

package api

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	volumev1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/volume/v1"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/proxmox"
)

// Server is a controller-runtime Runnable that serves the volume gRPC API.
//
// It runs on every replica (NeedLeaderElection returns false), because the API
// is stateless — it reads and writes CRs, and the reconcilers that act on them
// run only on the leader.
type Server struct {
	address string
	service *Service
	authn   *Authenticator
	rateRL  *RateLimiter
	grpc    *grpc.Server
}

// ServerConfig holds the configuration for the gRPC server.
type ServerConfig struct {
	// Address is the TCP address to listen on, e.g. ":9090".
	Address string
	// Client is the management cluster client.
	Client client.Client
	// VMs reads Proxmox VM inventory for VMID resolution by name.
	VMs proxmox.VMReader
	// Rate is the per-tenant requests-per-second limit.
	Rate float64
	// Burst is the per-tenant burst limit.
	Burst int
}

// NewServer creates a new volume API gRPC server.
func NewServer(cfg ServerConfig) *Server {
	authn := NewAuthenticator(cfg.Client)
	rl := NewRateLimiter(cfg.Rate, cfg.Burst)
	service := NewService(cfg.Client, cfg.VMs)

	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			RecoverUnaryInterceptor(),
			authn.UnaryInterceptor(),
			AuditUnaryInterceptor(),
			rl.UnaryInterceptor(),
		),
		grpc.ChainStreamInterceptor(
			RecoverStreamInterceptor(),
			authn.StreamInterceptor(),
			AuditStreamInterceptor(),
		),
	)

	volumev1.RegisterVolumeServiceServer(srv, service)

	return &Server{
		address: cfg.Address,
		service: service,
		authn:   authn,
		rateRL:  rl,
		grpc:    srv,
	}
}

// NeedLeaderElection returns false: the API runs on every replica.
func (s *Server) NeedLeaderElection() bool {
	return false
}

// Start implements the controller-runtime Runnable interface.
func (s *Server) Start(ctx context.Context) error {
	logger := ctrl.Log.WithName("volume-api")

	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", s.address, err)
	}

	logger.Info("serving volume API", "address", s.address)

	// Shut down gracefully when the context is canceled.
	go func() {
		<-ctx.Done()
		logger.Info("shutting down volume API")
		s.grpc.GracefulStop()
	}()

	if err := s.grpc.Serve(listener); err != nil {
		return fmt.Errorf("serving gRPC: %w", err)
	}

	return nil
}

// SetupIndexes registers the field indexes the service needs.
func SetupIndexes(ctx context.Context, mgr ctrl.Manager) error {
	return IndexVolumeIDs(ctx, mgr.GetFieldIndexer())
}
