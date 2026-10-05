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

	"google.golang.org/grpc"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	volumev1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/volume/v1"
)

// ContextWithTenantForTest is exported for tests in the api_test package.
func ContextWithTenantForTest(ctx context.Context, tenant *v1alpha1.TenantCluster) context.Context {
	return context.WithValue(ctx, tenantContextKey{}, tenant)
}

// SendCapacityForTest exposes sendCapacity for tests.
func SendCapacityForTest(ctx context.Context, s *Service, req *volumev1.WatchCapacityRequest, stream grpc.ServerStreamingServer[volumev1.WatchCapacityResponse]) error {
	tenant := TenantFromContext(ctx)
	if tenant == nil {
		return nil
	}

	return s.sendCapacity(ctx, tenant, req, stream)
}

// GRPCServerForTest exposes the configured gRPC server (interceptor chain and
// service registered) so a test can serve it over an in-memory listener.
func (s *Server) GRPCServerForTest() *grpc.Server {
	return s.grpc
}

// AttachmentNameForTest exposes the deterministic attachment name function.
func AttachmentNameForTest(volumeID, nodeID string) string {
	return attachmentName(volumeID, nodeID)
}
