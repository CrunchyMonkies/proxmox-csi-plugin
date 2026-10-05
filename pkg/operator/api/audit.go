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

	"github.com/google/uuid"
	"google.golang.org/grpc"
	grpcmetadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type requestIDKey struct{}

// RequestIDFromContext returns the request ID set by the audit interceptor.
func RequestIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey{}).(string); ok {
		return id
	}

	return ""
}

// AuditUnaryInterceptor logs every unary RPC with its outcome.
// Must be chained after authn so the tenant is available.
func AuditUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		requestID := uuid.New().String()
		ctx = context.WithValue(ctx, requestIDKey{}, requestID)

		// Return the request ID in a response header.
		_ = grpc.SetHeader(ctx, grpcmetadata.Pairs("x-request-id", requestID)) //nolint:errcheck

		resp, err := handler(ctx, req)

		code := status.Code(err)
		logger := log.FromContext(ctx)

		tenant := TenantFromContext(ctx)
		tenantName := ""
		azp := ""

		if tenant != nil {
			tenantName = tenant.Name
			azp = tenant.Spec.Subject
		}

		logger.Info("volume-api",
			"method", info.FullMethod,
			"requestId", requestID,
			"tenant", tenantName,
			"azp", azp,
			"code", code.String(),
		)

		return resp, err
	}
}

// AuditStreamInterceptor logs every stream RPC with its outcome.
func AuditStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		requestID := uuid.New().String()
		ctx := context.WithValue(ss.Context(), requestIDKey{}, requestID)

		_ = ss.SetHeader(grpcmetadata.Pairs("x-request-id", requestID)) //nolint:errcheck

		err := handler(srv, &wrappedStream{ServerStream: ss, ctx: ctx})

		code := status.Code(err)
		logger := log.FromContext(ctx)

		tenant := TenantFromContext(ctx)
		tenantName := ""
		azp := ""

		if tenant != nil {
			tenantName = tenant.Name
			azp = tenant.Spec.Subject
		}

		logger.Info("volume-api",
			"method", info.FullMethod,
			"requestId", requestID,
			"tenant", tenantName,
			"azp", azp,
			"code", code.String(),
		)

		return err
	}
}
