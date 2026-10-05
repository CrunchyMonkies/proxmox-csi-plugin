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
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	ctrl "sigs.k8s.io/controller-runtime"
)

// The volume API runs inside the operator process, so a panic in one handler
// would otherwise take the reconcilers and every tenant's API down with it.
// These turn it into an Internal error for that one call, and log the stack.

// RecoverUnaryInterceptor recovers a panicking unary handler.
func RecoverUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = recovered(info.FullMethod, r)
			}
		}()

		return handler(ctx, req)
	}
}

// RecoverStreamInterceptor recovers a panicking stream handler.
func RecoverStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = recovered(info.FullMethod, r)
			}
		}()

		return handler(srv, ss)
	}
}

func recovered(method string, r interface{}) error {
	ctrl.Log.WithName("volume-api").Error(nil, "handler panicked", "method", method, "panic", r, "stack", string(debug.Stack()))

	return status.Error(codes.Internal, "internal error")
}
