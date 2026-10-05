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
	"sync"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RateLimiter enforces per-tenant token-bucket rate limiting.
type RateLimiter struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
	rate     rate.Limit
	burst    int
}

// NewRateLimiter creates a rate limiter with the given per-tenant rate and burst.
func NewRateLimiter(r float64, burst int) *RateLimiter {
	return &RateLimiter{
		limiters: make(map[string]*rate.Limiter),
		rate:     rate.Limit(r),
		burst:    burst,
	}
}

// UnaryInterceptor returns a gRPC unary interceptor that rate-limits per tenant.
// Must be chained after the authn interceptor so TenantFromContext is populated.
func (rl *RateLimiter) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		tenant := TenantFromContext(ctx)
		if tenant == nil {
			return handler(ctx, req)
		}

		if !rl.limiterFor(tenant.Name).Allow() {
			return nil, status.Errorf(codes.ResourceExhausted, "rate limit exceeded for tenant %s", tenant.Name)
		}

		return handler(ctx, req)
	}
}

func (rl *RateLimiter) limiterFor(tenantName string) *rate.Limiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	l, ok := rl.limiters[tenantName]
	if !ok {
		l = rate.NewLimiter(rl.rate, rl.burst)
		rl.limiters[tenantName] = l
	}

	return l
}
