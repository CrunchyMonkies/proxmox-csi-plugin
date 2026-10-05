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

// Package api serves the gRPC volume API (ADR 6).
//
// The API authenticates callers via OIDC bearer tokens, resolves each caller to
// a TenantCluster, and maps volume operations to custom resources the operator
// reconciles. It runs on every replica (NeedLeaderElection returns false) and is
// stateless: it reads and writes CRs but never touches Proxmox.
package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/coreos/go-oidc/v3/oidc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
)

type tenantContextKey struct{}

// TenantFromContext returns the TenantCluster resolved by the authn interceptor.
func TenantFromContext(ctx context.Context) *v1alpha1.TenantCluster {
	if t, ok := ctx.Value(tenantContextKey{}).(*v1alpha1.TenantCluster); ok {
		return t
	}

	return nil
}

// jwtClaims are the claims we need from the token before verification.
type jwtClaims struct {
	Issuer   string   `json:"iss"`
	AZP      string   `json:"azp"`
	Audience audience `json:"aud"`
}

// audience handles both string and []string JSON representations.
type audience []string

func (a *audience) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*a = audience{s}

		return nil
	}

	var ss []string
	if err := json.Unmarshal(data, &ss); err != nil {
		return err
	}

	*a = audience(ss)

	return nil
}

// Authenticator verifies bearer tokens using OIDC discovery and resolves them
// to a TenantCluster.
type Authenticator struct {
	client client.Reader

	mu        sync.RWMutex
	verifiers map[string]*oidc.IDTokenVerifier
}

// NewAuthenticator creates a new OIDC-based authenticator.
func NewAuthenticator(c client.Reader) *Authenticator {
	return &Authenticator{
		client:    c,
		verifiers: make(map[string]*oidc.IDTokenVerifier),
	}
}

// UnaryInterceptor returns a gRPC unary interceptor that authenticates requests.
func (a *Authenticator) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		tenant, err := a.authenticate(ctx)
		if err != nil {
			return nil, err
		}

		ctx = context.WithValue(ctx, tenantContextKey{}, tenant)

		return handler(ctx, req)
	}
}

// StreamInterceptor returns a gRPC stream interceptor that authenticates requests.
func (a *Authenticator) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		tenant, err := a.authenticate(ss.Context())
		if err != nil {
			return err
		}

		ctx := context.WithValue(ss.Context(), tenantContextKey{}, tenant)

		return handler(srv, &wrappedStream{ServerStream: ss, ctx: ctx})
	}
}

type wrappedStream struct {
	grpc.ServerStream

	ctx context.Context //nolint:containedctx // Required to wrap a gRPC stream with a modified context.
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}

// authenticate extracts the bearer token, verifies it, and resolves the tenant.
func (a *Authenticator) authenticate(ctx context.Context) (*v1alpha1.TenantCluster, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}

	values := md.Get("authorization")
	if len(values) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing authorization header")
	}

	token := values[0]
	if !strings.HasPrefix(token, "Bearer ") {
		return nil, status.Error(codes.Unauthenticated, "authorization header must be Bearer")
	}

	rawToken := strings.TrimPrefix(token, "Bearer ")

	// Parse the JWT payload without verification to extract iss and azp.
	claims, err := parseUnverifiedClaims(rawToken)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}

	if claims.Issuer == "" {
		return nil, status.Error(codes.Unauthenticated, "token has no issuer")
	}

	if claims.AZP == "" {
		return nil, status.Error(codes.Unauthenticated, "token has no azp claim")
	}

	// Verify that aud contains azp.
	if !audienceContains(claims.Audience, claims.AZP) {
		return nil, status.Error(codes.Unauthenticated, "aud must contain azp")
	}

	// Look up the tenant BEFORE fetching the OIDC provider -- only issuers that
	// appear in some TenantCluster.spec.issuer are ever fetched.
	tenant, err := a.resolveTenant(ctx, claims.Issuer, claims.AZP)
	if err != nil {
		return nil, err
	}

	// Now verify the token signature using the tenant's issuer JWKS.
	verifier, err := a.getVerifier(ctx, claims.Issuer, claims.AZP)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "OIDC verification failed: %v", err)
	}

	if _, err := verifier.Verify(ctx, rawToken); err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "token verification failed: %v", err)
	}

	return tenant, nil
}

// parseUnverifiedClaims extracts the claims from a JWT without signature
// verification, used to determine the issuer before selecting a verifier.
func parseUnverifiedClaims(rawToken string) (*jwtClaims, error) {
	parts := strings.Split(rawToken, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("token must have 3 parts, got %d", len(parts))
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("decoding payload: %w", err)
	}

	var claims jwtClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("unmarshalling claims: %w", err)
	}

	return &claims, nil
}

// audienceContains checks whether the audience list contains the target.
func audienceContains(aud audience, target string) bool {
	for _, a := range aud {
		if a == target {
			return true
		}
	}

	return false
}

// resolveTenant finds exactly one TenantCluster matching the issuer and azp.
func (a *Authenticator) resolveTenant(ctx context.Context, issuer, azp string) (*v1alpha1.TenantCluster, error) {
	list := &v1alpha1.TenantClusterList{}
	if err := a.client.List(ctx, list); err != nil {
		return nil, status.Errorf(codes.Internal, "listing tenants: %v", err)
	}

	subject := "pvx:" + azp

	var matches []*v1alpha1.TenantCluster

	for i := range list.Items {
		tenant := &list.Items[i]

		if tenant.Spec.Issuer != issuer || tenant.Spec.Subject != subject {
			continue
		}

		matches = append(matches, tenant)
	}

	switch len(matches) {
	case 0:
		return nil, status.Errorf(codes.PermissionDenied, "no tenant matches issuer %s and subject %s", issuer, subject)
	case 1:
		// Exactly one match.
	default:
		return nil, status.Errorf(codes.PermissionDenied, "ambiguous: %d tenants match issuer %s and subject %s", len(matches), issuer, subject)
	}

	tenant := matches[0]

	if !apimeta.IsStatusConditionTrue(tenant.Status.Conditions, v1alpha1.ConditionAdmitted) {
		return nil, status.Errorf(codes.PermissionDenied, "tenant %s is not admitted", tenant.Name)
	}

	return tenant, nil
}

// getVerifier returns a cached OIDC verifier for the given issuer and audience.
func (a *Authenticator) getVerifier(ctx context.Context, issuer, azpAudience string) (*oidc.IDTokenVerifier, error) {
	key := fmt.Sprintf("%s|%s", issuer, azpAudience)

	a.mu.RLock()
	v, ok := a.verifiers[key]
	a.mu.RUnlock()

	if ok {
		return v, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Double-check after acquiring write lock.
	if v, ok := a.verifiers[key]; ok {
		return v, nil
	}

	provider, err := oidc.NewProvider(ctx, issuer)
	if err != nil {
		return nil, fmt.Errorf("OIDC discovery for %s: %w", issuer, err)
	}

	v = provider.Verifier(&oidc.Config{
		ClientID: azpAudience,
	})

	a.verifiers[key] = v

	return v, nil
}
