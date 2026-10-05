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

package api_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/sergelogvinov/proxmox-csi-plugin/pkg/apis/csi/v1alpha1"
	volumeapi "github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator/api"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// fakeOIDCServer serves OIDC discovery + JWKS for testing.
type fakeOIDCServer struct {
	server   *httptest.Server
	key      *rsa.PrivateKey
	requests atomic.Int64
}

func newFakeOIDCServer(t *testing.T) *fakeOIDCServer {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	f := &fakeOIDCServer{key: key}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		f.requests.Add(1)

		discovery := map[string]interface{}{
			"issuer":                                f.server.URL,
			"jwks_uri":                              f.server.URL + "/keys",
			"id_token_signing_alg_values_supported": []string{"RS256"},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(discovery) //nolint:errcheck
	})

	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		f.requests.Add(1)

		jwk := jose.JSONWebKey{
			Key:       &key.PublicKey,
			KeyID:     "test-key",
			Algorithm: string(jose.RS256),
			Use:       "sig",
		}

		jwks := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{jwk}}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(jwks) //nolint:errcheck
	})

	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)

	return f
}

func (f *fakeOIDCServer) issuer() string {
	return f.server.URL
}

func (f *fakeOIDCServer) signToken(t *testing.T, claims interface{}) string {
	t.Helper()

	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.RS256, Key: f.key},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "test-key"),
	)
	require.NoError(t, err)

	raw, err := jwt.Signed(signer).Claims(claims).Serialize()
	require.NoError(t, err)

	return raw
}

func (f *fakeOIDCServer) signTokenWithKey(t *testing.T, key *rsa.PrivateKey, claims interface{}) string {
	t.Helper()

	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.RS256, Key: key},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "wrong-key"),
	)
	require.NoError(t, err)

	raw, err := jwt.Signed(signer).Claims(claims).Serialize()
	require.NoError(t, err)

	return raw
}

type testClaims struct {
	Issuer   string           `json:"iss"`
	Subject  string           `json:"sub,omitempty"`
	Audience jwt.Audience     `json:"aud"`
	AZP      string           `json:"azp"`
	Expiry   *jwt.NumericDate `json:"exp"`
	IssuedAt *jwt.NumericDate `json:"iat"`
}

func validClaims(issuer string) testClaims {
	return testClaims{
		Issuer:   issuer,
		Subject:  "test",
		Audience: jwt.Audience{"test-client"},
		AZP:      "test-client",
		Expiry:   jwt.NewNumericDate(time.Now().Add(time.Hour)),
		IssuedAt: jwt.NewNumericDate(time.Now()),
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	return scheme
}

func admittedTenantWithIssuer(issuer string) *v1alpha1.TenantCluster {
	return &v1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-tenant"},
		Spec: v1alpha1.TenantClusterSpec{
			Namespace:       "tenant-ns",
			Region:          "bne",
			Subject:         "pvx:test-client",
			Issuer:          issuer,
			PlaceholderVMID: 9997,
			AllowedStorages: []string{"local"},
			Mode:            v1alpha1.TenantModeEnforce,
		},
		Status: v1alpha1.TenantClusterStatus{
			ResolvedVMIDs: []int32{411, 421, 422, 423},
			Conditions: []metav1.Condition{{
				Type:               v1alpha1.ConditionAdmitted,
				Status:             metav1.ConditionTrue,
				Reason:             v1alpha1.ReasonAccepted,
				LastTransitionTime: metav1.Now(),
			}},
		},
	}
}

func ctxWithBearer(token string) context.Context {
	md := metadata.Pairs("authorization", "Bearer "+token)

	return metadata.NewIncomingContext(context.Background(), md)
}

// noopHandler is a passthrough gRPC handler for testing interceptors.
func noopHandler(ctx context.Context, _ interface{}) (interface{}, error) {
	return volumeapi.TenantFromContext(ctx), nil
}

func TestValidTokenResolvesTenant(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	tenant := admittedTenantWithIssuer(oidc.issuer())

	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(tenant).Build()
	authn := volumeapi.NewAuthenticator(c)

	token := oidc.signToken(t, validClaims(oidc.issuer()))
	ctx := ctxWithBearer(token)

	interceptor := authn.UnaryInterceptor()
	resp, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, noopHandler)

	require.NoError(t, err)

	resolved, ok := resp.(*v1alpha1.TenantCluster)
	require.True(t, ok)
	assert.Equal(t, "test-tenant", resolved.Name)
	assert.Equal(t, "tenant-ns", resolved.Spec.Namespace,
		"namespace comes ONLY from the tenant, never from the token")
}

func TestWrongAZPNoTenant(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	tenant := admittedTenantWithIssuer(oidc.issuer())

	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(tenant).Build()
	authn := volumeapi.NewAuthenticator(c)

	claims := validClaims(oidc.issuer())
	claims.AZP = "wrong-client"
	claims.Audience = jwt.Audience{"wrong-client"}

	token := oidc.signToken(t, claims)
	ctx := ctxWithBearer(token)

	_, err := authn.UnaryInterceptor()(ctx, nil, &grpc.UnaryServerInfo{}, noopHandler)
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestUnknownIssuerNoDiscoveryFetch(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	// Tenant uses the real issuer, but the token claims a different one.
	tenant := admittedTenantWithIssuer(oidc.issuer())

	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(tenant).Build()
	authn := volumeapi.NewAuthenticator(c)

	claims := validClaims("https://unknown-issuer.example.com")
	token := oidc.signToken(t, claims)
	ctx := ctxWithBearer(token)

	requestsBefore := oidc.requests.Load()

	_, err := authn.UnaryInterceptor()(ctx, nil, &grpc.UnaryServerInfo{}, noopHandler)
	require.Error(t, err)
	// Should be PermissionDenied (no tenant matches), not Unauthenticated (which
	// would imply we tried to verify the token).
	assert.Equal(t, codes.PermissionDenied, status.Code(err))

	// The httptest server must NOT have received any requests for the unknown issuer.
	assert.Equal(t, requestsBefore, oidc.requests.Load(),
		"no discovery fetch for an issuer not in any TenantCluster")
}

func TestAudMissingAZP(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	tenant := admittedTenantWithIssuer(oidc.issuer())

	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(tenant).Build()
	authn := volumeapi.NewAuthenticator(c)

	claims := validClaims(oidc.issuer())
	claims.Audience = jwt.Audience{"some-other-audience"} // aud does not contain azp

	token := oidc.signToken(t, claims)
	ctx := ctxWithBearer(token)

	_, err := authn.UnaryInterceptor()(ctx, nil, &grpc.UnaryServerInfo{}, noopHandler)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.Contains(t, err.Error(), "aud must contain azp")
}

func TestExpiredToken(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	tenant := admittedTenantWithIssuer(oidc.issuer())

	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(tenant).Build()
	authn := volumeapi.NewAuthenticator(c)

	claims := validClaims(oidc.issuer())
	claims.Expiry = jwt.NewNumericDate(time.Now().Add(-time.Hour))

	token := oidc.signToken(t, claims)
	ctx := ctxWithBearer(token)

	_, err := authn.UnaryInterceptor()(ctx, nil, &grpc.UnaryServerInfo{}, noopHandler)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.Contains(t, err.Error(), "token verification failed")
}

func TestBadSignature(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	tenant := admittedTenantWithIssuer(oidc.issuer())

	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(tenant).Build()
	authn := volumeapi.NewAuthenticator(c)

	// Sign with a different key but claiming the same issuer.
	wrongKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	token := oidc.signTokenWithKey(t, wrongKey, validClaims(oidc.issuer()))
	ctx := ctxWithBearer(token)

	_, err = authn.UnaryInterceptor()(ctx, nil, &grpc.UnaryServerInfo{}, noopHandler)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestUnadmittedTenant(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	tenant := admittedTenantWithIssuer(oidc.issuer())
	// Mark unadmitted.
	tenant.Status.Conditions[0].Status = metav1.ConditionFalse

	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(tenant).Build()
	authn := volumeapi.NewAuthenticator(c)

	token := oidc.signToken(t, validClaims(oidc.issuer()))
	ctx := ctxWithBearer(token)

	_, err := authn.UnaryInterceptor()(ctx, nil, &grpc.UnaryServerInfo{}, noopHandler)
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
	assert.Contains(t, err.Error(), "not admitted")
}

func TestTwoTenantsMatching(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	tenant1 := admittedTenantWithIssuer(oidc.issuer())
	tenant2 := admittedTenantWithIssuer(oidc.issuer())
	tenant2.Name = "test-tenant-2"
	tenant2.Spec.Namespace = "tenant-ns-2"

	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(tenant1, tenant2).Build()
	authn := volumeapi.NewAuthenticator(c)

	token := oidc.signToken(t, validClaims(oidc.issuer()))
	ctx := ctxWithBearer(token)

	_, err := authn.UnaryInterceptor()(ctx, nil, &grpc.UnaryServerInfo{}, noopHandler)
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
	assert.Contains(t, err.Error(), "ambiguous")
}

func TestMissingAuthorizationHeader(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	authn := volumeapi.NewAuthenticator(c)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs())

	_, err := authn.UnaryInterceptor()(ctx, nil, &grpc.UnaryServerInfo{}, noopHandler)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestMissingMetadata(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	authn := volumeapi.NewAuthenticator(c)

	_, err := authn.UnaryInterceptor()(context.Background(), nil, &grpc.UnaryServerInfo{}, noopHandler)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestMalformedAuthorizationHeader(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	authn := volumeapi.NewAuthenticator(c)

	md := metadata.Pairs("authorization", "Basic dXNlcjpwYXNz")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	_, err := authn.UnaryInterceptor()(ctx, nil, &grpc.UnaryServerInfo{}, noopHandler)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestNoAZPClaim(t *testing.T) {
	oidc := newFakeOIDCServer(t)

	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	authn := volumeapi.NewAuthenticator(c)

	claims := map[string]interface{}{
		"iss": oidc.issuer(),
		"aud": []string{"test"},
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
		// no azp
	}

	token := oidc.signToken(t, claims)
	ctx := ctxWithBearer(token)

	_, err := authn.UnaryInterceptor()(ctx, nil, &grpc.UnaryServerInfo{}, noopHandler)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.Contains(t, err.Error(), "no azp")
}

// fakeStreamForTest implements grpc.ServerStream for stream interceptor testing.
type fakeStreamForTest struct {
	grpc.ServerStream

	ctx context.Context //nolint:containedctx // Required to implement grpc.ServerStream.
}

func (f *fakeStreamForTest) Context() context.Context { return f.ctx }

func TestStreamInterceptorEnforcesAuthn(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	tenant := admittedTenantWithIssuer(oidc.issuer())

	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(tenant).Build()
	authn := volumeapi.NewAuthenticator(c)

	// Without a token: should fail.
	noTokenCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs())
	stream := &fakeStreamForTest{ctx: noTokenCtx}

	err := authn.StreamInterceptor()(nil, stream, &grpc.StreamServerInfo{},
		func(_ interface{}, _ grpc.ServerStream) error {
			t.Fatal("handler should not be called without auth")

			return nil
		})

	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))

	// With a valid token: should succeed and pass tenant in context.
	token := oidc.signToken(t, validClaims(oidc.issuer()))
	validCtx := ctxWithBearer(token)
	validStream := &fakeStreamForTest{ctx: validCtx}

	var (
		resolvedTenant string
		gotSrv         interface{}
	)

	// The service implementation must reach the handler untouched: the generated
	// stream handlers type-assert it, and a nil there panics the whole operator.
	srv := &struct{ name string }{name: "volume-service"}

	err = authn.StreamInterceptor()(srv, validStream, &grpc.StreamServerInfo{},
		func(s interface{}, ss grpc.ServerStream) error {
			gotSrv = s

			resolved := volumeapi.TenantFromContext(ss.Context())
			if resolved != nil {
				resolvedTenant = resolved.Name
			}

			return nil
		})

	require.NoError(t, err)
	assert.Equal(t, "test-tenant", resolvedTenant)
	assert.Same(t, srv, gotSrv, "stream interceptor must pass the service through")
}

func TestAlgNoneReturnsUnauthenticated(t *testing.T) {
	// A raw JWT with alg:none. jose refuses to produce one, so this is built by hand.
	// The claims use the known issuer but with no signature.
	oidcSrv := newFakeOIDCServer(t)
	tenant := admittedTenantWithIssuer(oidcSrv.issuer())

	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(tenant).Build()
	authn := volumeapi.NewAuthenticator(c)

	// Build the payload with the actual issuer URL.
	claims := fmt.Sprintf(
		`{"iss":"%s","azp":"test-client","aud":["test-client"],"exp":9999999999}`,
		oidcSrv.issuer(),
	)

	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(claims))

	token := header + "." + payload + "."

	ctx := ctxWithBearer(token)

	_, err := authn.UnaryInterceptor()(ctx, nil, &grpc.UnaryServerInfo{}, noopHandler)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestTamperedPayloadReturnsUnauthenticated(t *testing.T) {
	oidc := newFakeOIDCServer(t)
	tenant := admittedTenantWithIssuer(oidc.issuer())

	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(tenant).Build()
	authn := volumeapi.NewAuthenticator(c)

	token := oidc.signToken(t, validClaims(oidc.issuer()))

	// Tamper with the payload by flipping a character.
	parts := strings.SplitN(token, ".", 3)
	require.Len(t, parts, 3)

	runes := []rune(parts[1])
	if len(runes) > 5 {
		if runes[5] == 'a' {
			runes[5] = 'b'
		} else {
			runes[5] = 'a'
		}
	}

	parts[1] = string(runes)
	tampered := strings.Join(parts, ".")

	ctx := ctxWithBearer(tampered)

	_, err := authn.UnaryInterceptor()(ctx, nil, &grpc.UnaryServerInfo{}, noopHandler)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestUnknownIssuerWithSecondServerReceivesZeroRequests(t *testing.T) {
	// This test uses a SEPARATE httptest server for the unknown issuer, and
	// verifies it receives exactly zero requests.
	oidc := newFakeOIDCServer(t)
	tenant := admittedTenantWithIssuer(oidc.issuer())

	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(tenant).Build()
	authn := volumeapi.NewAuthenticator(c)

	// A second OIDC server that should never be contacted.
	unknownOIDC := newFakeOIDCServer(t)

	claims := testClaims{
		Issuer:   unknownOIDC.issuer(),
		Subject:  "test",
		Audience: jwt.Audience{"test-client"},
		AZP:      "test-client",
		Expiry:   jwt.NewNumericDate(time.Now().Add(time.Hour)),
		IssuedAt: jwt.NewNumericDate(time.Now()),
	}

	token := unknownOIDC.signToken(t, claims)
	unknownOIDC.requests.Store(0)

	ctx := ctxWithBearer(token)

	_, err := authn.UnaryInterceptor()(ctx, nil, &grpc.UnaryServerInfo{}, noopHandler)
	require.Error(t, err)

	assert.Zero(t, unknownOIDC.requests.Load(),
		"the second httptest server for the unknown issuer must receive zero requests")
}

func TestNamespaceCannotBeChosenByRequest(t *testing.T) {
	// This is a structural test: the namespace is derived entirely from the
	// tenant's spec, never from any field in the request. TenantFromContext
	// returns the full TenantCluster, and every service method reads
	// tenant.Spec.Namespace. There is no namespace field in the proto.
	oidc := newFakeOIDCServer(t)
	tenant := admittedTenantWithIssuer(oidc.issuer())

	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(tenant).Build()
	authn := volumeapi.NewAuthenticator(c)

	token := oidc.signToken(t, validClaims(oidc.issuer()))
	ctx := ctxWithBearer(token)

	resp, err := authn.UnaryInterceptor()(ctx, nil, &grpc.UnaryServerInfo{}, noopHandler)
	require.NoError(t, err)

	resolved := resp.(*v1alpha1.TenantCluster)
	assert.Equal(t, "tenant-ns", resolved.Spec.Namespace,
		fmt.Sprintf("namespace is always %q from the tenant, never from the request", "tenant-ns"))
}
