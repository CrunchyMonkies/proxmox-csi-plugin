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

package proxmoxpool

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testClient wraps the transport the way newAPIHTTPClient does, but with a
// backoff short enough not to slow the suite down.
func testClient(attempts int) *http.Client {
	return &http.Client{
		Transport: &apiTransport{attempts: attempts, backoff: time.Millisecond},
	}
}

func TestAPITransportRetriesGET(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32

	// Two empty-bodied 596s — what pveproxy answers with when it cannot reach
	// pvedaemon — then the real response.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) <= 2 {
			w.WriteHeader(596)

			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"name":"pve-1"}}`)) //nolint:errcheck
	}))
	defer srv.Close()

	res, err := testClient(3).Get(srv.URL)
	require.NoError(t, err)

	defer res.Body.Close() //nolint:errcheck

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, res.StatusCode)
	assert.JSONEq(t, `{"data":{"name":"pve-1"}}`, string(body))
	assert.Equal(t, int32(3), hits.Load())
}

func TestAPITransportRetriesEmptySuccess(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32

	// A 200 with nothing in it is the shape the client library reports as
	// "unexpected end of JSON input".
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	res, err := testClient(3).Get(srv.URL)
	require.NoError(t, err)

	defer res.Body.Close() //nolint:errcheck

	assert.Equal(t, http.StatusOK, res.StatusCode)
	assert.Equal(t, int32(3), hits.Load(), "an empty 200 should be retried")
}

func TestAPITransportDoesNotRetryWrites(t *testing.T) {
	t.Parallel()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()

			var hits atomic.Int32

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer srv.Close()

			req, err := http.NewRequestWithContext(context.Background(), method, srv.URL, nil)
			require.NoError(t, err)

			res, err := testClient(3).Do(req)
			require.NoError(t, err)

			defer res.Body.Close() //nolint:errcheck

			// A replayed rename or copy would act on a volume twice.
			assert.Equal(t, int32(1), hits.Load(), "%s must never be replayed", method)
		})
	}
}

func TestAPITransportSurfacesSwallowedStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		msg        string
		status     int
		body       string
		expected   int
		statusText string
	}{
		{
			msg: "empty body carries the status into the error the library builds",
			// handleResponse maps only 500 and 501 to errors.New(res.Status); 596
			// would otherwise be unmarshalled as JSON and lost.
			status:     596,
			body:       "",
			expected:   http.StatusInternalServerError,
			statusText: "596",
		},
		{
			msg:        "an HTML error page is not JSON either",
			status:     http.StatusBadGateway,
			body:       "<html><body>502 Bad Gateway</body></html>",
			expected:   http.StatusInternalServerError,
			statusText: "502",
		},
		{
			msg:        "a real error payload is left for the library to report",
			status:     http.StatusForbidden,
			body:       `{"data":null,"errors":{"perm":"Permission check failed"}}`,
			expected:   http.StatusForbidden,
			statusText: "403",
		},
		{
			msg:        "500 is already handled by the library",
			status:     http.StatusInternalServerError,
			body:       "",
			expected:   http.StatusInternalServerError,
			statusText: "500",
		},
		{
			msg:        "400 is already handled by the library",
			status:     http.StatusBadRequest,
			body:       `{"errors":{"vmid":"invalid"}}`,
			expected:   http.StatusBadRequest,
			statusText: "400",
		},
	}

	for _, tt := range tests {
		t.Run(tt.msg, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body)) //nolint:errcheck
			}))
			defer srv.Close()

			// One attempt: this is about what the caller is told, not about retrying.
			res, err := testClient(1).Get(srv.URL)
			require.NoError(t, err)

			defer res.Body.Close() //nolint:errcheck

			body, err := io.ReadAll(res.Body)
			require.NoError(t, err)

			assert.Equal(t, tt.expected, res.StatusCode)
			assert.Contains(t, res.Status, tt.statusText, "the status line must keep the real code")
			assert.Equal(t, tt.body, string(body), "the body must still be readable")
		})
	}
}

func TestAPITransportHonorsContext(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	require.NoError(t, err)

	// A backoff far longer than the deadline: the caller's deadline wins, and it
	// wins during the wait rather than after another pointless request.
	client := &http.Client{Transport: &apiTransport{attempts: 5, backoff: 10 * time.Second}}

	res, err := client.Do(req) //nolint:bodyclose // there is no response: the context expired
	require.Nil(t, res)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, int32(1), hits.Load())
}

func TestAPITransportRetriesTransportErrors(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) <= 1 {
			// Hijack and close without writing a response: an EOF at the client,
			// which is what the live controller logged twice.
			conn, _, err := w.(http.Hijacker).Hijack()
			require.NoError(t, err)

			_ = conn.Close() //nolint:errcheck

			return
		}

		_, _ = w.Write([]byte(`{"data":[]}`)) //nolint:errcheck
	}))
	defer srv.Close()

	res, err := testClient(3).Get(srv.URL)
	require.NoError(t, err)

	defer res.Body.Close() //nolint:errcheck

	assert.Equal(t, http.StatusOK, res.StatusCode)
	assert.Equal(t, int32(2), hits.Load())
}

func TestNewAPIHTTPClient(t *testing.T) {
	t.Parallel()

	secure := newAPIHTTPClient(false)
	require.IsType(t, &apiTransport{}, secure.Transport)
	assert.Equal(t, apiRequestTimeout, secure.Timeout)

	// Nil, so that RoundTrip resolves http.DefaultTransport per request. The unit
	// suite swaps it with httpmock after the pool has already been built.
	assert.Nil(t, secure.Transport.(*apiTransport).base) //nolint:forcetypeassert

	insecure := newAPIHTTPClient(true)
	base, ok := insecure.Transport.(*apiTransport).base.(*http.Transport) //nolint:forcetypeassert
	require.True(t, ok)
	assert.True(t, base.TLSClientConfig.InsecureSkipVerify)
	assert.Equal(t, apiMaxIdleConnsPerHost, base.MaxIdleConnsPerHost)
}
