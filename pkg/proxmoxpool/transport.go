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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/metrics"
)

const (
	// apiRequestTimeout bounds a single Proxmox API call, retries included.
	//
	// Every long-running Proxmox operation the driver issues is asynchronous: the
	// content DELETE and the disk copy both return a UPID that is polled
	// separately. The one synchronous call, the proxmod rename, is a file rename
	// plus a config edit.
	apiRequestTimeout = 90 * time.Second

	// apiRetryAttempts is the total number of tries, not the number of retries.
	apiRetryAttempts = 3
	apiRetryBackoff  = 250 * time.Millisecond

	apiMaxIdleConns        = 64
	apiMaxIdleConnsPerHost = 32
	apiMaxConnsPerHost     = 64
	apiIdleConnTimeout     = 90 * time.Second
)

// apiTransport retries idempotent Proxmox API requests and stops the client
// library from swallowing HTTP status codes.
//
// Both behaviors exist because of the same live incident: a burst of
// ControllerModifyVolume calls provoked 33 "unexpected end of JSON input" and 2
// "EOF" errors from the Proxmox API in a fourteen-second window. Nothing was
// wrong with the volumes — the driver simply had no retry on reads and no way to
// report what Proxmox had actually answered.
type apiTransport struct {
	// base is the underlying RoundTripper. It is deliberately nil unless the
	// cluster is configured insecure, and resolved to http.DefaultTransport per
	// request rather than at construction: the unit suite mocks the Proxmox API
	// with httpmock, which swaps http.DefaultTransport after the pool has already
	// been built. Capturing it here would bypass the mock and send the tests at
	// the real network.
	base http.RoundTripper

	attempts int
	backoff  time.Duration
}

// newAPIHTTPClient builds the HTTP client every Proxmox API call in the pool goes
// through.
func newAPIHTTPClient(insecure bool) *http.Client {
	tr := &apiTransport{
		attempts: apiRetryAttempts,
		backoff:  apiRetryBackoff,
	}

	if insecure {
		tr.base = &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, //nolint:gosec // the cluster is explicitly configured insecure
				MinVersion:         tls.VersionTLS12,
			},
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			// A single volume operation fans out over every VM on a node. Go's
			// default of two idle connections per host turns that into a burst of
			// TCP and TLS handshakes against one pveproxy.
			MaxIdleConns:        apiMaxIdleConns,
			MaxIdleConnsPerHost: apiMaxIdleConnsPerHost,
			MaxConnsPerHost:     apiMaxConnsPerHost,
			IdleConnTimeout:     apiIdleConnTimeout,
		}
	}

	return &http.Client{
		Transport: tr,
		Timeout:   apiRequestTimeout,
	}
}

// RoundTrip implements http.RoundTripper.
func (t *apiTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}

	// Only GET is replayed. The proxmod copy and rename, the VM config POSTs and
	// the storage content DELETE all change state and must never be sent twice.
	if req.Method != http.MethodGet {
		res, err := base.RoundTrip(req)
		if err != nil {
			return nil, err
		}

		return describeStatus(res)
	}

	attempts := t.attempts
	if attempts < 1 {
		attempts = 1
	}

	var (
		res *http.Response
		err error
	)

	for attempt := 1; ; attempt++ {
		res, err = base.RoundTrip(req)

		if attempt >= attempts || !retryable(res, err) {
			break
		}

		metrics.ObserveRetry(req.Method, retryOutcome(res, err))
		drain(res)

		if werr := sleep(req.Context(), t.backoff); werr != nil {
			return nil, werr
		}
	}

	if err != nil {
		return nil, err
	}

	return describeStatus(res)
}

// retryable reports whether the exchange is worth repeating. It is only ever
// consulted for GET.
func retryable(res *http.Response, err error) bool {
	if err != nil {
		// A canceled or expired context will not improve on a second try, and the
		// caller's deadline is the authority on how long to keep going.
		return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
	}

	if res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= http.StatusInternalServerError {
		// >= 500 deliberately covers PVE's own proxy codes — 595 no route to host,
		// 596 connection error, 599 too many redirections — which pveproxy answers
		// with an empty body.
		return true
	}

	// A successful read with nothing in it is what the client library reports as
	// "unexpected end of JSON input". No Proxmox API GET legitimately answers with
	// an empty body.
	return res.StatusCode/100 == 2 && res.ContentLength == 0
}

// retryOutcome labels why a request is being retried.
func retryOutcome(res *http.Response, err error) string {
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return "timeout"
		}

		return "transport-error"
	}

	if res.StatusCode/100 == 2 {
		return "empty-body"
	}

	return strconv.Itoa(res.StatusCode)
}

// describeStatus rewrites responses the Proxmox client library would otherwise
// report as "unexpected end of JSON input".
//
// handleResponse in luthermonson/go-proxmox maps only 400, 500 and 501 to an
// error; every other status falls through to a JSON unmarshal of a body Proxmox
// never wrote, and the status code is lost with it. Swapping the code — while
// leaving the status line alone, which is what the library puts in the error —
// turns that into "596 Connection error".
func describeStatus(res *http.Response) (*http.Response, error) {
	switch {
	case res.StatusCode < http.StatusBadRequest,
		res.StatusCode == http.StatusBadRequest,
		res.StatusCode == http.StatusInternalServerError,
		res.StatusCode == http.StatusNotImplemented:
		return res, nil
	}

	body, err := io.ReadAll(res.Body)

	_ = res.Body.Close() //nolint:errcheck

	if err != nil {
		return nil, fmt.Errorf("reading %s response body: %w", res.Status, err)
	}

	res.Body = io.NopCloser(bytes.NewReader(body))

	// A well-formed error payload says more than the status line does; leave it
	// for the library to unmarshal and report.
	if json.Valid(body) {
		return res, nil
	}

	res.StatusCode = http.StatusInternalServerError

	return res, nil
}

// drain empties and closes a response being thrown away, so its connection can
// go back to the idle pool instead of being torn down.
func drain(res *http.Response) {
	if res == nil || res.Body == nil {
		return
	}

	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 1<<16)) //nolint:errcheck
	_ = res.Body.Close()                                        //nolint:errcheck
}

// sleep waits for d, or returns early if the request context is done.
func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
