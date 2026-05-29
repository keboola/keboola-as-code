// nolint: thelper // because it wants the run functions to start with t.Helper()
package proxy_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"html"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/jonboulle/clockwork"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/oauth2-proxy/mockoidc"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/umisama/go-regexpcache"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/dynamic/fake"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	proxyDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/kaipreview"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/oauthproxy/logging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/testutil"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/server"
)

type testCase struct {
	name                  string
	setupK8s              func(t *testing.T, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher)
	run                   func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher)
	expectedNotifications map[string]int
	expectedSpans         tracetest.SpanStubs
}

func TestAppProxyRouter(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			name: "health-check",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request to health-check endpoint
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://hub.keboola.local/health-check", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Equal(t, "OK\n", string(body))
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "missing-app-id",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request without app id
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusBadRequest, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Unexpected domain, missing application ID.")
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "unknown-app-id",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request to unknown app
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://unknown.hub.keboola.local/health-check", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusNotFound, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), html.EscapeString(`Application "unknown" not found in the stack.`))
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "broken-app",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request to broken app
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://broken.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), html.EscapeString(`Application "broken" has invalid configuration for authentication provider "oidc"`))
				assert.Contains(t, string(body), pagewriter.ExceptionIDPrefix)
			},
			expectedNotifications: map[string]int{},
			expectedSpans: []tracetest.SpanStub{
				{
					Name:     "keboola.go.common.dependencies.NewBaseScope",
					SpanKind: 1,
					SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
						TraceID: [16]byte{
							171,
							206,
						},
						SpanID: [8]byte{
							16,
							1,
						},
						TraceFlags: 1,
					}),
				},
				{
					Name:     "keboola.go.common.dependencies.NewPublicScope",
					SpanKind: 1,
					SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
						TraceID: [16]byte{
							171,
							207,
						},
						SpanID: [8]byte{
							16,
							2,
						},
						TraceFlags: 1,
					}),
					Status: tracesdk.Status{
						Code: codes.Ok,
					},
					ChildSpanCount: 1,
				},
				{
					Name:     "keboola.go.api.client.request",
					SpanKind: 3,
					SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
						TraceID: [16]byte{
							171,
							207,
						},
						SpanID: [8]byte{
							16,
							3,
						},
						TraceFlags: 1,
					}),
					Parent: trace.NewSpanContext(trace.SpanContextConfig{
						TraceID: [16]byte{
							171,
							207,
						},
						SpanID: [8]byte{
							16,
							2,
						},
						TraceFlags: 1,
					}),
					Attributes: []attribute.KeyValue{
						attribute.String("span.kind", "client"),
						attribute.String("span.type", "http"),
						attribute.Int("api.requests_count", 1),
						attribute.String("http.result_type", "*keboola.IndexComponents"),
						attribute.String("resource.name", "keboola.(*PublicAPI).IndexComponentsRequest"),
						attribute.String("api.request_defined_in", "keboola.(*PublicAPI).IndexComponentsRequest"),
					},
				},
				{
					Name:     "keboola.go.appsproxy.dependencies.newServiceScope",
					SpanKind: 1,
					SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
						TraceID: [16]byte{
							171,
							208,
						},
						SpanID: [8]byte{
							16,
							4,
						},
						TraceFlags: 1,
					}),
					Status: tracesdk.Status{
						Code: codes.Ok,
					},
				},
			},
		},
		{
			name: "no-rule-app",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request to app with no path rules
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://norule.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), html.EscapeString(`no path rule is configured`))
				assert.Contains(t, string(body), pagewriter.ExceptionIDPrefix)
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "wrong-rule-type-app",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request to app with invalid rule type
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://invalid1.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), html.EscapeString(`no authentication provider is configured`))
				assert.Contains(t, string(body), pagewriter.ExceptionIDPrefix)
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "missing-referenced-provider",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request to app with invalid rule type
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://invalid2.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), html.EscapeString(`authentication provider "test" not found for "/"`))
				assert.Contains(t, string(body), pagewriter.ExceptionIDPrefix)
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "empty-allowed-roles-array-app",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request to app with invalid rule type
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://invalid3.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), html.EscapeString(`Application "invalid3" has invalid configuration for authentication provider "oidc"`))
				assert.Contains(t, string(body), pagewriter.ExceptionIDPrefix)
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "unknown-provider-app",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request to app with unknown provider
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://invalid4.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), html.EscapeString(`authentication provider "unknown" not found for "/"`))
				assert.Contains(t, string(body), pagewriter.ExceptionIDPrefix)
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "not-empty-providers-auth-required-false",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request to app with invalid rule type
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://invalid5.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), html.EscapeString(`no authentication provider is expected for "/"`))
				assert.Contains(t, string(body), pagewriter.ExceptionIDPrefix)
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "redirect-to-canonical-host",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Redirect to the canonical URL (match cookies domain)
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://foo-bar-123.hub.keboola.local/some/data/app/url?foo=bar", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusPermanentRedirect, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Equal(t, "https://public-123.hub.keboola.local/some/data/app/url?foo=bar", location)
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "redirect-to-host-lowercase",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://foo-12345.hub.keboola.local/some/data/app/url?foo=bar", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusPermanentRedirect, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Equal(t, "https://lowercase-12345.hub.keboola.local/some/data/app/url?foo=bar", location)
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "public-app-down",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				appServer.Close()

				// Request to public app
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusBadGateway, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), `Request to application failed.`)
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "public-app-sub-url",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request to public app
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://public-123.hub.keboola.local/some/data/app/url?foo=bar", nil)
				request.Header.Set("User-Agent", "Internet Exploder")
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("x-kbc-Test", "something")
				request.Header.Set("X-KBC-User-Email", "admin@keboola.com")
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Equal(t, `Hello, client`, string(body))

				require.Len(t, *appServer.Requests, 1)
				appRequest := (*appServer.Requests)[0]
				assert.Equal(t, "/some/data/app/url?foo=bar", appRequest.URL.String())
				assert.Equal(t, "Internet Exploder", appRequest.Header.Get("User-Agent"))
				assert.Equal(t, "application/json", appRequest.Header.Get("Content-Type"))
				assert.NotEmpty(t, appRequest.Header.Get("X-Request-ID"))
				assert.Empty(t, appRequest.Header.Get("X-Kbc-Test"))
				assert.Empty(t, appRequest.Header.Get("X-Kbc-User-Email"))
			},
			expectedNotifications: map[string]int{
				"123": 1,
			},
		},
		{
			name: "forwarded-headers-http",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)

				require.Len(t, *appServer.Requests, 1)
				appRequest := (*appServer.Requests)[0]

				// Host header is rewritten to upstream hostname (for LB routing).
				assert.Equal(t, appRequest.Host, appServer.Listener.Addr().String())

				// X-Forwarded-Host preserves the original client-facing hostname.
				assert.Equal(t, "public-123.hub.keboola.local", appRequest.Header.Get("X-Forwarded-Host"))

				// X-Forwarded-Proto preserves the original scheme.
				assert.Equal(t, "https", appRequest.Header.Get("X-Forwarded-Proto"))

				// X-Forwarded-For contains the client IP.
				assert.NotEmpty(t, appRequest.Header.Get("X-Forwarded-For"))
			},
			expectedNotifications: map[string]int{
				"123": 1,
			},
		},
		{
			// Nginx Ingress on GKE sets X-Forwarded-Scheme instead of X-Forwarded-Proto.
			// Verify apps-proxy normalises it to X-Forwarded-Proto for the upstream.
			name: "forwarded-headers-x-forwarded-scheme-fallback",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
				require.NoError(t, err)
				// Simulate ingress that sets X-Forwarded-Scheme but not X-Forwarded-Proto.
				request.Header.Set("X-Forwarded-Scheme", "https")
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)

				require.Len(t, *appServer.Requests, 1)
				appRequest := (*appServer.Requests)[0]

				// X-Forwarded-Proto must be set from X-Forwarded-Scheme fallback.
				assert.Equal(t, "https", appRequest.Header.Get("X-Forwarded-Proto"))
			},
			expectedNotifications: map[string]int{
				"123": 1,
			},
		},
		{
			name: "forwarded-headers-websocket",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()

				c, _, err := websocket.Dial(
					ctx,
					"wss://public-123.hub.keboola.local/ws",
					&websocket.DialOptions{
						HTTPClient: client,
					},
				)
				require.NoError(t, err)

				var v any
				err = wsjson.Read(ctx, c, &v)
				require.NoError(t, err)
				assert.Equal(t, "Hello websocket", v)
				require.NoError(t, c.Close(websocket.StatusNormalClosure, ""))

				require.Len(t, *appServer.Requests, 1)
				appRequest := (*appServer.Requests)[0]

				// Host header is rewritten to upstream hostname (for LB routing).
				assert.Equal(t, appRequest.Host, appServer.Listener.Addr().String())

				// X-Forwarded-Host preserves the original client-facing hostname.
				assert.Equal(t, "public-123.hub.keboola.local", appRequest.Header.Get("X-Forwarded-Host"))

				// X-Forwarded-Proto preserves the original scheme.
				assert.Equal(t, "https", appRequest.Header.Get("X-Forwarded-Proto"))

				// X-Forwarded-For contains the client IP.
				assert.NotEmpty(t, appRequest.Header.Get("X-Forwarded-For"))

				// Origin is rewritten to the upstream hostname (Streamlit/Tornado workaround).
				assert.Equal(t, "http://"+appServer.Listener.Addr().String(), appRequest.Header.Get("Origin"))
			},
			expectedNotifications: map[string]int{
				"123": 1,
			},
		},
		{
			// Streamlit (Tornado) rejects WebSocket connections when Origin does not match Host.
			// apps-proxy rewrites Host to the upstream hostname for LB routing, so Origin
			// (set by the browser to the public domain) would mismatch.
			// Verify the temporary workaround: Origin is rewritten to the upstream hostname.
			name: "websocket-origin-rewrite-streamlit-workaround",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()

				c, _, err := websocket.Dial(
					ctx,
					"wss://public-123.hub.keboola.local/ws",
					&websocket.DialOptions{
						HTTPClient: client,
						// Simulate browser setting Origin to the public domain.
						HTTPHeader: http.Header{
							"Origin": []string{"https://public-123.hub.keboola.local"},
						},
					},
				)
				require.NoError(t, err)

				var v any
				err = wsjson.Read(ctx, c, &v)
				require.NoError(t, err)
				assert.Equal(t, "Hello websocket", v)
				require.NoError(t, c.Close(websocket.StatusNormalClosure, ""))

				require.Len(t, *appServer.Requests, 1)
				appRequest := (*appServer.Requests)[0]

				// Origin must be rewritten to the upstream hostname so Tornado's
				// check_origin() sees Origin == Host and accepts the connection.
				assert.Equal(t, "http://"+appServer.Listener.Addr().String(), appRequest.Header.Get("Origin"))
			},
			expectedNotifications: map[string]int{
				"123": 1,
			},
		},
		{
			name: "private-app-verified-email",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:         "admin@keboola.com",
					EmailVerified: new(true),
					Groups:        []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Retry with provider cookie
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://oidc.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Request to private app (authorized)
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)

				// Request to sign out url
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/_proxy/sign_out", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				cookies := response.Cookies()
				if assert.Len(t, cookies, 2) {
					assert.Equal(t, "_oauth2_provider", cookies[0].Name)
					assert.Empty(t, cookies[0].Value)
					assert.Equal(t, "/", cookies[0].Path)
					assert.Equal(t, "oidc.hub.keboola.local", cookies[0].Domain)
					assert.True(t, cookies[0].HttpOnly)
					assert.True(t, cookies[0].Secure)
					assert.Equal(t, http.SameSiteStrictMode, cookies[0].SameSite)

					assert.Equal(t, "_oauth2_proxy", cookies[1].Name)
					assert.Empty(t, cookies[1].Value)
					assert.Equal(t, "/", cookies[1].Path)
					assert.Equal(t, "oidc.hub.keboola.local", cookies[1].Domain)
					assert.True(t, cookies[1].HttpOnly)
					assert.True(t, cookies[1].Secure)
					assert.Equal(t, http.SameSiteStrictMode, cookies[1].SameSite)
				}
			},
			expectedNotifications: map[string]int{
				"oidc": 1,
			},
		},
		{
			name: "private-app-unauthorized",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "/oidc/authorize?client_id=")

				// Make the next request to the provider fail
				m[0].QueueError(&mockoidc.ServerError{
					Code:  http.StatusUnauthorized,
					Error: mockoidc.InvalidRequest,
				})

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusUnauthorized, response.StatusCode)

				// Request to private app (still unauthorized because login failed)
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "private-missing-csrf-token",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "/oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://oidc.hub.keboola.local/_proxy/callback?")

				// Remove csrf cookie
				client.Jar.SetCookies(
					&url.URL{
						Scheme: "https",
						Host:   "oidc.hub.keboola.local",
					},
					[]*http.Cookie{
						{
							Name:   "_oauth2_proxy_csrf",
							Value:  "",
							Path:   "/",
							Domain: "oidc.hub.keboola.local",
						},
					},
				)

				// Request to proxy callback
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback (fails because of missing CSRF token)
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)
				wildcards.Assert(t, "%ALogin Failed: Unable to find a valid CSRF token. Please try again.%A", string(body))

				// Request to private app
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "private-app-group-mismatch",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:  "manager@keboola.com",
					Groups: []string{"manager"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "/oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://oidc.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback (fails because of missing group) - meta tag redirect
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)
				wildcards.Assert(t, "%AYou do not have permission to access this resource.%A", string(body))

				// Request to private app
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "private-app-unverified-email",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:         "admin@keboola.com",
					EmailVerified: new(false),
					Groups:        []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "/oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://oidc.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback (fails because of unverified email) - meta tag redirect
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusInternalServerError, response.StatusCode)

				// Request to private app
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "private-app-oidc-down",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "/oidc/authorize?client_id=")
				cookies := response.Cookies()
				if assert.Len(t, cookies, 2) {
					assert.Equal(t, "_oauth2_provider", cookies[0].Name)
					assert.Equal(t, "oidc", cookies[0].Value)
					assert.Equal(t, "/", cookies[0].Path)
					assert.Equal(t, "oidc.hub.keboola.local", cookies[0].Domain)
					assert.True(t, cookies[0].HttpOnly)
					assert.True(t, cookies[0].Secure)
					assert.Equal(t, http.SameSiteStrictMode, cookies[0].SameSite)

					assert.Equal(t, "_oauth2_proxy_csrf", cookies[1].Name)
					assert.Equal(t, "/", cookies[1].Path)
					assert.Equal(t, "oidc.hub.keboola.local", cookies[1].Domain)
					assert.True(t, cookies[1].HttpOnly)
					assert.True(t, cookies[1].Secure)
					assert.Equal(t, http.SameSiteNoneMode, cookies[1].SameSite)
				}

				// Shutdown provider server
				require.NoError(t, m[0].Shutdown())

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				_, err = client.Do(request)
				require.Error(t, err)
				require.Contains(t, err.Error(), "refused")
				var syscallError *os.SyscallError
				errors.As(err, &syscallError)
				require.Contains(t, syscallError.Syscall, "connect")

				// Request to private app (unauthorized)
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "private-app-down",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				appServer.Close()

				m[0].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "oidc/authorize?client_id=")
				cookies := response.Cookies()
				if assert.Len(t, cookies, 2) {
					assert.Equal(t, "_oauth2_provider", cookies[0].Name)
					assert.Equal(t, "oidc", cookies[0].Value)
					assert.Equal(t, "/", cookies[0].Path)
					assert.Equal(t, "oidc.hub.keboola.local", cookies[0].Domain)
					assert.True(t, cookies[0].HttpOnly)
					assert.True(t, cookies[0].Secure)
					assert.Equal(t, http.SameSiteStrictMode, cookies[0].SameSite)

					assert.Equal(t, "_oauth2_proxy_csrf", cookies[1].Name)
					assert.Equal(t, "/", cookies[1].Path)
					assert.Equal(t, "oidc.hub.keboola.local", cookies[1].Domain)
					assert.True(t, cookies[1].HttpOnly)
					assert.True(t, cookies[1].Secure)
					assert.Equal(t, http.SameSiteNoneMode, cookies[1].SameSite)
				}

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://oidc.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				cookies = response.Cookies()
				if assert.Len(t, cookies, 2) {
					assert.Equal(t, "_oauth2_proxy_csrf", cookies[0].Name)
					assert.Empty(t, cookies[0].Value)
					assert.Equal(t, "/", cookies[0].Path)
					assert.Equal(t, "oidc.hub.keboola.local", cookies[0].Domain)
					assert.True(t, cookies[0].HttpOnly)
					assert.True(t, cookies[0].Secure)
					assert.Equal(t, http.SameSiteStrictMode, cookies[0].SameSite)

					assert.Equal(t, "_oauth2_proxy", cookies[1].Name)
					assert.Equal(t, "/", cookies[1].Path)
					assert.Equal(t, "oidc.hub.keboola.local", cookies[1].Domain)
					assert.True(t, cookies[1].HttpOnly)
					assert.True(t, cookies[1].Secure)
					assert.Equal(t, http.SameSiteStrictMode, cookies[1].SameSite)
				}

				// Request to private app (authorized but down)
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusBadGateway, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), `Request to application failed.`)
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "multi-app-basic-flow",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				m[1].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin", "manager"},
				})

				// Request to private app, unauthorized - selector page
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://multi.hub.keboola.local/some/path", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusUnauthorized, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), htmlLinkTo(`https://multi.hub.keboola.local/_proxy/selection?provider=oidc0&rd=%2Fsome%2Fpath`))
				assert.Contains(t, string(body), htmlLinkTo(`https://multi.hub.keboola.local/_proxy/selection?provider=oidc1&rd=%2Fsome%2Fpath`))
				assert.Contains(t, string(body), htmlLinkTo(`https://multi.hub.keboola.local/_proxy/selection?provider=oidc2&rd=%2Fsome%2Fpath`))

				// Select provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://multi.hub.keboola.local/_proxy/selection?provider=oidc1&rd=%2Fsome%2Fpath", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Equal(t, "https://multi.hub.keboola.local/_proxy/sign_in?rd=%2Fsome%2Fpath", location)
				cookies := response.Cookies()
				if assert.Len(t, cookies, 1) {
					assert.Equal(t, "_oauth2_provider", cookies[0].Name)
					assert.Equal(t, "oidc1", cookies[0].Value)
					assert.Equal(t, "/", cookies[0].Path)
					assert.Equal(t, "multi.hub.keboola.local", cookies[0].Domain)
					assert.True(t, cookies[0].HttpOnly)
					assert.True(t, cookies[0].Secure)
					assert.Equal(t, http.SameSiteStrictMode, cookies[0].SameSite)
				}

				// Redirect to the provider sing in page
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "/oidc/authorize?client_id=")
				cookies = response.Cookies()
				if assert.Len(t, cookies, 1) {
					assert.Equal(t, "_oauth2_proxy_csrf", cookies[0].Name)
					assert.Equal(t, "/", cookies[0].Path)
					assert.Equal(t, "multi.hub.keboola.local", cookies[0].Domain)
					assert.True(t, cookies[0].HttpOnly)
					assert.True(t, cookies[0].Secure)
					assert.Equal(t, http.SameSiteNoneMode, cookies[0].SameSite)
				}

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://multi.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				location = response.Header.Get("Location")
				assert.Equal(t, "/some/path", location)

				// Request to private app (authorized)
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://multi.hub.keboola.local/some/data/app/url?foo=bar", nil)
				request.Header.Set("X-Kbc-Test", "something")
				request.Header.Set("X-Kbc-User-Email", "manager@keboola.com")
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)

				require.Len(t, *appServer.Requests, 1)
				appRequest := (*appServer.Requests)[0]
				assert.Equal(t, "/some/data/app/url?foo=bar", appRequest.URL.String())
				assert.Equal(t, "admin@keboola.com", appRequest.Header.Get("X-Kbc-User-Email"))
				assert.Equal(t, "admin,manager", appRequest.Header.Get("X-Kbc-User-Roles"))
				assert.Empty(t, appRequest.Header.Get("X-Kbc-Test"))
			},
			expectedNotifications: map[string]int{
				"multi": 1,
			},
		},
		{
			name: "multi-app-redirect-to-selection-page",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				m[1].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Provider selection
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://multi.hub.keboola.local/_proxy/selection?provider=oidc1", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Equal(t, "https://multi.hub.keboola.local/_proxy/sign_in", location)

				// Request to app - unauthorized - redirect to the selector page
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusUnauthorized, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), htmlLinkTo(`https://multi.hub.keboola.local/_proxy/selection?provider=oidc0`))
				assert.Contains(t, string(body), htmlLinkTo(`https://multi.hub.keboola.local/_proxy/selection?provider=oidc1`))
				assert.Contains(t, string(body), htmlLinkTo(`https://multi.hub.keboola.local/_proxy/selection?provider=oidc2`))
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "multi-app-unverified-email",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				m[1].QueueUser(&mockoidc.MockUser{
					Email:         "admin@keboola.com",
					EmailVerified: new(false),
					Groups:        []string{"admin"},
				})

				// Provider selection
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://multi.hub.keboola.local/_proxy/selection?provider=oidc1", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "https://multi.hub.keboola.local/_proxy/sign_in")

				// Redirect to the provider sing in page
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "oidc/authorize?client_id=")

				// Request to private app (unauthorized)
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusUnauthorized, response.StatusCode)

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://multi.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback (fails because of unverified email)
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusInternalServerError, response.StatusCode)

				// Request to private app - unauthorized - selection page
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusUnauthorized, response.StatusCode)
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "multi-app-down",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				appServer.Close()

				m[1].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app, unauthorized - selector page
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusUnauthorized, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), htmlLinkTo(`https://multi.hub.keboola.local/_proxy/selection?provider=oidc0`))
				assert.Contains(t, string(body), htmlLinkTo(`https://multi.hub.keboola.local/_proxy/selection?provider=oidc1`))
				assert.Contains(t, string(body), htmlLinkTo(`https://multi.hub.keboola.local/_proxy/selection?provider=oidc2`))

				// Provider selection
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://multi.hub.keboola.local/_proxy/selection?provider=oidc1", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "https://multi.hub.keboola.local/_proxy/sign_in")

				// Redirect to the provider sing in page
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://multi.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Request to private app (authorized but down)
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusBadGateway, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), `Request to application failed.`)
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "multi-app-broken-provider",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				appServer.Close()

				// Provider selection
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://multi.hub.keboola.local/_proxy/selection?provider=oidc2", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Equal(t, "https://multi.hub.keboola.local/error", location)

				// Follow redirect to sign in page
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), html.EscapeString(`Application "multi" has invalid configuration for authentication provider "oidc2"`))
				assert.Contains(t, string(body), pagewriter.ExceptionIDPrefix)
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "public-app-websocket",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()

				c, _, err := websocket.Dial(
					ctx,
					"wss://public-123.hub.keboola.local/ws",
					&websocket.DialOptions{
						HTTPClient: client,
					},
				)
				require.NoError(t, err)

				var v any
				err = wsjson.Read(ctx, c, &v)
				require.NoError(t, err)

				assert.Equal(t, "Hello websocket", v)

				require.NoError(t, c.Close(websocket.StatusNormalClosure, ""))
			},
			expectedNotifications: map[string]int{
				"123": 1,
			},
		},
		{
			name: "private-app-websocket-unauthorized",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()

				_, _, err := websocket.Dial(
					ctx,
					"wss://oidc.hub.keboola.local/ws",
					&websocket.DialOptions{
						HTTPClient: client,
					},
				)
				require.Error(t, err)
				require.Contains(t, err.Error(), "failed to WebSocket dial: expected handshake response status code 101 but got 302")
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "private-app-websocket",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:         "admin@keboola.com",
					EmailVerified: new(true),
					Groups:        []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "/oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://oidc.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Websocket request
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "wss://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)

				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()

				c, _, err := websocket.Dial(
					ctx,
					"wss://oidc.hub.keboola.local/ws",
					&websocket.DialOptions{
						HTTPClient: client,
						HTTPHeader: request.Header,
					},
				)
				require.NoError(t, err)

				var v any
				err = wsjson.Read(ctx, c, &v)
				require.NoError(t, err)

				assert.Equal(t, "Hello websocket", v)

				require.NoError(t, c.Close(websocket.StatusNormalClosure, ""))
			},
			expectedNotifications: map[string]int{
				"oidc": 1,
			},
		},
		{
			name: "websocket-connection-check",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				ctx, cancel := context.WithTimeout(t.Context(), time.Second*30)
				defer cancel()

				c, _, err := websocket.Dial(ctx, "wss://public-111.hub.keboola.local/ws2", &websocket.DialOptions{HTTPClient: client})
				require.NoError(t, err)

				originalConfig := service.Apps["111"]
				originalConfig.AppSlug = new("p")
				service.Apps["111"] = originalConfig

				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://p-111.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)

				assert.Eventually(t, func() bool {
					if err = c.Write(ctx, websocket.MessageText, []byte("Hello websocket")); err == nil {
						return false
					}
					return true
				}, 10*time.Second, time.Millisecond*100)
			},
			expectedNotifications: map[string]int{
				"111": 1,
			},
		},
		{
			name: "multi-app-websocket",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				m[1].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app, unauthorized - selector page
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusUnauthorized, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), htmlLinkTo(`https://multi.hub.keboola.local/_proxy/selection?provider=oidc0`))
				assert.Contains(t, string(body), htmlLinkTo(`https://multi.hub.keboola.local/_proxy/selection?provider=oidc1`))
				assert.Contains(t, string(body), htmlLinkTo(`https://multi.hub.keboola.local/_proxy/selection?provider=oidc2`))

				// Provider selection
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://multi.hub.keboola.local/_proxy/selection?provider=oidc1", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "https://multi.hub.keboola.local/_proxy/sign_in")

				// Redirect to the provider sing in page
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://multi.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Websocket request
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "wss://multi.hub.keboola.local/", nil)
				require.NoError(t, err)

				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()

				c, _, err := websocket.Dial(
					ctx,
					"wss://multi.hub.keboola.local/ws",
					&websocket.DialOptions{
						HTTPClient: client,
						HTTPHeader: request.Header,
					},
				)
				require.NoError(t, err)

				var v any
				err = wsjson.Read(ctx, c, &v)
				require.NoError(t, err)

				assert.Equal(t, "Hello websocket", v)

				require.NoError(t, c.Close(websocket.StatusNormalClosure, ""))
			},
			expectedNotifications: map[string]int{
				"multi": 1,
			},
		},
		{
			name: "prefix-app-no-auth",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request to public part of the app
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://prefix.hub.keboola.local/public", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)

				// Request to api unauthorized - redirect to the sign in page, there is only one provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://prefix.hub.keboola.local/api", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Request to web - unauthorized - selection page, there are two providers
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://prefix.hub.keboola.local/web", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusUnauthorized, response.StatusCode)

				// Request to web (no matching prefix)
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://prefix.hub.keboola.local/unknown", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusNotFound, response.StatusCode)
			},
			expectedNotifications: map[string]int{
				"prefix": 1,
			},
		},
		{
			name: "prefix-app-api-auth",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private part (unauthorized)
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://prefix.hub.keboola.local/api", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "/oidc/authorize?client_id=")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://prefix.hub.keboola.local/_proxy/callback")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Equal(t, "/api", location)

				// Request to private part (authorized)
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://prefix.hub.keboola.local/api", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)

				// Since the provider is configured for both /api and /web this works as well.
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://prefix.hub.keboola.local/web", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
			},
			expectedNotifications: map[string]int{
				"prefix": 1,
			},
		},
		{
			name: "prefix-app-web-auth",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				m[1].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private part, unauthorized - selector page
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://prefix.hub.keboola.local/web", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusUnauthorized, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), htmlLinkTo(`https://prefix.hub.keboola.local/_proxy/selection?provider=oidc0&rd=%2Fweb`))
				assert.Contains(t, string(body), htmlLinkTo(`https://prefix.hub.keboola.local/_proxy/selection?provider=oidc1&rd=%2Fweb`))

				// Provider selection
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://prefix.hub.keboola.local/_proxy/selection?provider=oidc1&rd=%2Fweb", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "https://prefix.hub.keboola.local/_proxy/sign_in")

				// Redirect to the provider sing in page
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://prefix.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Request to private part (authorized)
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://prefix.hub.keboola.local/web", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)

				// Since the provider is configured only for web, this needs to fail.
				// !! In order for this to fail it is necessary for each provider to use a different cookie secret.
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://prefix.hub.keboola.local/api", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
			expectedNotifications: map[string]int{
				"prefix": 1,
			},
		},
		{
			name: "shared-provider",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				m[1].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Provider selection
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://prefix.hub.keboola.local/_proxy/selection?provider=oidc1", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "https://prefix.hub.keboola.local/_proxy/sign_in")

				// Redirect to the provider sing in page
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://prefix.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Request to private part (authorized)
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://prefix.hub.keboola.local/web", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)

				// Use the same cookie values for another app, this needs to fail.
				cookies := client.Jar.Cookies(
					&url.URL{
						Scheme: "https",
						Host:   "prefix.hub.keboola.local",
					},
				)
				for _, cookie := range cookies {
					cookie.Domain = `multi.hub.keboola.local`
				}
				client.Jar.SetCookies(
					&url.URL{
						Scheme: "https",
						Host:   "multi.hub.keboola.local",
					},
					cookies,
				)

				// !! In order for this to fail it is necessary for each app to use a different cookie secret even if the provider is the same.
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusUnauthorized, response.StatusCode)
			},
			expectedNotifications: map[string]int{
				"prefix": 1,
			},
		},
		{
			name: "configuration-change",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request to public app
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)

				// Change configuration to private
				originalConfig := service.Apps["123"]
				newConfig := service.Apps["oidc"]
				newConfig.ID = "public-123"

				service.Apps["123"] = newConfig

				// Request to the same app which is now private
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
				require.NoError(t, err)

				assert.Eventually(t, func() bool {
					response, err = client.Do(request)
					return err == nil
				}, time.Second*5, time.Millisecond*100)

				require.Equal(t, http.StatusFound, response.StatusCode)

				// Revert configuration
				service.Apps["123"] = originalConfig

				// Request to public app
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
				require.NoError(t, err)

				assert.Eventually(t, func() bool {
					response, err = client.Do(request)
					return err == nil
				}, time.Second*5, time.Millisecond*100)

				require.Equal(t, http.StatusOK, response.StatusCode)
			},
			expectedNotifications: map[string]int{
				"123": 1,
			},
		},
		{
			name: "concurrency-test",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()

				wg := sync.WaitGroup{}
				counter := atomic.NewInt64(0)
				for range 100 {
					wg.Go(func() {
						m[0].QueueUser(&mockoidc.MockUser{
							Email:         "admin@keboola.com",
							EmailVerified: new(true),
							Groups:        []string{"admin"},
						})

						request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://oidc.hub.keboola.local/foo/bar", nil)
						require.NoError(t, err)

						response, err := client.Do(request)
						if assert.NoError(t, err) {
							if assert.Equal(t, http.StatusFound, response.StatusCode) {
								counter.Add(1)
							}
						}
					})
				}

				// Wait for all requests
				wg.Wait()

				// Check total requests count
				assert.Equal(t, int64(100), counter.Load())
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "public-app-wakeup",
			setupK8s: func(t *testing.T, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				patch := []byte(`{"status":{"currentState":"Stopped"}}`)
				_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace("keboola").Patch(
					t.Context(), "app-123", k8stypes.MergePatchType, patch, metav1.PatchOptions{},
				)
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					info, ok := watcher.GetState(t.Context(), api.AppID("123"))
					return ok && info.ActualState == k8sapp.AppActualStateStopped
				}, 5*time.Second, 50*time.Millisecond)
			},
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request to public app - fails because the app is Stopped
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)

				// Patch app back to Running
				patch := []byte(`{"status":{"currentState":"Running"}}`)
				_, err = fakeClient.Resource(k8sapp.AppGVR()).Namespace("keboola").Patch(
					t.Context(), "app-123", k8stypes.MergePatchType, patch, metav1.PatchOptions{},
				)
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					info, ok := watcher.GetState(t.Context(), api.AppID("123"))
					return ok && info.ActualState == k8sapp.AppActualStateRunning
				}, 5*time.Second, 50*time.Millisecond)

				// Request to public app
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Equal(t, `Hello, client`, string(body))
			},
			expectedNotifications: map[string]int{
				"123": 1,
			},
		},
		{
			name: "public-app-wakeup-only",
			setupK8s: func(t *testing.T, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				patch := []byte(`{"status":{"currentState":"Stopped"}}`)
				_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace("keboola").Patch(
					t.Context(), "app-123", k8stypes.MergePatchType, patch, metav1.PatchOptions{},
				)
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					info, ok := watcher.GetState(t.Context(), api.AppID("123"))
					return ok && info.ActualState == k8sapp.AppActualStateStopped
				}, 5*time.Second, 50*time.Millisecond)
			},
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request to public app - fails because the app is Stopped, triggers wakeup
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Starting your application...")

				// Expect wakeup but no notification since there was an authorized request to the app but not while it was running.
			},
			expectedNotifications: map[string]int{},
		},
		{
			// Background-poll endpoints emitted by data-app frontends
			// (Streamlit's /_stcore/health and /_stcore/host-config) fire
			// independently of user interaction — every WS reconnect cycle
			// while the tab stays open. Apps-proxy must NOT count them as
			// activity, otherwise auto-suspend never triggers.
			name: "public-app-framework-background-poll-skips-notify",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Three background-poll requests against a Running app. Each
				// reaches the upstream (GotConn fires), but the new filter in
				// trace() must skip notify for these paths. The upstream test
				// server has no handler for them, so we accept whatever status
				// it returns — what matters is the notify count below.
				for _, path := range []string{"/_stcore/health", "/_stcore/host-config", "/_stcore/health"} {
					request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://public-123.hub.keboola.local"+path, nil)
					require.NoError(t, err)
					response, err := client.Do(request)
					require.NoError(t, err)
					_ = response.Body.Close()
				}
			},
			expectedNotifications: map[string]int{},
		},
		{
			// On a Suspended app, framework background polls must NOT trigger
			// wakeup — otherwise a forgotten tab whose frontend keeps polling
			// every WS reconnect cycle would re-wake the app indefinitely,
			// defeating auto-suspend. Apps-proxy replies 503 with a plain-text
			// "paused due to inactivity, refresh to start" message (shown in the
			// frontend's connection modal) and leaves the app alone. Meaningful
			// user actions (refresh → GET /) wake the app via the default branch.
			name: "public-app-framework-background-poll-on-suspended-returns-503-no-wakeup",
			setupK8s: func(t *testing.T, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				patch := []byte(`{"status":{"currentState":"Stopped"}}`)
				_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace("keboola").Patch(
					t.Context(), "app-123", k8stypes.MergePatchType, patch, metav1.PatchOptions{},
				)
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					info, ok := watcher.GetState(t.Context(), api.AppID("123"))
					return ok && info.ActualState == k8sapp.AppActualStateStopped
				}, 5*time.Second, 50*time.Millisecond)
			},
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://public-123.hub.keboola.local/_stcore/health", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				defer response.Body.Close()

				require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
				assert.NotEmpty(t, response.Header.Get("Retry-After"), "should advise the client to back off")
				assert.Equal(t, "text/plain; charset=utf-8", response.Header.Get("Content-Type"),
					"must be plain text — the frontend modal does not render HTML")

				// Body carries the user-facing "paused, refresh to start" message
				// that the frontend shows in its connection modal. It must NOT be
				// the spinner page ("Starting your application...") served by the
				// default branch, which would imply the app is auto-restarting.
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Refresh the page",
					"should instruct the user to reload")
				assert.NotContains(t, string(body), "Starting your application...")
			},
			expectedNotifications: map[string]int{},
		},

		{
			name: "private-one-provider-selector",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request provider selector page - no auth provider
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/_proxy/selection", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), htmlLinkTo(`https://oidc.hub.keboola.local/_proxy/selection?provider=oidc`))
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "private-app-wakeup",
			setupK8s: func(t *testing.T, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				patch := []byte(`{"status":{"currentState":"Stopped"}}`)
				_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace("keboola").Patch(
					t.Context(), "app-oidc", k8stypes.MergePatchType, patch, metav1.PatchOptions{},
				)
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					info, ok := watcher.GetState(t.Context(), api.AppID("oidc"))
					return ok && info.ActualState == k8sapp.AppActualStateStopped
				}, 5*time.Second, 50*time.Millisecond)
			},
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Retry with provider cookie
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://oidc.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Request to private app (authorized but app is Stopped, triggers wakeup)
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)

				// Patch app back to Running
				patch := []byte(`{"status":{"currentState":"Running"}}`)
				_, err = fakeClient.Resource(k8sapp.AppGVR()).Namespace("keboola").Patch(
					t.Context(), "app-oidc", k8stypes.MergePatchType, patch, metav1.PatchOptions{},
				)
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					info, ok := watcher.GetState(t.Context(), api.AppID("oidc"))
					return ok && info.ActualState == k8sapp.AppActualStateRunning
				}, 5*time.Second, 50*time.Millisecond)

				// Request to private app (authorized)
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
			},
			expectedNotifications: map[string]int{
				"oidc": 1,
			},
		},
		{
			name: "private-app-wakeup-only",
			setupK8s: func(t *testing.T, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				patch := []byte(`{"status":{"currentState":"Stopped"}}`)
				_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace("keboola").Patch(
					t.Context(), "app-oidc", k8stypes.MergePatchType, patch, metav1.PatchOptions{},
				)
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					info, ok := watcher.GetState(t.Context(), api.AppID("oidc"))
					return ok && info.ActualState == k8sapp.AppActualStateStopped
				}, 5*time.Second, 50*time.Millisecond)
			},
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Retry with provider cookie
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://oidc.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Request to private app (authorized but app is Stopped, triggers wakeup)
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Starting your application...")

				// Expect wakeup but no notification since there was an authorized request to the app but not while it was running.
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "private-app-no-wakeup",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Retry with provider cookie
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://oidc.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Expect no notification or wakeup because there was never an authorized request to the app
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "public-basic-auth-wrong-login-no-password",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request public basic auth app
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://basic-auth.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Basic Authentication")

				// Fill wrong password into form
				request, err = http.NewRequestWithContext(t.Context(), http.MethodPost, "https://basic-auth.hub.keboola.local/", bytes.NewBuffer([]byte("password=")))
				request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Please enter a correct password.")
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "public-basic-auth-wrong-login",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request public basic auth app
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://basic-auth.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Basic Authentication")

				// Fill wrong password into form
				request, err = http.NewRequestWithContext(t.Context(), http.MethodPost, "https://basic-auth.hub.keboola.local/", bytes.NewBuffer([]byte("password=def")))
				request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Please enter a correct password.")
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "public-basic-auth-correct-app-url",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request public basic auth app
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://basic-auth.hub.keboola.local/app/url", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Basic Authentication")

				request, err = http.NewRequestWithContext(t.Context(), http.MethodPost, "https://basic-auth.hub.keboola.local/app/url", bytes.NewBuffer([]byte("password=abc")))
				request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusMovedPermanently, response.StatusCode)
				// Check that cookies were set
				cookies := response.Cookies()
				if assert.Len(t, cookies, 1) {
					assert.Equal(t, "proxyBasicAuth", cookies[0].Name)
					assert.Contains(t, cookies[0].Value, "$2a$10$")
					assert.Equal(t, "/", cookies[0].Path)
					assert.Equal(t, "basic-auth.hub.keboola.local", cookies[0].Domain)
					assert.True(t, cookies[0].HttpOnly)
					assert.True(t, cookies[0].Secure)
					assert.Equal(t, http.SameSiteStrictMode, cookies[0].SameSite)
				}

				location := response.Header.Get("Location")
				assert.Contains(t, location, "https://basic-auth.hub.keboola.local/app/url")

				// Request to proxy location
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Hello, client")
			},
			expectedNotifications: map[string]int{
				"auth": 1,
			},
		},
		{
			name: "public-basic-auth-correct-login",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request public basic auth app
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://basic-auth.hub.keboola.local/_proxy/form", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Basic Authentication")

				// Fill correct password into form
				request, err = http.NewRequestWithContext(t.Context(), http.MethodPost, "https://basic-auth.hub.keboola.local/_proxy/form", bytes.NewBuffer([]byte("password=abc")))
				request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusMovedPermanently, response.StatusCode)
				// Check that cookies were set
				cookies := response.Cookies()
				if assert.Len(t, cookies, 1) {
					assert.Equal(t, "proxyBasicAuth", cookies[0].Name)
					assert.Contains(t, cookies[0].Value, "$2a$10$")
					assert.Equal(t, "/", cookies[0].Path)
					assert.Equal(t, "basic-auth.hub.keboola.local", cookies[0].Domain)
					assert.True(t, cookies[0].HttpOnly)
					assert.True(t, cookies[0].Secure)
					assert.Equal(t, http.SameSiteStrictMode, cookies[0].SameSite)
				}

				location := response.Header.Get("Location")
				// All config.InternalPrefix are redirected to `/`
				assert.Contains(t, location, "https://basic-auth.hub.keboola.local/")

				// Request to proxy location
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				request.AddCookie(&http.Cookie{Name: "proxyBasicAuth", Value: cookies[0].Value})
				response, err = client.Do(request)
				require.NoError(t, err)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Hello, client")
			},
			expectedNotifications: map[string]int{
				"auth": 1,
			},
		},
		{
			name: "public-basic-auth-wrong-cookie",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Access with cookie
				request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://basic-auth.hub.keboola.local/", nil)
				request.AddCookie(&http.Cookie{Name: "proxyBasicAuth", Value: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015a"})
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Cookie has expired")
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "public-basic-auth-cookie",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Access with cookie
				request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://basic-auth.hub.keboola.local/", nil)
				request.AddCookie(&http.Cookie{Name: "proxyBasicAuth", Value: "$2a$10$65mF6LI2F0Nm9PkQk8DlJu.C5jD.fseeXWn9CCGmDxLPomikYWtte"})
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Hello, client")
				require.Len(t, response.Cookies(), 2)
			},
			expectedNotifications: map[string]int{
				"auth": 1,
			},
		},
		{
			name: "public-basic-auth-sign-out",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Access with cookie
				request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://basic-auth.hub.keboola.local/_proxy/sign_out", nil)
				request.AddCookie(&http.Cookie{Name: "proxyBasicAuth", Value: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"})
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "https://basic-auth.hub.keboola.local/")

				// Request to proxy location
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Basic Authentication")
				require.Empty(t, response.Cookies())
			},
			expectedNotifications: map[string]int{},
		},
		{
			name: "restart-disabled",
			setupK8s: func(t *testing.T, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// The default setup already created app "123" as Running.
				// Patch it to Stopped + autoRestartEnabled=false.
				patch := []byte(`{"spec":{"autoRestartEnabled":false},"status":{"currentState":"Stopped"}}`)
				_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace("keboola").Patch(
					t.Context(), "app-123", k8stypes.MergePatchType, patch, metav1.PatchOptions{},
				)
				require.NoError(t, err)

				require.Eventually(t, func() bool {
					info, ok := watcher.GetState(t.Context(), api.AppID("123"))
					return ok && !info.AutoRestartEnabled
				}, 5*time.Second, 50*time.Millisecond)
			},
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Use canonical slug-based URL to bypass the slug redirect.
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Application Disabled")
			},
			expectedNotifications: map[string]int{},
		},
		{
			// DEV mode app that is Stopped — proxy must not auto-resume it
			// and must show the "Application Disabled" page.
			name: "dev-mode-no-wakeup",
			setupK8s: func(t *testing.T, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				patch := []byte(`{"spec":{"devMode":{"enabled":true}},"status":{"currentState":"Stopped"}}`)
				_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace("keboola").Patch(
					t.Context(), "app-devmode", k8stypes.MergePatchType, patch, metav1.PatchOptions{},
				)
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					info, ok := watcher.GetState(t.Context(), api.AppID("devmode"))
					return ok && info.DevMode && info.ActualState == k8sapp.AppActualStateStopped
				}, 5*time.Second, 50*time.Millisecond)
			},
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://dev-devmode.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Application Disabled")

				// Confirm the app was not auto-resumed: state remains Stopped.
				info, ok := watcher.GetState(t.Context(), api.AppID("devmode"))
				require.True(t, ok)
				assert.Equal(t, k8sapp.AppActualStateStopped, info.ActualState)
			},
			expectedNotifications: map[string]int{},
		},
		{
			// kai-preview: GET /_proxy/kai-preview/bootstrap on a dev-mode app (Running) → 200 with HTML shim.
			name: "kai-preview-bootstrap-dev-mode-running",
			setupK8s: func(t *testing.T, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				patch := []byte(`{"spec":{"devMode":{"enabled":true}},"status":{"currentState":"Running"}}`)
				_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace("keboola").Patch(
					t.Context(), "app-devmode", k8stypes.MergePatchType, patch, metav1.PatchOptions{},
				)
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					info, ok := watcher.GetState(t.Context(), api.AppID("devmode"))
					return ok && info.DevMode && info.ActualState == k8sapp.AppActualStateRunning
				}, 5*time.Second, 50*time.Millisecond)
			},
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://dev-devmode.hub.keboola.local/_proxy/kai-preview/bootstrap", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				// The bootstrap shim is an HTML page with postMessage logic.
				assert.Contains(t, response.Header.Get("Content-Type"), "text/html")
				// The bootstrap shim references the exchange path.
				assert.Contains(t, string(body), "/_proxy/kai-preview/exchange")
			},
			expectedNotifications: map[string]int{},
		},
		{
			// kai-preview: GET /_proxy/kai-preview/bootstrap on a non-dev-mode app falls through to
			// AuthRules. The "devmode" app has AuthRequired=false, so the upstream is reached — the
			// kai-preview bootstrap shim is NOT served.
			name: "kai-preview-bootstrap-non-dev-mode",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// "devmode" app has DevMode=false by default (makeDefaultK8sObjects does not set devMode).
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://dev-devmode.hub.keboola.local/_proxy/kai-preview/bootstrap", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				// With DevMode=false the request falls through to the AuthRules path.
				// The "devmode" app is public (AuthRequired=false), so the upstream responds directly.
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				// Upstream app returns its response; the bootstrap shim is NOT served.
				assert.Equal(t, "Hello, client", string(body))
			},
			expectedNotifications: map[string]int{
				"devmode": 1,
			},
		},
		{
			// kai-preview: GET / with Sec-Fetch-Dest=iframe on a dev-mode Running app and no session cookie
			// → proxy serves the bootstrap shim (iframe document load detection).
			name: "kai-preview-iframe-bootstrap-fallback",
			setupK8s: func(t *testing.T, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				patch := []byte(`{"spec":{"devMode":{"enabled":true}},"status":{"currentState":"Running"}}`)
				_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace("keboola").Patch(
					t.Context(), "app-devmode", k8stypes.MergePatchType, patch, metav1.PatchOptions{},
				)
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					info, ok := watcher.GetState(t.Context(), api.AppID("devmode"))
					return ok && info.DevMode && info.ActualState == k8sapp.AppActualStateRunning
				}, 5*time.Second, 50*time.Millisecond)
			},
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://dev-devmode.hub.keboola.local/", nil)
				require.NoError(t, err)
				// Simulate an iframe document load (Sec-Fetch-Dest=iframe + Accept=text/html).
				request.Header.Set("Sec-Fetch-Dest", "iframe")
				request.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				// Should be the bootstrap shim HTML, not the upstream app response.
				assert.Contains(t, response.Header.Get("Content-Type"), "text/html")
				assert.NotEqual(t, "Hello, client", strings.TrimSpace(string(body)))
				assert.NotEmpty(t, string(body))
			},
			expectedNotifications: map[string]int{},
		},
	}

	publicAppTestCaseFactory := func(method string) testCase {
		return testCase{
			name: "public-app-" + method,
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				// Request to public app
				request, err := http.NewRequestWithContext(t.Context(), method, "https://public-123.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Equal(t, `Hello, client`, string(body))
			},
			expectedNotifications: map[string]int{
				"123": 1,
			},
		}
	}

	testCases = append(
		testCases,
		publicAppTestCaseFactory(http.MethodGet),
		publicAppTestCaseFactory(http.MethodPost),
		publicAppTestCaseFactory(http.MethodPut),
		publicAppTestCaseFactory(http.MethodPatch),
		publicAppTestCaseFactory(http.MethodDelete),
	)

	privateAppTestCaseFactory := func(method string) testCase {
		return testCase{
			name: "private-app-oidc-" + method,
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, fakeClient *k8sfake.FakeDynamicClient, watcher *k8sapp.StateWatcher) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(t.Context(), method, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "/oidc/authorize?client_id=")
				cookies := response.Cookies()
				if assert.Len(t, cookies, 2) {
					assert.Equal(t, "_oauth2_provider", cookies[0].Name)
					assert.Equal(t, "oidc", cookies[0].Value)
					assert.Equal(t, "/", cookies[0].Path)
					assert.Equal(t, "oidc.hub.keboola.local", cookies[0].Domain)
					assert.True(t, cookies[0].HttpOnly)
					assert.True(t, cookies[0].Secure)
					assert.Equal(t, http.SameSiteStrictMode, cookies[0].SameSite)

					assert.Equal(t, "_oauth2_proxy_csrf", cookies[1].Name)
					assert.Equal(t, "/", cookies[1].Path)
					assert.Equal(t, "oidc.hub.keboola.local", cookies[1].Domain)
					assert.True(t, cookies[1].HttpOnly)
					assert.True(t, cookies[1].Secure)
					assert.Equal(t, http.SameSiteNoneMode, cookies[1].SameSite)
				}

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://oidc.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				cookies = response.Cookies()
				if assert.Len(t, cookies, 2) {
					assert.Equal(t, "_oauth2_proxy_csrf", cookies[0].Name)
					assert.Empty(t, cookies[0].Value)
					assert.Equal(t, "/", cookies[0].Path)
					assert.Equal(t, "oidc.hub.keboola.local", cookies[0].Domain)
					assert.True(t, cookies[0].HttpOnly)
					assert.True(t, cookies[0].Secure)
					assert.Equal(t, http.SameSiteStrictMode, cookies[0].SameSite)

					assert.Equal(t, "_oauth2_proxy", cookies[1].Name)
					assert.Equal(t, "/", cookies[1].Path)
					assert.Equal(t, "oidc.hub.keboola.local", cookies[1].Domain)
					assert.True(t, cookies[1].HttpOnly)
					assert.True(t, cookies[1].Secure)
					assert.Equal(t, http.SameSiteStrictMode, cookies[1].SameSite)
				}

				// Request to private app (authorized)
				request, err = http.NewRequestWithContext(t.Context(), method, "https://oidc.hub.keboola.local/some/data/app/url?foo=bar", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)

				require.Len(t, *appServer.Requests, 1)
				appRequest := (*appServer.Requests)[0]
				assert.Equal(t, "/some/data/app/url?foo=bar", appRequest.URL.String())
				assert.Equal(t, "admin@keboola.com", appRequest.Header.Get("X-Kbc-User-Email"))
				assert.Equal(t, "admin", appRequest.Header.Get("X-Kbc-User-Roles"))
			},
			expectedNotifications: map[string]int{
				"oidc": 1,
			},
		}
	}

	testCases = append(
		testCases,
		privateAppTestCaseFactory(http.MethodGet),
		privateAppTestCaseFactory(http.MethodPost),
		privateAppTestCaseFactory(http.MethodPut),
		privateAppTestCaseFactory(http.MethodPatch),
		privateAppTestCaseFactory(http.MethodDelete),
	)

	tmpDir := path.Join(os.Getenv("TEST_KBC_TMP_DIR"), "TestAppsProxyRouter") // nolint:forbidigo
	pm, _ := server.NewPortManager(t, tmpDir, "appsproxy")
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()

			// Create testing apps API
			appsAPI := testutil.StartDataAppsAPI(t, pm)
			t.Cleanup(func() {
				appsAPI.Close()
			})

			// Create testing app upstream
			appServer := testutil.StartAppServer(t, pm)
			t.Cleanup(func() {
				appServer.Close()
			})

			// Create OIDC providers and data apps before creating dependencies so that the
			// fake K8s client can be pre-populated with App CRD objects. This avoids a race
			// between the informer's initial list completing (HasSynced=true) and the watch
			// channel being established: objects created in that window are silently dropped
			// by the fake client, causing WaitForCacheSync to succeed but GetState to never
			// return the expected state.
			providers := testAuthProviders(t, pm)
			appURL := testutil.AppServerURL(t, appServer)
			apps := testDataApps(appURL, providers)
			appsAPI.Register(apps)

			// Create dependencies with K8s App CRD objects pre-populated in the fake client.
			// The informer picks them up during the initial list — no watch event needed.
			d, mocked := createDependencies(t, ctx, appsAPI.URL, makeDefaultK8sObjects(apps, appURL.String())...)

			// Test generated spans
			if tc.expectedSpans != nil {
				var opts []telemetry.TestSpanOption
				mocked.TestTelemetry().AssertSpans(t, tc.expectedSpans, opts...)
			}

			// Create proxy handler
			handler := createProxyHandler(ctx, d)

			// Create a test server for the proxy handler
			port := pm.GetFreePort()
			var lc net.ListenConfig
			l, err := lc.Listen(t.Context(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
			for err != nil {
				port = pm.GetFreePort()
				l, err = lc.Listen(t.Context(), "tcp", fmt.Sprintf("[::1]:%d", port))
			}

			proxySrv := &httptest.Server{
				Listener: l,
				Config:   &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ErrorLog: log.NewStdErrorLogger(d.Logger())},
			}
			proxySrv.StartTLS()
			t.Cleanup(func() {
				proxySrv.Close()
			})

			proxyURL, err := url.Parse(proxySrv.URL)
			require.NoError(t, err)

			client := createHTTPClient(t, proxyURL)

			// Wait for the informer to complete its initial list. Objects are already in the
			// fake client from pre-population above, so they are available immediately after sync.
			// Must be called before tc.setupK8s so per-test overrides (Patch) can override specific apps.
			registerDefaultK8sApps(t, d.AppStateWatcher())
			if tc.setupK8s != nil {
				tc.setupK8s(t, mocked.TestFakeK8sClient(), d.AppStateWatcher())
			}

			tc.run(t, client, providers, appServer, appsAPI, mocked.TestFakeK8sClient(), d.AppStateWatcher())

			d.Process().Shutdown(t.Context(), errors.New("bye bye"))
			d.Process().WaitForShutdown()

			assert.Equal(t, tc.expectedNotifications, appsAPI.Notifications)
			assert.Empty(t, mocked.DebugLogger().ErrorMessages())
		})
	}
}

func testAuthProviders(t *testing.T, pm server.PortManager) []*mockoidc.MockOIDC {
	t.Helper()

	oidc0 := testutil.StartOIDCProviderServer(t, pm)
	t.Cleanup(func() {
		require.NoError(t, oidc0.Shutdown())
	})

	oidc1 := testutil.StartOIDCProviderServer(t, pm)
	t.Cleanup(func() {
		require.NoError(t, oidc1.Shutdown())
	})

	return []*mockoidc.MockOIDC{oidc0, oidc1}
}

func testDataApps(upstream *url.URL, m []*mockoidc.MockOIDC) []api.AppConfig {
	return []api.AppConfig{
		{
			ID:             "norule",
			ProjectID:      "123",
			UpstreamAppURL: upstream.String(),
		},
		{
			ID:             "12345",
			ProjectID:      "123",
			Name:           "my-app-lowercase",
			AppSlug:        new("LOWERCASE"),
			UpstreamAppURL: upstream.String(),
			AuthRules: []api.Rule{
				{
					Type:         api.RulePathPrefix,
					Value:        "/",
					AuthRequired: new(false),
				},
			},
		},
		{
			ID:             "123",
			ProjectID:      "123",
			Name:           "my-app",
			AppSlug:        new("public"),
			UpstreamAppURL: upstream.String(),
			AuthRules: []api.Rule{
				{
					Type:         api.RulePathPrefix,
					Value:        "/",
					AuthRequired: new(false),
				},
			},
		},
		{
			ID:             "111",
			ProjectID:      "12345",
			Name:           "my-app-ws",
			AppSlug:        new("public"),
			UpstreamAppURL: upstream.String(),
			AuthRules: []api.Rule{
				{
					Type:         api.RulePathPrefix,
					Value:        "/",
					AuthRequired: new(false),
				},
			},
		},
		{
			ID:             "invalid1",
			ProjectID:      "123",
			UpstreamAppURL: upstream.String(),
			AuthRules: []api.Rule{
				{
					Type:  "unknown",
					Value: "/",
					Auth:  nil,
				},
			},
		},
		{
			ID:             "invalid2",
			ProjectID:      "123",
			UpstreamAppURL: upstream.String(),
			AuthRules: []api.Rule{
				{
					Type:         api.RulePathPrefix,
					Value:        "/",
					AuthRequired: new(true),
					Auth:         []provider.ID{"test"},
				},
			},
		},
		{
			ID:             "invalid3",
			ProjectID:      "123",
			UpstreamAppURL: upstream.String(),
			AuthProviders: provider.Providers{
				provider.OIDC{
					Base: provider.Base{
						Info: provider.Info{
							ID:   "oidc",
							Type: provider.TypeOIDC,
						},
					},
					ClientID:     m[0].Config().ClientID,
					ClientSecret: m[0].Config().ClientSecret,
					AllowedRoles: new([]string{}),
					IssuerURL:    m[0].Issuer(),
				},
			},
			AuthRules: []api.Rule{
				{
					Type:  api.RulePathPrefix,
					Value: "/",
					Auth:  []provider.ID{"oidc"},
				},
			},
		},
		{
			ID:             "invalid4",
			ProjectID:      "123",
			UpstreamAppURL: upstream.String(),
			AuthRules: []api.Rule{
				{
					Type:  api.RulePathPrefix,
					Value: "/",
					Auth:  []provider.ID{"unknown"},
				},
			},
		},
		{
			ID:             "invalid5",
			ProjectID:      "123",
			UpstreamAppURL: upstream.String(),
			AuthRules: []api.Rule{
				{
					Type:         api.RulePathPrefix,
					Value:        "/",
					AuthRequired: new(false),
					Auth:         []provider.ID{"test"},
				},
			},
		},
		{
			ID:             "oidc",
			ProjectID:      "123",
			UpstreamAppURL: upstream.String(),
			AuthProviders: provider.Providers{
				provider.OIDC{
					Base: provider.Base{
						Info: provider.Info{
							ID:   "oidc",
							Type: provider.TypeOIDC,
						},
					},
					ClientID:     m[0].Config().ClientID,
					ClientSecret: m[0].Config().ClientSecret,
					AllowedRoles: new([]string{"admin"}),
					IssuerURL:    m[0].Issuer(),
				},
			},
			AuthRules: []api.Rule{
				{
					Type:  api.RulePathPrefix,
					Value: "/",
					Auth:  []provider.ID{"oidc"},
				},
			},
		},
		{
			ID:             "multi",
			ProjectID:      "123",
			UpstreamAppURL: upstream.String(),
			AuthProviders: provider.Providers{
				provider.OIDC{
					Base: provider.Base{
						Info: provider.Info{
							ID:   "oidc0",
							Type: provider.TypeOIDC,
						},
					},
					ClientID:     m[0].Config().ClientID,
					ClientSecret: m[0].Config().ClientSecret,
					AllowedRoles: new([]string{"manager"}),
					IssuerURL:    m[0].Issuer(),
				},
				provider.OIDC{
					Base: provider.Base{
						Info: provider.Info{
							ID:   "oidc1",
							Type: provider.TypeOIDC,
						},
					},
					ClientID:     m[1].Config().ClientID,
					ClientSecret: m[1].Config().ClientSecret,
					AllowedRoles: new([]string{"admin"}),
					IssuerURL:    m[1].Issuer(),
				},
				provider.OIDC{
					Base: provider.Base{
						Info: provider.Info{
							ID:   "oidc2",
							Type: provider.TypeOIDC,
						},
					},
				},
			},
			AuthRules: []api.Rule{
				{
					Type:  api.RulePathPrefix,
					Value: "/",
					Auth:  []provider.ID{"oidc0", "oidc1", "oidc2"},
				},
			},
		},
		{
			ID:             "broken",
			ProjectID:      "123",
			UpstreamAppURL: upstream.String(),
			AuthProviders: provider.Providers{
				provider.OIDC{
					Base: provider.Base{
						Info: provider.Info{
							ID:   "oidc",
							Type: provider.TypeOIDC,
						},
					},
					ClientID:     "",
					ClientSecret: m[0].Config().ClientSecret,
					AllowedRoles: new([]string{"admin"}),
					IssuerURL:    m[0].Issuer(),
				},
			},
			AuthRules: []api.Rule{
				{
					Type:  api.RulePathPrefix,
					Value: "/",
					Auth:  []provider.ID{"oidc"},
				},
			},
		},
		{
			ID:             "prefix",
			ProjectID:      "123",
			UpstreamAppURL: upstream.String(),
			AuthProviders: provider.Providers{
				provider.OIDC{
					Base: provider.Base{
						Info: provider.Info{
							ID:   "oidc0",
							Type: provider.TypeOIDC,
						},
					},
					ClientID:     m[0].Config().ClientID,
					ClientSecret: m[0].Config().ClientSecret,
					AllowedRoles: new([]string{"admin"}),
					IssuerURL:    m[0].Issuer(),
				},
				provider.OIDC{
					Base: provider.Base{
						Info: provider.Info{
							ID:   "oidc1",
							Type: provider.TypeOIDC,
						},
					},
					ClientID:     m[1].Config().ClientID,
					ClientSecret: m[1].Config().ClientSecret,
					AllowedRoles: new([]string{"admin"}),
					IssuerURL:    m[1].Issuer(),
				},
			},
			AuthRules: []api.Rule{
				{
					Type:  api.RulePathPrefix,
					Value: "/api",
					Auth:  []provider.ID{"oidc0"},
				},
				{
					Type:  api.RulePathPrefix,
					Value: "/web",
					Auth:  []provider.ID{"oidc0", "oidc1"},
				},
				{
					Type:         api.RulePathPrefix,
					Value:        "/public",
					AuthRequired: new(false),
				},
			},
		},
		{
			ID:             "auth",
			ProjectID:      "123",
			UpstreamAppURL: upstream.String(),
			AppSlug:        new("basic"), // basic-auth.hub.keboola.local
			AuthProviders: provider.Providers{
				provider.Basic{
					Base: provider.Base{
						Info: provider.Info{
							ID:   "basic",
							Type: provider.TypeBasic,
						},
					},
					Password: "abc",
				},
			},
			AuthRules: []api.Rule{
				{
					Type:  api.RulePathPrefix,
					Value: "/",
					Auth:  []provider.ID{"basic"},
				},
			},
		},
		{
			// DEV mode test fixture: public app; spec.devMode.enabled is
			// flipped on the App CRD by the test setupK8s.
			ID:             "devmode",
			ProjectID:      "123",
			Name:           "dev-mode-app",
			AppSlug:        new("dev"), // dev-devmode.hub.keboola.local
			UpstreamAppURL: upstream.String(),
			AuthRules: []api.Rule{
				{
					Type:         api.RulePathPrefix,
					Value:        "/",
					AuthRequired: new(false),
				},
			},
		},
	}
}

func createDependencies(t *testing.T, ctx context.Context, sandboxesAPIURL string, initialK8sObjects ...runtime.Object) (proxyDependencies.ServiceScope, proxyDependencies.Mocked) {
	t.Helper()

	secret := make([]byte, 32)
	_, err := rand.Read(secret)
	require.NoError(t, err)

	csrfSecret := make([]byte, 32)
	_, err = rand.Read(csrfSecret)
	require.NoError(t, err)

	cfg := config.New()
	cfg.API.PublicURL, _ = url.Parse("https://hub.keboola.local")
	cfg.CookieSecretSalt = string(secret)
	cfg.CsrfTokenSalt = string(csrfSecret)
	cfg.SandboxesAPI.URL = sandboxesAPIURL
	return proxyDependencies.NewMockedServiceScopeWithK8sObjects(t, ctx, cfg, initialK8sObjects, dependencies.WithRealHTTPClient())
}

func createProxyHandler(ctx context.Context, d proxyDependencies.ServiceScope) http.Handler {
	loggerWriter := logging.NewLoggerWriter(d.Logger(), "info")
	logger.SetOutput(loggerWriter)
	// Cannot separate errors from info because ooidcproxy will override its error writer with either
	// the info writer or os.Stderr depending on Logging.ErrToInfo value whenever a new proxy instance is created.
	logger.SetErrOutput(loggerWriter)
	return proxy.NewHandler(ctx, d)
}

func createHTTPClient(t *testing.T, proxyURL *url.URL) *http.Client {
	t.Helper()

	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()

	// URLs sent to hub.keboola.local:443 are sent to the proxy server. This is necessary for subdomain matching.
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if strings.HasSuffix(addr, "hub.keboola.local:443") {
			addr = proxyURL.Host
		}
		return dialer.DialContext(ctx, network, addr)
	}

	// Disabled TLS verification since the certificate won't match due to the above change.
	transport.TLSClientConfig.InsecureSkipVerify = true

	// Create a cookie jar so that the client is aware of cookies.
	jar, err := cookiejar.New(nil)
	require.NoError(t, err)

	// Disable redirect following so that we can assert the target urls.
	checkRedirect := func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	return &http.Client{
		CheckRedirect: checkRedirect,
		Transport:     transport,
		Jar:           jar,
	}
}

func extractMetaRefreshTag(t *testing.T, body []byte) string {
	m := regexpcache.MustCompile(`<meta http-equiv="refresh" content="0;URL='(.+)'">`).FindStringSubmatch(string(body))
	require.NotNil(t, m)
	return html.UnescapeString(m[1])
}

func htmlLinkTo(url string) string {
	return fmt.Sprintf(`<a href="%s"`, html.EscapeString(url))
}

// registerDefaultK8sApps creates Running App CRD objects in the fake K8s client for all test apps
// and waits for the StateWatcher to sync them. The proxy reads appsProxy.upstreamUrl from the CRD
// to get the upstream URL, so this is required for requests to route to the upstream.
// Must be called before tc.setupK8s so per-test overrides (Patch) can override specific apps.
// registerDefaultK8sApps waits for the K8s informer cache to sync after the fake client has been
// pre-populated with App CRD objects via makeDefaultK8sObjects. Because the objects already exist
// in the fake client at informer startup, they are picked up during the initial list phase rather
// than relying on watch events. This avoids the race between HasSynced becoming true and the watch
// channel being established, which caused the fake client to silently drop creation events.
//
// Must be called before tc.setupK8s so per-test overrides (Patch) can override specific apps.
func registerDefaultK8sApps(t *testing.T, watcher *k8sapp.StateWatcher) {
	t.Helper()
	require.True(t, watcher.WaitForCacheSync(t.Context()), "App CRD informer cache sync timed out")
}

// TestKaiPreviewSlidingRefresh is a focused regression test for the sliding-refresh path in
// apphandler.serveHTTPOrError. It boots a dev-mode app via the standard harness but injects a
// FakeClock so that time can be advanced deterministically.
//
//   - At t+1h (before midpoint of 4h TTL) → no Set-Cookie on the response.
//   - At t+3h (past midpoint) → Set-Cookie: kbc-kai-preview-session=… appears and the new JWT
//     validates as fresh (NeedsRefresh==false immediately after issuance).
func TestKaiPreviewSlidingRefresh(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	const sessionTTL = 4 * time.Hour

	fakeClock := clockwork.NewFakeClock()

	// Create testing apps API and upstream.
	tmpDir := t.TempDir()
	pm, _ := server.NewPortManager(t, tmpDir, "appsproxy")
	appsAPI := testutil.StartDataAppsAPI(t, pm)
	t.Cleanup(func() { appsAPI.Close() })
	appServer := testutil.StartAppServer(t, pm)
	t.Cleanup(func() { appServer.Close() })

	providers := testAuthProviders(t, pm)
	appURL := testutil.AppServerURL(t, appServer)
	apps := testDataApps(appURL, providers)
	appsAPI.Register(apps)

	// Build the service scope with an injected FakeClock so the proxy sees our
	// controlled notion of "now".
	secret := make([]byte, 32)
	_, err := rand.Read(secret)
	require.NoError(t, err)
	csrfSecret := make([]byte, 32)
	_, err = rand.Read(csrfSecret)
	require.NoError(t, err)

	cfg := config.New()
	cfg.API.PublicURL, _ = url.Parse("https://hub.keboola.local")
	cfg.CookieSecretSalt = string(secret)
	cfg.CsrfTokenSalt = string(csrfSecret)
	cfg.SandboxesAPI.URL = appsAPI.URL
	cfg.KaiPreview.SessionTTL = sessionTTL

	d, mocked := proxyDependencies.NewMockedServiceScopeWithK8sObjects(
		t, ctx, cfg,
		makeDefaultK8sObjects(apps, appURL.String()),
		dependencies.WithRealHTTPClient(),
		dependencies.WithClock(fakeClock),
	)
	defer func() {
		d.Process().Shutdown(ctx, errors.New("bye bye"))
		d.Process().WaitForShutdown()
	}()

	// Wire logging the same way as createProxyHandler.
	loggerWriter := logging.NewLoggerWriter(d.Logger(), "info")
	logger.SetOutput(loggerWriter)
	logger.SetErrOutput(loggerWriter)
	handler := proxy.NewHandler(ctx, d)

	// Boot a TLS proxy server. Bind to :0 to let the OS assign a free port atomically,
	// eliminating the TOCTOU race that occurs when GetFreePort() releases the port before Listen().
	var lc net.ListenConfig
	l, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	proxySrv := &httptest.Server{
		Listener: l,
		Config:   &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ErrorLog: log.NewStdErrorLogger(d.Logger())},
	}
	proxySrv.StartTLS()
	t.Cleanup(func() { proxySrv.Close() })

	proxyURL, err := url.Parse(proxySrv.URL)
	require.NoError(t, err)
	client := createHTTPClient(t, proxyURL)

	// Wait for K8s informer to sync, then enable DevMode=Running for the "devmode" app.
	registerDefaultK8sApps(t, d.AppStateWatcher())
	patch := []byte(`{"spec":{"devMode":{"enabled":true}},"status":{"currentState":"Running"}}`)
	_, err = mocked.TestFakeK8sClient().Resource(k8sapp.AppGVR()).Namespace("keboola").Patch(
		ctx, "app-devmode", k8stypes.MergePatchType, patch, metav1.PatchOptions{},
	)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		info, ok := d.AppStateWatcher().GetState(ctx, api.AppID("devmode"))
		return ok && info.DevMode && info.ActualState == k8sapp.AppActualStateRunning
	}, 5*time.Second, 50*time.Millisecond)

	// Retrieve the session signing key that the mocked scope auto-filled.
	sessionKey := mocked.TestConfig().KaiPreview.SessionSigningKey

	// Mint a session JWT anchored at FakeClock's current time (t=0).
	rawJWT, err := kaipreview.MintSessionJWT(sessionKey, fakeClock, "devmode", "123", sessionTTL)
	require.NoError(t, err)

	sendRequest := func(t *testing.T) *http.Response {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://dev-devmode.hub.keboola.local/", nil)
		require.NoError(t, err)
		req.AddCookie(&http.Cookie{Name: kaipreview.SessionCookieName, Value: rawJWT})
		resp, err := client.Do(req)
		require.NoError(t, err)
		return resp
	}

	// --- t+1h: before midpoint — no refresh expected ---
	fakeClock.Advance(1 * time.Hour)
	resp := sendRequest(t)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	var setCookieHeader string
	for _, c := range resp.Cookies() {
		if c.Name == kaipreview.SessionCookieName {
			setCookieHeader = c.Value
		}
	}
	assert.Empty(t, setCookieHeader, "expected no Set-Cookie at t+1h (before midpoint)")

	// --- t+3h: past midpoint (3h > 4h/2) — refresh expected ---
	fakeClock.Advance(2 * time.Hour) // total 3h elapsed
	resp = sendRequest(t)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	var refreshedJWT string
	for _, c := range resp.Cookies() {
		if c.Name == kaipreview.SessionCookieName {
			refreshedJWT = c.Value
		}
	}
	require.NotEmpty(t, refreshedJWT, "expected Set-Cookie with refreshed JWT at t+3h")

	// Verify the new JWT is valid and immediately fresh (NeedsRefresh should be false right now).
	newClaims, err := kaipreview.VerifySessionJWT(sessionKey, fakeClock, refreshedJWT)
	require.NoError(t, err)
	assert.Equal(t, "devmode", newClaims.AppID)
	assert.Equal(t, "123", newClaims.ProjectID)
	assert.False(t, newClaims.NeedsRefresh(fakeClock.Now()), "newly minted JWT should not need refresh immediately")

	assert.Empty(t, mocked.DebugLogger().ErrorMessages())
}

// TestWebsocketActivityTracking verifies the per-frame WebSocket activity-tracking
// behavior introduced by wsactivity:
//
//  1. The initial handshake produces exactly one Sandboxes Service notify (from the
//     GotConn ClientTrace hook).
//  2. An idle WebSocket — connection open, no data frames exchanged — does NOT
//     spontaneously increment the notify count past the throttle window. The
//     customer-visible goal of the change (forgotten browser tab no longer blocks
//     auto-suspend) is that an idle connection produces zero activity-driven
//     notifies; this test confirms that for the per-frame path.
//  3. A real data frame past the throttle window MUST produce a second notify,
//     proving the per-frame path is wired end-to-end through the wrapped conn.
//
// The test uses an injected FakeClock so the notify throttle can be advanced
// deterministically without sleeping for 30 s of real time.
//
// Caveat on (2): this is not a hard regression test against re-introducing a real-
// time ticker (the previous implementation slept 30 s of wall-clock time). The
// 250 ms wall-clock yield only catches accidentally short-period real-time goroutines;
// a future 30 s ticker would still pass this test. Catching that regression
// reliably would require making the ticker interval injectable. Code review is
// expected to catch any reintroduction of presence-based polling.
//
// The "control frames are ignored" property is covered separately by
// wsactivity's TestWrap_OnlyControlFrames_NoCallbacks unit test, which can do
// the assertion without driving ping/pong through coder/websocket's read loop.
func TestWebsocketActivityTracking(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	fakeClock := clockwork.NewFakeClock()

	// Standard testing scaffold: apps API + upstream app server + secrets + config.
	tmpDir := t.TempDir()
	pm, _ := server.NewPortManager(t, tmpDir, "appsproxy")
	appsAPI := testutil.StartDataAppsAPI(t, pm)
	t.Cleanup(func() { appsAPI.Close() })
	appServer := testutil.StartAppServer(t, pm)
	t.Cleanup(func() { appServer.Close() })

	providers := testAuthProviders(t, pm)
	appURL := testutil.AppServerURL(t, appServer)
	apps := testDataApps(appURL, providers)
	appsAPI.Register(apps)

	secret := make([]byte, 32)
	_, err := rand.Read(secret)
	require.NoError(t, err)
	csrfSecret := make([]byte, 32)
	_, err = rand.Read(csrfSecret)
	require.NoError(t, err)

	cfg := config.New()
	cfg.API.PublicURL, _ = url.Parse("https://hub.keboola.local")
	cfg.CookieSecretSalt = string(secret)
	cfg.CsrfTokenSalt = string(csrfSecret)
	cfg.SandboxesAPI.URL = appsAPI.URL

	d, mocked := proxyDependencies.NewMockedServiceScopeWithK8sObjects(
		t, ctx, cfg,
		makeDefaultK8sObjects(apps, appURL.String()),
		dependencies.WithRealHTTPClient(),
		dependencies.WithClock(fakeClock),
	)
	defer func() {
		d.Process().Shutdown(ctx, errors.New("bye bye"))
		d.Process().WaitForShutdown()
	}()

	loggerWriter := logging.NewLoggerWriter(d.Logger(), "info")
	logger.SetOutput(loggerWriter)
	logger.SetErrOutput(loggerWriter)
	handler := proxy.NewHandler(ctx, d)

	var lc net.ListenConfig
	l, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	proxySrv := &httptest.Server{
		Listener: l,
		Config:   &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ErrorLog: log.NewStdErrorLogger(d.Logger())},
	}
	proxySrv.StartTLS()
	t.Cleanup(func() { proxySrv.Close() })

	proxyURL, err := url.Parse(proxySrv.URL)
	require.NoError(t, err)
	client := createHTTPClient(t, proxyURL)
	registerDefaultK8sApps(t, d.AppStateWatcher())

	// notifyCount is locked behind the testutil mutex so it is safe to read
	// concurrently with the in-flight notify goroutine driven by data frames.
	notifyCount := func() int { return appsAPI.NotificationsCount("123") }

	wsCtx, wsCancel := context.WithTimeout(ctx, time.Minute)
	defer wsCancel()

	// Open the idle WebSocket. The handshake's GotConn ClientTrace hook fires
	// u.notify once, producing the first Sandboxes Service PATCH call.
	c, _, err := websocket.Dial(
		wsCtx,
		"wss://public-123.hub.keboola.local/ws-idle",
		&websocket.DialOptions{HTTPClient: client},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close(websocket.StatusNormalClosure, "") })

	require.Eventually(t, func() bool { return notifyCount() == 1 }, 5*time.Second, 10*time.Millisecond,
		"handshake should produce exactly one notify (from GotConn)")

	// Advance the fake clock past the per-app throttle window. From now on,
	// any activity-driven notify will pass the throttle gate. With no
	// frames flowing through the wrapper, the count must stay at 1.
	fakeClock.Advance(31 * time.Second)
	// Brief wall-clock yield so that any short-period goroutine that mistakenly
	// fired on connection presence (rather than per-frame) would surface. See
	// the doc comment caveat — this does NOT catch a 30 s real-time ticker.
	time.Sleep(250 * time.Millisecond)
	assert.Equal(t, 1, notifyCount(),
		"idle WS past the throttle window must NOT trigger an activity-driven notify")

	// Write a real data frame (text message). The proxy observes a non-control
	// opcode (0x1) in the client→server direction, fires u.notify, and — since
	// fakeClock is past the throttle window — the throttle gate opens and we
	// get a second PATCH call to apps API.
	require.NoError(t, c.Write(wsCtx, websocket.MessageText, []byte("hello data")))

	require.Eventually(t, func() bool { return notifyCount() == 2 }, 5*time.Second, 10*time.Millisecond,
		"data-frame activity past the throttle window must produce a second notify")

	assert.Empty(t, mocked.DebugLogger().ErrorMessages())
}

// makeDefaultK8sObjects converts a slice of app configs into K8s unstructured App CRD objects
// with Running state and the given upstream URL. Pass the result to NewMockedServiceScopeWithK8sObjects
// so the fake client is pre-populated before the informer starts.
func makeDefaultK8sObjects(apps []api.AppConfig, serviceURL string) []runtime.Object {
	objects := make([]runtime.Object, 0, len(apps))
	for _, app := range apps {
		objects = append(objects, &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": k8sapp.Group + "/" + k8sapp.Version,
				"kind":       "App",
				"metadata": map[string]any{
					"name":      "app-" + string(app.ID),
					"namespace": "keboola",
				},
				"spec": map[string]any{
					"appId": string(app.ID),
				},
				"status": map[string]any{
					"currentState": string(k8sapp.AppActualStateRunning),
					"appsProxy": map[string]any{
						"upstreamUrl": serviceURL,
					},
				},
			},
		})
	}
	return objects
}
