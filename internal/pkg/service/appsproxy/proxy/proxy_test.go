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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/miekg/dns"
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
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	proxyDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/oidcproxy/logging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/testutil"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/transport/dns/dnsmock"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type testCase struct {
	name                  string
	run                   func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server)
	expectedNotifications map[string]int
	expectedWakeUps       map[string]int
	expectedSpans         tracetest.SpanStubs
}

func TestAppProxyRouter(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			name: "health-check",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Request to health-check endpoint
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://hub.keboola.local/health-check", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Equal(t, "OK\n", string(body))
			},
			expectedNotifications: map[string]int{},
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "missing-app-id",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Request without app id
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusBadRequest, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Unexpected domain, missing application ID.")
			},
			expectedNotifications: map[string]int{},
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "unknown-app-id",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Request to unknown app
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://unknown.hub.keboola.local/health-check", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusNotFound, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), html.EscapeString(`Application "unknown" not found in the stack.`))
			},
			expectedNotifications: map[string]int{},
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "broken-app",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Request to broken app
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://broken.hub.keboola.local/", nil)
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
			expectedWakeUps:       map[string]int{},
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
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Request to app with no path rules
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://norule.hub.keboola.local/", nil)
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
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "wrong-rule-type-app",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Request to app with invalid rule type
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://invalid1.hub.keboola.local/", nil)
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
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "missing-referenced-provider",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Request to app with invalid rule type
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://invalid2.hub.keboola.local/", nil)
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
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "empty-allowed-roles-array-app",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Request to app with invalid rule type
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://invalid3.hub.keboola.local/", nil)
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
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "unknown-provider-app",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Request to app with unknown provider
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://invalid4.hub.keboola.local/", nil)
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
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "not-empty-providers-auth-required-false",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Request to app with invalid rule type
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://invalid5.hub.keboola.local/", nil)
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
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "redirect-to-canonical-host",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Redirect to the canonical URL (match cookies domain)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://foo-bar-123.hub.keboola.local/some/data/app/url?foo=bar", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusPermanentRedirect, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Equal(t, "https://public-123.hub.keboola.local/some/data/app/url?foo=bar", location)
			},
			expectedNotifications: map[string]int{},
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "redirect-to-host-lowercase",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://foo-12345.hub.keboola.local/some/data/app/url?foo=bar", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusPermanentRedirect, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Equal(t, "https://lowercase-12345.hub.keboola.local/some/data/app/url?foo=bar", location)
			},
			expectedNotifications: map[string]int{},
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "public-app-down",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				appServer.Close()

				// Request to public app
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusBadGateway, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), `Request to application failed.`)
			},
			expectedNotifications: map[string]int{},
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "public-app-sub-url",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Request to public app
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://public-123.hub.keboola.local/some/data/app/url?foo=bar", nil)
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
				assert.Equal(t, "", appRequest.Header.Get("X-Kbc-Test"))
				assert.Equal(t, "", appRequest.Header.Get("X-Kbc-User-Email"))
			},
			expectedNotifications: map[string]int{
				"123": 1,
			},
			expectedWakeUps: map[string]int{},
		},
		{
			name: "private-app-verified-email",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:         "admin@keboola.com",
					EmailVerified: ptr.Ptr(true),
					Groups:        []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Retry with provider cookie
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://oidc.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Request to private app (authorized)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)

				// Request to sign out url
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/_proxy/sign_out", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				cookies := response.Cookies()
				if assert.Len(t, cookies, 2) {
					assert.Equal(t, "_oauth2_provider", cookies[0].Name)
					assert.Equal(t, "", cookies[0].Value)
					assert.Equal(t, "/", cookies[0].Path)
					assert.Equal(t, "oidc.hub.keboola.local", cookies[0].Domain)
					assert.True(t, cookies[0].HttpOnly)
					assert.True(t, cookies[0].Secure)
					assert.Equal(t, http.SameSiteStrictMode, cookies[0].SameSite)

					assert.Equal(t, "_oauth2_proxy", cookies[1].Name)
					assert.Equal(t, "", cookies[1].Value)
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
			expectedWakeUps: map[string]int{},
		},
		{
			name: "private-app-unauthorized",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
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
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusUnauthorized, response.StatusCode)

				// Request to private app (still unauthorized because login failed)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
			expectedNotifications: map[string]int{},
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "private-missing-csrf-token",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "/oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
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
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback (fails because of missing CSRF token)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)
				wildcards.Assert(t, "%ALogin Failed: Unable to find a valid CSRF token. Please try again.%A", string(body))

				// Request to private app
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
			expectedNotifications: map[string]int{},
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "private-app-group-mismatch",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:  "manager@keboola.com",
					Groups: []string{"manager"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "/oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://oidc.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback (fails because of missing group) - meta tag redirect
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)
				wildcards.Assert(t, "%AYou do not have permission to access this resource.%A", string(body))

				// Request to private app
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
			expectedNotifications: map[string]int{},
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "private-app-unverified-email",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:         "admin@keboola.com",
					EmailVerified: ptr.Ptr(false),
					Groups:        []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "/oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://oidc.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback (fails because of unverified email) - meta tag redirect
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusInternalServerError, response.StatusCode)

				// Request to private app
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
			expectedNotifications: map[string]int{},
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "private-app-oidc-down",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
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
				assert.NoError(t, m[0].Shutdown())

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				_, err = client.Do(request)
				require.Error(t, err)
				require.Contains(t, err.Error(), "refused")
				var syscallError *os.SyscallError
				errors.As(err, &syscallError)
				require.Contains(t, syscallError.Syscall, "connect")

				// Request to private app (unauthorized)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
			expectedNotifications: map[string]int{},
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "private-app-down",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				appServer.Close()

				m[0].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
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
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://oidc.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				cookies = response.Cookies()
				if assert.Len(t, cookies, 2) {
					assert.Equal(t, "_oauth2_proxy_csrf", cookies[0].Name)
					assert.Equal(t, "", cookies[0].Value)
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
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusBadGateway, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), `Request to application failed.`)
			},
			expectedNotifications: map[string]int{},
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "multi-app-basic-flow",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				m[1].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin", "manager"},
				})

				// Request to private app, unauthorized - selector page
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/some/path", nil)
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
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/_proxy/selection?provider=oidc1&rd=%2Fsome%2Fpath", nil)
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
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
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
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://multi.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				location = response.Header.Get("Location")
				assert.Equal(t, "/some/path", location)

				// Request to private app (authorized)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/some/data/app/url?foo=bar", nil)
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
				assert.Equal(t, "", appRequest.Header.Get("X-Kbc-Test"))
			},
			expectedNotifications: map[string]int{
				"multi": 1,
			},
			expectedWakeUps: map[string]int{},
		},
		{
			name: "multi-app-redirect-to-selection-page",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				m[1].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Provider selection
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/_proxy/selection?provider=oidc1", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Equal(t, "https://multi.hub.keboola.local/_proxy/sign_in", location)

				// Request to app - unauthorized - redirect to the selector page
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
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
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "multi-app-unverified-email",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				m[1].QueueUser(&mockoidc.MockUser{
					Email:         "admin@keboola.com",
					EmailVerified: ptr.Ptr(false),
					Groups:        []string{"admin"},
				})

				// Provider selection
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/_proxy/selection?provider=oidc1", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "https://multi.hub.keboola.local/_proxy/sign_in")

				// Redirect to the provider sing in page
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "oidc/authorize?client_id=")

				// Request to private app (unauthorized)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusUnauthorized, response.StatusCode)

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://multi.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback (fails because of unverified email)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusInternalServerError, response.StatusCode)

				// Request to private app - unauthorized - selection page
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusUnauthorized, response.StatusCode)
			},
			expectedNotifications: map[string]int{},
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "multi-app-down",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				appServer.Close()

				m[1].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app, unauthorized - selector page
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
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
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/_proxy/selection?provider=oidc1", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "https://multi.hub.keboola.local/_proxy/sign_in")

				// Redirect to the provider sing in page
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://multi.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Request to private app (authorized but down)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusBadGateway, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), `Request to application failed.`)
			},
			expectedNotifications: map[string]int{},
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "multi-app-broken-provider",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				appServer.Close()

				// Provider selection
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/_proxy/selection?provider=oidc2", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Equal(t, "https://multi.hub.keboola.local/error", location)

				// Follow redirect to sign in page
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
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
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "public-app-websocket",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()

				c, _, err := websocket.Dial(
					ctx,
					"wss://public-123.hub.keboola.local/ws",
					&websocket.DialOptions{
						HTTPClient: client,
					},
				)
				require.NoError(t, err)

				var v interface{}
				err = wsjson.Read(ctx, c, &v)
				require.NoError(t, err)

				assert.Equal(t, "Hello websocket", v)

				assert.NoError(t, c.Close(websocket.StatusNormalClosure, ""))
			},
			expectedNotifications: map[string]int{
				"123": 1,
			},
			expectedWakeUps: map[string]int{},
		},
		{
			name: "private-app-websocket-unauthorized",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
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
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "private-app-websocket",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:         "admin@keboola.com",
					EmailVerified: ptr.Ptr(true),
					Groups:        []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "/oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://oidc.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Websocket request
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "wss://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)

				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
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

				var v interface{}
				err = wsjson.Read(ctx, c, &v)
				require.NoError(t, err)

				assert.Equal(t, "Hello websocket", v)

				assert.NoError(t, c.Close(websocket.StatusNormalClosure, ""))
			},
			expectedNotifications: map[string]int{
				"oidc": 1,
			},
			expectedWakeUps: map[string]int{},
		},
		{
			name: "multi-app-websocket",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				m[1].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app, unauthorized - selector page
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
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
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/_proxy/selection?provider=oidc1", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "https://multi.hub.keboola.local/_proxy/sign_in")

				// Redirect to the provider sing in page
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://multi.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Websocket request
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "wss://multi.hub.keboola.local/", nil)
				require.NoError(t, err)

				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
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

				var v interface{}
				err = wsjson.Read(ctx, c, &v)
				require.NoError(t, err)

				assert.Equal(t, "Hello websocket", v)

				assert.NoError(t, c.Close(websocket.StatusNormalClosure, ""))
			},
			expectedNotifications: map[string]int{
				"multi": 1,
			},
			expectedWakeUps: map[string]int{},
		},
		{
			name: "prefix-app-no-auth",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Request to public part of the app
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://prefix.hub.keboola.local/public", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)

				// Request to api unauthorized - redirect to the sign in page, there is only one provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://prefix.hub.keboola.local/api", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Request to web - unauthorized - selection page, there are two providers
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://prefix.hub.keboola.local/web", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusUnauthorized, response.StatusCode)

				// Request to web (no matching prefix)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://prefix.hub.keboola.local/unknown", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusNotFound, response.StatusCode)
			},
			expectedNotifications: map[string]int{
				"prefix": 1,
			},
			expectedWakeUps: map[string]int{},
		},
		{
			name: "prefix-app-api-auth",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private part (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://prefix.hub.keboola.local/api", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "/oidc/authorize?client_id=")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://prefix.hub.keboola.local/_proxy/callback")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Equal(t, location, "/api")

				// Request to private part (authorized)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://prefix.hub.keboola.local/api", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)

				// Since the provider is configured for both /api and /web this works as well.
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://prefix.hub.keboola.local/web", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
			},
			expectedNotifications: map[string]int{
				"prefix": 1,
			},
			expectedWakeUps: map[string]int{},
		},
		{
			name: "prefix-app-web-auth",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				m[1].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private part, unauthorized - selector page
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://prefix.hub.keboola.local/web", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusUnauthorized, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), htmlLinkTo(`https://prefix.hub.keboola.local/_proxy/selection?provider=oidc0&rd=%2Fweb`))
				assert.Contains(t, string(body), htmlLinkTo(`https://prefix.hub.keboola.local/_proxy/selection?provider=oidc1&rd=%2Fweb`))

				// Provider selection
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://prefix.hub.keboola.local/_proxy/selection?provider=oidc1&rd=%2Fweb", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "https://prefix.hub.keboola.local/_proxy/sign_in")

				// Redirect to the provider sing in page
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://prefix.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Request to private part (authorized)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://prefix.hub.keboola.local/web", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)

				// Since the provider is configured only for web, this needs to fail.
				// !! In order for this to fail it is necessary for each provider to use a different cookie secret.
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://prefix.hub.keboola.local/api", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
			expectedNotifications: map[string]int{
				"prefix": 1,
			},
			expectedWakeUps: map[string]int{},
		},
		{
			name: "shared-provider",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				m[1].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Provider selection
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://prefix.hub.keboola.local/_proxy/selection?provider=oidc1", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "https://prefix.hub.keboola.local/_proxy/sign_in")

				// Redirect to the provider sing in page
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://prefix.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Request to private part (authorized)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://prefix.hub.keboola.local/web", nil)
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
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusUnauthorized, response.StatusCode)
			},
			expectedNotifications: map[string]int{
				"prefix": 1,
			},
			expectedWakeUps: map[string]int{},
		},
		{
			name: "configuration-change",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Request to public app
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
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
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Revert configuration
				service.Apps["123"] = originalConfig

				// Request to public app
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
			},
			expectedNotifications: map[string]int{
				"123": 1,
			},
			expectedWakeUps: map[string]int{},
		},
		{
			name: "concurrency-test",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				wg := sync.WaitGroup{}
				counter := atomic.NewInt64(0)
				for i := 0; i < 100; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()

						m[0].QueueUser(&mockoidc.MockUser{
							Email:         "admin@keboola.com",
							EmailVerified: ptr.Ptr(true),
							Groups:        []string{"admin"},
						})

						request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://oidc.hub.keboola.local/foo/bar", nil)
						assert.NoError(t, err)

						response, err := client.Do(request)
						if assert.NoError(t, err) {
							if assert.Equal(t, http.StatusFound, response.StatusCode) {
								counter.Add(1)
							}
						}
					}()
				}

				// Wait for all requests
				wg.Wait()

				// Check total requests count
				assert.Equal(t, int64(100), counter.Load())
			},
			expectedNotifications: map[string]int{},
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "public-app-wakeup",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				dnsServer.RemoveARecords(dns.Fqdn("app.local"))

				// Request to public app - fails because the app doesn't have a DNS record
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)

				dnsServer.AddARecord(dns.Fqdn("app.local"), net.ParseIP("127.0.0.1"))

				// Request to public app
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
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
			expectedWakeUps: map[string]int{
				"123": 1,
			},
		},
		{
			name: "public-app-wakeup-only",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				dnsServer.RemoveARecords(dns.Fqdn("app.local"))

				// Request to public app - fails because the app doesn't have a DNS record
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
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
			expectedWakeUps: map[string]int{
				"123": 1,
			},
		},
		{
			name: "private-one-provider-selector",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Request provider selector page - no auth provider
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/_proxy/selection", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), htmlLinkTo(`https://oidc.hub.keboola.local/_proxy/selection?provider=oidc`))
			},
			expectedNotifications: map[string]int{},
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "private-app-wakeup",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				dnsServer.RemoveARecords(dns.Fqdn("app.local"))

				m[0].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Retry with provider cookie
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://oidc.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Request to private app (authorized but missing dns, triggers wakeup)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)

				dnsServer.AddARecord(dns.Fqdn("app.local"), net.ParseIP("127.0.0.1"))

				// Request to private app (authorized)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
			},
			expectedNotifications: map[string]int{
				"oidc": 1,
			},
			expectedWakeUps: map[string]int{
				"oidc": 1,
			},
		},
		{
			name: "private-app-wakeup-only",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				dnsServer.RemoveARecords(dns.Fqdn("app.local"))

				m[0].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Retry with provider cookie
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://oidc.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Request to private app (authorized but missing dns, triggers wakeup)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
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
			expectedWakeUps: map[string]int{
				"oidc": 1,
			},
		},
		{
			name: "private-app-no-wakeup",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				dnsServer.RemoveARecords(dns.Fqdn("app.local"))

				m[0].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Retry with provider cookie
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "oidc/authorize?client_id=")

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://oidc.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Expect no notification or wakeup because there was never an authorized request to the app
			},
			expectedNotifications: map[string]int{},
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "public-basic-auth-wrong-login",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Request public basic auth app
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://basic-auth.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Basic Authentication")

				// Fill wrong password into form
				request, err = http.NewRequestWithContext(context.Background(), http.MethodPost, "https://basic-auth.hub.keboola.local/", bytes.NewBuffer([]byte("password=def")))
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
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "public-basic-auth-correct-login",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Request public basic auth app
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://basic-auth.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Basic Authentication")

				// Fill correct password into form
				request, err = http.NewRequestWithContext(context.Background(), http.MethodPost, "https://basic-auth.hub.keboola.local/", bytes.NewBuffer([]byte("password=abc")))
				request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Hello, client")
				// Check that cookies were set
				cookies := response.Cookies()
				if assert.Len(t, cookies, 1) {
					assert.Equal(t, "proxyBasicAuth", cookies[0].Name)
					assert.Equal(t, "bfd0255218cceb44fec106d13875be3b7120b304b97df9bfccbeb9aab19019fa", cookies[0].Value)
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
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
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
			expectedWakeUps: map[string]int{},
		},
		{
			name: "public-basic-auth-wrong-cookie",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Access with cookie
				request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://basic-auth.hub.keboola.local/", nil)
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
			expectedWakeUps:       map[string]int{},
		},
		{
			name: "public-basic-auth-cookie",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Access with cookie
				request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://basic-auth.hub.keboola.local/", nil)
				request.AddCookie(&http.Cookie{Name: "proxyBasicAuth", Value: "bfd0255218cceb44fec106d13875be3b7120b304b97df9bfccbeb9aab19019fa"})
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
			expectedWakeUps: map[string]int{},
		},
		{
			name: "public-basic-auth-sign-out",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Access with cookie
				request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://basic-auth.hub.keboola.local/_proxy/sign_out", nil)
				request.AddCookie(&http.Cookie{Name: "proxyBasicAuth", Value: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"})
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				location := response.Header.Get("Location")
				assert.Contains(t, location, "https://basic-auth.hub.keboola.local/")

				// Request to proxy location
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				body, err = io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "Basic Authentication")
				require.Len(t, response.Cookies(), 0)
			},
			expectedNotifications: map[string]int{},
			expectedWakeUps:       map[string]int{},
		},
	}

	publicAppTestCaseFactory := func(method string) testCase {
		return testCase{
			name: "public-app-" + method,
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				// Request to public app
				request, err := http.NewRequestWithContext(context.Background(), method, "https://public-123.hub.keboola.local/", nil)
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
			expectedWakeUps: map[string]int{},
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
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *testutil.AppServer, service *testutil.DataAppsAPI, dnsServer *dnsmock.Server) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), method, "https://oidc.hub.keboola.local/", nil)
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
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header.Get("Location")
				assert.Contains(t, location, "https://oidc.hub.keboola.local/_proxy/callback?")

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				// Request to proxy callback - meta tag redirect
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, extractMetaRefreshTag(t, body), nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				cookies = response.Cookies()
				if assert.Len(t, cookies, 2) {
					assert.Equal(t, "_oauth2_proxy_csrf", cookies[0].Name)
					assert.Equal(t, "", cookies[0].Value)
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
				request, err = http.NewRequestWithContext(context.Background(), method, "https://oidc.hub.keboola.local/some/data/app/url?foo=bar", nil)
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
			expectedWakeUps: map[string]int{},
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

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Create testing apps API
			appsAPI := testutil.StartDataAppsAPI(t)
			t.Cleanup(func() {
				appsAPI.Close()
			})

			// Create testing app upstream
			appServer := testutil.StartAppServer(t)
			t.Cleanup(func() {
				appServer.Close()
			})

			// Create dependencies
			d, mocked := createDependencies(t, appsAPI.URL)

			// Test generated spans
			if tc.expectedSpans != nil {
				var opts []telemetry.TestSpanOption
				mocked.TestTelemetry().AssertSpans(t, tc.expectedSpans, opts...)
			}

			dnsServer := mocked.TestDNSServer()

			// Create test OIDC providers
			providers := testAuthProviders(t)

			// Register data apps
			appURL := testutil.AddAppDNSRecord(t, appServer, dnsServer)
			appsAPI.Register(testDataApps(appURL, providers))

			// Create proxy handler
			handler := createProxyHandler(d)

			// Create a test server for the proxy handler
			proxySrv := httptest.NewUnstartedServer(handler)
			proxySrv.Config.ErrorLog = log.NewStdErrorLogger(d.Logger())
			proxySrv.StartTLS()
			t.Cleanup(func() {
				proxySrv.Close()
			})

			proxyURL, err := url.Parse(proxySrv.URL)
			require.NoError(t, err)

			client := createHTTPClient(t, proxyURL)

			tc.run(t, client, providers, appServer, appsAPI, dnsServer)

			d.Process().Shutdown(context.Background(), errors.New("bye bye"))
			d.Process().WaitForShutdown()

			assert.Equal(t, tc.expectedNotifications, appsAPI.Notifications)
			assert.Equal(t, tc.expectedWakeUps, appsAPI.WakeUps)
			assert.Equal(t, "", mocked.DebugLogger().ErrorMessages())
		})
	}
}

func testAuthProviders(t *testing.T) []*mockoidc.MockOIDC {
	t.Helper()

	oidc0 := testutil.StartOIDCProviderServer(t)
	t.Cleanup(func() {
		assert.NoError(t, oidc0.Shutdown())
	})

	oidc1 := testutil.StartOIDCProviderServer(t)
	t.Cleanup(func() {
		assert.NoError(t, oidc1.Shutdown())
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
			AppSlug:        ptr.Ptr("LOWERCASE"),
			UpstreamAppURL: upstream.String(),
			AuthRules: []api.Rule{
				{
					Type:         api.RulePathPrefix,
					Value:        "/",
					AuthRequired: ptr.Ptr(false),
				},
			},
		},
		{
			ID:             "123",
			ProjectID:      "123",
			Name:           "my-app",
			AppSlug:        ptr.Ptr("public"),
			UpstreamAppURL: upstream.String(),
			AuthRules: []api.Rule{
				{
					Type:         api.RulePathPrefix,
					Value:        "/",
					AuthRequired: ptr.Ptr(false),
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
					AuthRequired: ptr.Ptr(true),
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
					AllowedRoles: ptr.Ptr([]string{}),
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
					AuthRequired: ptr.Ptr(false),
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
					AllowedRoles: ptr.Ptr([]string{"admin"}),
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
					AllowedRoles: ptr.Ptr([]string{"manager"}),
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
					AllowedRoles: ptr.Ptr([]string{"admin"}),
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
					AllowedRoles: ptr.Ptr([]string{"admin"}),
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
					AllowedRoles: ptr.Ptr([]string{"admin"}),
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
					AllowedRoles: ptr.Ptr([]string{"admin"}),
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
					AuthRequired: ptr.Ptr(false),
				},
			},
		},
		{
			ID:             "auth",
			ProjectID:      "123",
			UpstreamAppURL: upstream.String(),
			AppSlug:        ptr.Ptr("basic"), // basic-auth.hub.keboola.local
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
	}
}

func createDependencies(t *testing.T, sandboxesAPIURL string) (proxyDependencies.ServiceScope, proxyDependencies.Mocked) {
	t.Helper()

	secret := make([]byte, 32)
	_, err := rand.Read(secret)
	require.NoError(t, err)

	cfg := config.New()
	cfg.API.PublicURL, _ = url.Parse("https://hub.keboola.local")
	cfg.CookieSecretSalt = string(secret)
	cfg.SandboxesAPI.URL = sandboxesAPIURL

	return proxyDependencies.NewMockedServiceScope(t, cfg, dependencies.WithRealHTTPClient())
}

func createProxyHandler(d proxyDependencies.ServiceScope) http.Handler {
	loggerWriter := logging.NewLoggerWriter(d.Logger(), "info")
	logger.SetOutput(loggerWriter)
	// Cannot separate errors from info because ooidcproxy will override its error writer with either
	// the info writer or os.Stderr depending on Logging.ErrToInfo value whenever a new proxy instance is created.
	logger.SetErrOutput(loggerWriter)
	return proxy.NewHandler(d)
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
