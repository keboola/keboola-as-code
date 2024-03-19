// nolint: thelper // because it wants the run functions to start with t.Helper()
package http

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/oauth2-proxy/mockoidc"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/appconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	proxyDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/logging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type testCase struct {
	name string
	run  func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService)
}

func TestAppProxyRouter(t *testing.T) {
	testCases := []testCase{
		{
			name: "missing-app-id",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				// Request without app id
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusNotFound, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Equal(t, `Unable to find application ID from the URL.`, string(body))

				// Request to health-check endpoint
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://hub.keboola.local/health-check", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
			},
		},
		{
			name: "unknown-app-id",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				// Request to unknown app
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://unknown.hub.keboola.local/health-check", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusNotFound, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Equal(t, `Application "unknown" not found.`, string(body))
			},
		},
		{
			name: "broken-app",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				// Request to broken app
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://broken.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), `Application has invalid configuration.`)
			},
		},
		{
			name: "no-rule-app",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				// Request to app with no path rules
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://norule.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), `Application has invalid configuration.`)
			},
		},
		{
			name: "wrong-rule-type-app",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				// Request to app with invalid rule type
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://invalid1.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), `Application has invalid configuration.`)
			},
		},
		{
			name: "empty-providers-array-app",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				// Request to app with invalid rule type
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://invalid2.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), `Application has invalid configuration.`)
			},
		},
		{
			name: "empty-allowed-roles-array-app",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				// Request to app with invalid rule type
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://invalid3.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), `Application has invalid configuration.`)
			},
		},
		{
			name: "unknown-provider-app",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				// Request to app with unknown provider
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://invalid4.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), `Application has invalid configuration.`)
			},
		},
		{
			name: "public-app-down",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				appServer.Close()

				// Request to public app
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusBadGateway, response.StatusCode)
			},
		},
		{
			name: "public-app-sub-url",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
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

				require.Len(t, *appServer.appRequests, 1)
				appRequest := (*appServer.appRequests)[0]
				assert.Equal(t, "/some/data/app/url?foo=bar", appRequest.URL.String())
				assert.Equal(t, "Internet Exploder", appRequest.Header.Get("User-Agent"))
				assert.Equal(t, "application/json", appRequest.Header.Get("Content-Type"))
				assert.Equal(t, "", appRequest.Header.Get("X-Kbc-Test"))
				assert.Equal(t, "", appRequest.Header.Get("X-Kbc-User-Email"))
			},
		},
		{
			name: "private-app-verified-email",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:         "admin@keboola.com",
					EmailVerified: pointer(true),
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
				location := response.Header["Location"][0]

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
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
				assert.Len(t, cookies, 2)

				assert.Equal(t, "_oauth2_provider", cookies[0].Name)
				assert.Equal(t, "", cookies[0].Value)
				assert.Equal(t, "/", cookies[0].Path)
				assert.Equal(t, "oidc.hub.keboola.local", cookies[0].Domain)
				assert.True(t, cookies[0].HttpOnly)
				assert.True(t, cookies[0].Secure)

				assert.Equal(t, "_oauth2_proxy", cookies[1].Name)
				assert.Equal(t, "", cookies[1].Value)
				assert.Equal(t, "/", cookies[1].Path)
				assert.Equal(t, "oidc.hub.keboola.local", cookies[1].Domain)
				assert.True(t, cookies[1].HttpOnly)
				assert.True(t, cookies[1].Secure)
				assert.Equal(t, http.SameSiteLaxMode, cookies[1].SameSite)
			},
		},
		{
			name: "private-app-unauthorized",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header["Location"][0]

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
		},
		{
			name: "private-missing-csrf-token",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
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
				location := response.Header["Location"][0]

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

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

				// Request to proxy callback (fails because of missing CSRF token)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				wildcards.Assert(t, "%ALogin Failed: Unable to find a valid CSRF token. Please try again.%A", string(body))

				// Request to private app
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
		},
		{
			name: "private-app-group-mismatch",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
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
				location := response.Header["Location"][0]

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to proxy callback (fails because of missing group)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				wildcards.Assert(t, "%AYou do not have permission to access this resource.%A", string(body))

				// Request to private app
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
		},
		{
			name: "private-app-unverified-email",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:         "admin@keboola.com",
					EmailVerified: pointer(false),
					Groups:        []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header["Location"][0]

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to proxy callback (fails because of unverified email)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
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
		},
		{
			name: "private-app-oidc-down",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header["Location"][0]
				cookies := response.Cookies()
				assert.Len(t, cookies, 2)

				assert.Equal(t, "_oauth2_provider", cookies[0].Name)
				assert.Equal(t, "oidc", cookies[0].Value)
				assert.Equal(t, "/", cookies[0].Path)
				assert.Equal(t, "oidc.hub.keboola.local", cookies[0].Domain)
				assert.True(t, cookies[0].HttpOnly)
				assert.True(t, cookies[0].Secure)
				assert.Equal(t, http.SameSiteLaxMode, cookies[0].SameSite)

				assert.Equal(t, "_oauth2_proxy_csrf", cookies[1].Name)
				assert.Equal(t, "/", cookies[1].Path)
				assert.Equal(t, "oidc.hub.keboola.local", cookies[1].Domain)
				assert.True(t, cookies[1].HttpOnly)
				assert.True(t, cookies[1].Secure)
				assert.Equal(t, http.SameSiteNoneMode, cookies[1].SameSite)

				// Shutdown provider server
				m[0].Shutdown()

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
		},
		{
			name: "private-app-down",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
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
				location := response.Header["Location"][0]
				cookies := response.Cookies()
				assert.Len(t, cookies, 2)

				assert.Equal(t, "_oauth2_provider", cookies[0].Name)
				assert.Equal(t, "oidc", cookies[0].Value)
				assert.Equal(t, "/", cookies[0].Path)
				assert.Equal(t, "oidc.hub.keboola.local", cookies[0].Domain)
				assert.True(t, cookies[0].HttpOnly)
				assert.True(t, cookies[0].Secure)
				assert.Equal(t, http.SameSiteLaxMode, cookies[0].SameSite)

				assert.Equal(t, "_oauth2_proxy_csrf", cookies[1].Name)
				assert.Equal(t, "/", cookies[1].Path)
				assert.Equal(t, "oidc.hub.keboola.local", cookies[1].Domain)
				assert.True(t, cookies[1].HttpOnly)
				assert.True(t, cookies[1].Secure)
				assert.Equal(t, http.SameSiteNoneMode, cookies[1].SameSite)

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				cookies = response.Cookies()
				assert.Len(t, cookies, 2)

				assert.Equal(t, "_oauth2_proxy_csrf", cookies[0].Name)
				assert.Equal(t, "", cookies[0].Value)
				assert.Equal(t, "/", cookies[0].Path)
				assert.Equal(t, "oidc.hub.keboola.local", cookies[0].Domain)
				assert.True(t, cookies[0].HttpOnly)
				assert.True(t, cookies[0].Secure)
				assert.Equal(t, http.SameSiteLaxMode, cookies[0].SameSite)

				assert.Equal(t, "_oauth2_proxy", cookies[1].Name)
				assert.Equal(t, "/", cookies[1].Path)
				assert.Equal(t, "oidc.hub.keboola.local", cookies[1].Domain)
				assert.True(t, cookies[1].HttpOnly)
				assert.True(t, cookies[1].Secure)
				assert.Equal(t, http.SameSiteLaxMode, cookies[1].SameSite)

				// Request to private app (authorized but down)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusBadGateway, response.StatusCode)
			},
		},
		{
			name: "multi-app-basic-flow",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				m[1].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin", "manager"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header["Location"][0]
				assert.Equal(t, "https://multi.hub.keboola.local/_proxy/selection", location)

				// Request to private selection page
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), `https://multi.hub.keboola.local/_proxy/selection?provider=oidc0`)
				assert.Contains(t, string(body), `https://multi.hub.keboola.local/_proxy/selection?provider=oidc1`)

				// Provider selection
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/_proxy/selection?provider=oidc1", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]
				cookies := response.Cookies()

				assert.Equal(t, "_oauth2_provider", cookies[0].Name)
				assert.Equal(t, "oidc1", cookies[0].Value)
				assert.Equal(t, "/", cookies[0].Path)
				assert.Equal(t, "multi.hub.keboola.local", cookies[0].Domain)
				assert.True(t, cookies[0].HttpOnly)
				assert.True(t, cookies[0].Secure)
				assert.Equal(t, http.SameSiteLaxMode, cookies[0].SameSite)

				assert.Equal(t, "_oauth2_proxy_csrf", cookies[1].Name)
				assert.Equal(t, "/", cookies[1].Path)
				assert.Equal(t, "multi.hub.keboola.local", cookies[1].Domain)
				assert.True(t, cookies[1].HttpOnly)
				assert.True(t, cookies[1].Secure)
				assert.Equal(t, http.SameSiteNoneMode, cookies[1].SameSite)

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Request to private app (authorized)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/some/data/app/url?foo=bar", nil)
				request.Header.Set("X-Kbc-Test", "something")
				request.Header.Set("X-Kbc-User-Email", "manager@keboola.com")
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)

				require.Len(t, *appServer.appRequests, 1)
				appRequest := (*appServer.appRequests)[0]
				assert.Equal(t, "/some/data/app/url?foo=bar", appRequest.URL.String())
				assert.Equal(t, "admin@keboola.com", appRequest.Header.Get("X-Kbc-User-Email"))
				assert.Equal(t, "admin,manager", appRequest.Header.Get("X-Kbc-User-Roles"))
				assert.Equal(t, "", appRequest.Header.Get("X-Kbc-Test"))
			},
		},
		{
			name: "multi-app-selection-page-redirect",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
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

				// Request to private app (unauthorized)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header["Location"][0]
				assert.Equal(t, "https://multi.hub.keboola.local/_proxy/selection", location)
			},
		},
		{
			name: "multi-app-unverified-email",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				m[1].QueueUser(&mockoidc.MockUser{
					Email:         "admin@keboola.com",
					EmailVerified: pointer(false),
					Groups:        []string{"admin"},
				})

				// Provider selection
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/_proxy/selection?provider=oidc1", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header["Location"][0]

				// Request to private app (unauthorized)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to proxy callback (fails because of unverified email)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusInternalServerError, response.StatusCode)

				// Request to private app
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
		},
		{
			name: "multi-app-down",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				appServer.Close()

				m[1].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header["Location"][0]
				assert.Equal(t, "https://multi.hub.keboola.local/_proxy/selection", location)

				// Provider selection
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/_proxy/selection?provider=oidc1", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
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
			},
		},
		{
			name: "multi-app-broken-provider",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				appServer.Close()

				// Provider selection
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/_proxy/selection?provider=oidc2", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				require.Empty(t, response.Cookies())
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), `Application has invalid configuration.`)
			},
		},
		{
			name: "public-app-websocket",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
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
				defer c.CloseNow()

				var v interface{}
				err = wsjson.Read(ctx, c, &v)
				require.NoError(t, err)

				assert.Equal(t, "Hello websocket", v)

				c.Close(websocket.StatusNormalClosure, "")
			},
		},
		{
			name: "private-app-websocket-unauthorized",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
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
		},
		{
			name: "private-app-websocket",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				m[0].QueueUser(&mockoidc.MockUser{
					Email:         "admin@keboola.com",
					EmailVerified: pointer(true),
					Groups:        []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header["Location"][0]

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
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
				defer c.CloseNow()

				var v interface{}
				err = wsjson.Read(ctx, c, &v)
				require.NoError(t, err)

				assert.Equal(t, "Hello websocket", v)

				c.Close(websocket.StatusNormalClosure, "")
			},
		},
		{
			name: "multi-app-websocket",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				m[1].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header["Location"][0]
				assert.Equal(t, "https://multi.hub.keboola.local/_proxy/selection", location)

				// Provider selection
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://multi.hub.keboola.local/_proxy/selection?provider=oidc1", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
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
				defer c.CloseNow()

				var v interface{}
				err = wsjson.Read(ctx, c, &v)
				require.NoError(t, err)

				assert.Equal(t, "Hello websocket", v)

				c.Close(websocket.StatusNormalClosure, "")
			},
		},
		{
			name: "prefix-app-no-auth",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				// Request to public part of the app
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://prefix.hub.keboola.local/public", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)

				// Request to api (unauthorized)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://prefix.hub.keboola.local/api", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Request to web (unauthorized)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://prefix.hub.keboola.local/web", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Request to web (no matching prefix)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://prefix.hub.keboola.local/unknown", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusNotFound, response.StatusCode)
			},
		},
		{
			name: "prefix-app-api-auth",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
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
				location := response.Header["Location"][0]

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

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
		},
		{
			name: "prefix-app-web-auth",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				m[1].QueueUser(&mockoidc.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private part (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://prefix.hub.keboola.local/web", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Provider selection
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://prefix.hub.keboola.local/_proxy/selection?provider=oidc1", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header["Location"][0]

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
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
		},
		{
			name: "shared-provider",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
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
				location := response.Header["Location"][0]

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
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
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
		},
		{
			name: "configuration-change",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
				// Request to public app
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)

				// Change configuration to private
				originalConfig := service.apps["123"]
				newConfig := service.apps["oidc"]
				newConfig.ID = "public-123"
				service.apps["123"] = newConfig

				// Request to the same app which is now private
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)

				// Revert configuration
				service.apps["123"] = originalConfig

				// Request to public app
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://public-123.hub.keboola.local/", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
			},
		},
		{
			name: "concurrency-test",
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
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
							EmailVerified: pointer(true),
							Groups:        []string{"admin"},
						})

						request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://oidc.hub.keboola.local/foo/bar", nil)
						assert.NoError(t, err)

						response, err := client.Do(request)
						assert.NoError(t, err)

						if assert.Equal(t, http.StatusFound, response.StatusCode) {
							counter.Add(1)
						}
					}()
				}

				// Wait for all requests
				wg.Wait()

				// Check total requests count
				assert.Equal(t, int64(100), counter.Load())
			},
		},
	}

	publicAppTestCaseFactory := func(method string) testCase {
		return testCase{
			name: "public-app-" + method,
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
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
			run: func(t *testing.T, client *http.Client, m []*mockoidc.MockOIDC, appServer *appServer, service *sandboxesService) {
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
				location := response.Header["Location"][0]
				cookies := response.Cookies()
				assert.Len(t, cookies, 2)

				assert.Equal(t, "_oauth2_provider", cookies[0].Name)
				assert.Equal(t, "oidc", cookies[0].Value)
				assert.Equal(t, "/", cookies[0].Path)
				assert.Equal(t, "oidc.hub.keboola.local", cookies[0].Domain)
				assert.True(t, cookies[0].HttpOnly)
				assert.True(t, cookies[0].Secure)
				assert.Equal(t, http.SameSiteLaxMode, cookies[0].SameSite)

				assert.Equal(t, "_oauth2_proxy_csrf", cookies[1].Name)
				assert.Equal(t, "/", cookies[1].Path)
				assert.Equal(t, "oidc.hub.keboola.local", cookies[1].Domain)
				assert.True(t, cookies[1].HttpOnly)
				assert.True(t, cookies[1].Secure)
				assert.Equal(t, http.SameSiteNoneMode, cookies[1].SameSite)

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to proxy callback
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				cookies = response.Cookies()
				assert.Len(t, cookies, 2)

				assert.Equal(t, "_oauth2_proxy_csrf", cookies[0].Name)
				assert.Equal(t, "", cookies[0].Value)
				assert.Equal(t, "/", cookies[0].Path)
				assert.Equal(t, "oidc.hub.keboola.local", cookies[0].Domain)
				assert.True(t, cookies[0].HttpOnly)
				assert.True(t, cookies[0].Secure)
				assert.Equal(t, http.SameSiteLaxMode, cookies[0].SameSite)

				assert.Equal(t, "_oauth2_proxy", cookies[1].Name)
				assert.Equal(t, "/", cookies[1].Path)
				assert.Equal(t, "oidc.hub.keboola.local", cookies[1].Domain)
				assert.True(t, cookies[1].HttpOnly)
				assert.True(t, cookies[1].Secure)
				assert.Equal(t, http.SameSiteLaxMode, cookies[1].SameSite)

				// Request to private app (authorized)
				request, err = http.NewRequestWithContext(context.Background(), method, "https://oidc.hub.keboola.local/some/data/app/url?foo=bar", nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)

				require.Len(t, *appServer.appRequests, 1)
				appRequest := (*appServer.appRequests)[0]
				assert.Equal(t, "/some/data/app/url?foo=bar", appRequest.URL.String())
				assert.Equal(t, "admin@keboola.com", appRequest.Header.Get("X-Kbc-User-Email"))
				assert.Equal(t, "admin", appRequest.Header.Get("X-Kbc-User-Roles"))
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

	t.Parallel()

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			appServer := startAppServer(t)
			defer appServer.Close()

			m0 := startOIDCProviderServer(t)
			defer m0.Shutdown()

			m1 := startOIDCProviderServer(t)
			defer m1.Shutdown()

			tsURL, err := url.Parse(appServer.URL)
			require.NoError(t, err)

			m := []*mockoidc.MockOIDC{m0, m1}

			apps := configureDataApps(tsURL, m)

			service := startSandboxesService(t, apps)
			defer service.Close()

			handler := createProxyHandler(t, service.URL)

			proxy := httptest.NewUnstartedServer(handler)
			proxy.EnableHTTP2 = true
			proxy.StartTLS()
			defer proxy.Close()

			proxyURL, err := url.Parse(proxy.URL)
			require.NoError(t, err)

			client := createHTTPClient(t, proxyURL)

			tc.run(t, client, m, appServer, service)
		})
	}
}

func configureDataApps(tsURL *url.URL, m []*mockoidc.MockOIDC) []appconfig.AppProxyConfig {
	return []appconfig.AppProxyConfig{
		{
			ID:             "norule",
			UpstreamAppURL: tsURL.String(),
		},
		{
			ID:             "123",
			Name:           "public",
			UpstreamAppURL: tsURL.String(),
			AuthRules: []appconfig.AuthRule{
				{
					Type:         appconfig.PathPrefix,
					Value:        "/",
					AuthRequired: pointer(false),
				},
			},
		},
		{
			ID:             "invalid1",
			UpstreamAppURL: tsURL.String(),
			AuthRules: []appconfig.AuthRule{
				{
					Type:  "unknown",
					Value: "/",
					Auth:  nil,
				},
			},
		},
		{
			ID:             "invalid2",
			UpstreamAppURL: tsURL.String(),
			AuthRules: []appconfig.AuthRule{
				{
					Type:         appconfig.PathPrefix,
					Value:        "/",
					AuthRequired: pointer(false),
					Auth:         []string{"test"},
				},
			},
		},
		{
			ID:             "invalid3",
			UpstreamAppURL: tsURL.String(),
			AuthProviders: []appconfig.AuthProvider{
				{
					ID:           "oidc",
					ClientID:     m[0].Config().ClientID,
					ClientSecret: m[0].Config().ClientSecret,
					Type:         appconfig.OIDCProvider,
					AllowedRoles: pointer([]string{}),
					IssuerURL:    m[0].Issuer(),
				},
			},
			AuthRules: []appconfig.AuthRule{
				{
					Type:  appconfig.PathPrefix,
					Value: "/",
					Auth:  []string{"oidc"},
				},
			},
		},
		{
			ID:             "invalid4",
			UpstreamAppURL: tsURL.String(),
			AuthRules: []appconfig.AuthRule{
				{
					Type:  appconfig.PathPrefix,
					Value: "/",
					Auth:  []string{"unknown"},
				},
			},
		},
		{
			ID:             "oidc",
			UpstreamAppURL: tsURL.String(),
			AuthProviders: []appconfig.AuthProvider{
				{
					ID:           "oidc",
					ClientID:     m[0].Config().ClientID,
					ClientSecret: m[0].Config().ClientSecret,
					Type:         appconfig.OIDCProvider,
					AllowedRoles: pointer([]string{"admin"}),
					IssuerURL:    m[0].Issuer(),
				},
			},
			AuthRules: []appconfig.AuthRule{
				{
					Type:  appconfig.PathPrefix,
					Value: "/",
					Auth:  []string{"oidc"},
				},
			},
		},
		{
			ID:             "multi",
			UpstreamAppURL: tsURL.String(),
			AuthProviders: []appconfig.AuthProvider{
				{
					ID:           "oidc0",
					ClientID:     m[0].Config().ClientID,
					ClientSecret: m[0].Config().ClientSecret,
					Type:         appconfig.OIDCProvider,
					AllowedRoles: pointer([]string{"manager"}),
					IssuerURL:    m[0].Issuer(),
				},
				{
					ID:           "oidc1",
					ClientID:     m[1].Config().ClientID,
					ClientSecret: m[1].Config().ClientSecret,
					Type:         appconfig.OIDCProvider,
					AllowedRoles: pointer([]string{"admin"}),
					IssuerURL:    m[1].Issuer(),
				},
				{
					ID: "oidc2",
				},
			},
			AuthRules: []appconfig.AuthRule{
				{
					Type:  appconfig.PathPrefix,
					Value: "/",
					Auth:  []string{"oidc0", "oidc1", "oidc2"},
				},
			},
		},
		{
			ID:             "broken",
			UpstreamAppURL: tsURL.String(),
			AuthProviders: []appconfig.AuthProvider{
				{
					ID:           "oidc",
					ClientID:     "",
					ClientSecret: m[0].Config().ClientSecret,
					Type:         appconfig.OIDCProvider,
					AllowedRoles: pointer([]string{"admin"}),
					IssuerURL:    m[0].Issuer(),
				},
			},
			AuthRules: []appconfig.AuthRule{
				{
					Type:  appconfig.PathPrefix,
					Value: "/",
					Auth:  []string{"oidc"},
				},
			},
		},
		{
			ID:             "prefix",
			UpstreamAppURL: tsURL.String(),
			AuthProviders: []appconfig.AuthProvider{
				{
					ID:           "oidc0",
					ClientID:     m[0].Config().ClientID,
					ClientSecret: m[0].Config().ClientSecret,
					Type:         appconfig.OIDCProvider,
					AllowedRoles: pointer([]string{"admin"}),
					IssuerURL:    m[0].Issuer(),
				},
				{
					ID:           "oidc1",
					ClientID:     m[1].Config().ClientID,
					ClientSecret: m[1].Config().ClientSecret,
					Type:         appconfig.OIDCProvider,
					AllowedRoles: pointer([]string{"admin"}),
					IssuerURL:    m[1].Issuer(),
				},
			},
			AuthRules: []appconfig.AuthRule{
				{
					Type:  appconfig.PathPrefix,
					Value: "/api",
					Auth:  []string{"oidc0"},
				},
				{
					Type:  appconfig.PathPrefix,
					Value: "/web",
					Auth:  []string{"oidc0", "oidc1"},
				},
				{
					Type:         appconfig.PathPrefix,
					Value:        "/public",
					AuthRequired: pointer(false),
				},
			},
		},
	}
}

type appServer struct {
	*httptest.Server
	appRequests *[]*http.Request
}

func startAppServer(t *testing.T) *appServer {
	t.Helper()

	lock := &sync.Mutex{}
	var requests []*http.Request

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		require.NoError(t, err)
		defer c.CloseNow()

		ctx, cancel := context.WithTimeout(r.Context(), time.Second*10)
		defer cancel()

		err = wsjson.Write(ctx, c, "Hello websocket")
		require.NoError(t, err)

		c.Close(websocket.StatusNormalClosure, "")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		defer lock.Unlock()
		requests = append(requests, r)
		fmt.Fprint(w, "Hello, client")
	})

	ts := httptest.NewUnstartedServer(mux)
	ts.EnableHTTP2 = true
	ts.Start()

	return &appServer{ts, &requests}
}

type sandboxesService struct {
	*httptest.Server
	apps map[string]appconfig.AppProxyConfig
}

func startSandboxesService(t *testing.T, apps []appconfig.AppProxyConfig) *sandboxesService {
	t.Helper()

	service := &sandboxesService{
		apps: make(map[string]appconfig.AppProxyConfig),
	}

	for _, app := range apps {
		service.apps[app.ID] = app
	}

	r := regexp.MustCompile("apps/([a-zA-Z0-9]+)/proxy-config")

	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		match := r.FindStringSubmatch(req.RequestURI)
		if len(match) < 2 {
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, "{}")
			return
		}

		appID := match[1]
		app, ok := service.apps[appID]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, "{}")
			return
		}

		// Calculate ETag (in this test we simply use provider count)
		w.Header().Set("ETag", fmt.Sprintf(`"%d"`, len(app.AuthProviders)))
		w.WriteHeader(http.StatusOK)

		jsonData, err := json.Encode(app, true)
		assert.NoError(t, err)

		w.Write(jsonData)
	})

	ts := httptest.NewUnstartedServer(handler)
	ts.EnableHTTP2 = true
	ts.Start()

	service.Server = ts

	return service
}

func startOIDCProviderServer(t *testing.T) *mockoidc.MockOIDC {
	t.Helper()

	m, err := mockoidc.Run()
	require.NoError(t, err)

	return m
}

func createProxyHandler(t *testing.T, sandboxesAPIURL string) http.Handler {
	t.Helper()

	secret := make([]byte, 32)
	_, err := rand.Read(secret)
	require.NoError(t, err)

	cfg := config.New()
	cfg.CookieSecretSalt = string(secret)
	cfg.SandboxesAPI.URL = sandboxesAPIURL

	d, _ := proxyDependencies.NewMockedServiceScope(t, cfg, dependencies.WithRealHTTPClient())

	loggerWriter := logging.NewLoggerWriter(d.Logger(), "info")
	logger.SetOutput(loggerWriter)
	// Cannot separate errors from info because oauthproxy will override its error writer with either
	// the info writer or os.Stderr depending on Logging.ErrToInfo value whenever a new proxy instance is created.
	logger.SetErrOutput(loggerWriter)

	router, err := NewRouter(d, "proxy-")
	require.NoError(t, err)

	return middleware.Wrap(
		router.CreateHandler(),
		appIDMiddleware(d.Config().API.PublicURL),
	)
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

func pointer[T any](d T) *T {
	return &d
}
