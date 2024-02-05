// nolint: thelper // because it wants the run functions to start with t.Helper()
package http

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/oauth2-proxy/mockoidc"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/providers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/config"
	proxyDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/dependencies"
	mockoidcCustom "github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/mockoidc"
)

type testCase struct {
	name string
	run  func(t *testing.T, client *http.Client, m *mockoidc.MockOIDC, appServer *appServer)
}

func TestAppProxyRouter(t *testing.T) {
	testCases := []testCase{
		{
			name: "missing-app-id",
			run: func(t *testing.T, client *http.Client, m *mockoidc.MockOIDC, appServer *appServer) {
				// Request without app id
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://data-apps.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusNotFound, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Equal(t, `Unable to parse application ID from the URL.`, string(body))
			},
		},
		{
			name: "unknown-app-id",
			run: func(t *testing.T, client *http.Client, m *mockoidc.MockOIDC, appServer *appServer) {
				// Request to unknown app
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://unknown.data-apps.keboola.local/", nil)
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
			name: "public-app-down",
			run: func(t *testing.T, client *http.Client, m *mockoidc.MockOIDC, appServer *appServer) {
				appServer.Close()

				// Request to public app
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://public.data-apps.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusBadGateway, response.StatusCode)
			},
		},
		{
			name: "public-app-sub-url",
			run: func(t *testing.T, client *http.Client, m *mockoidc.MockOIDC, appServer *appServer) {
				// Request to public app
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://public.data-apps.keboola.local/some/data/app/url?foo=bar", nil)
				request.Header.Set("User-Agent", "Internet Exploder")
				request.Header.Set("Content-Type", "application/json")
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
			},
		},
		{
			name: "private-app-verified-email",
			run: func(t *testing.T, client *http.Client, m *mockoidc.MockOIDC, appServer *appServer) {
				m.QueueUser(&mockoidcCustom.MockUser{
					Email:         "admin@keboola.com",
					EmailVerified: pointer(true),
					Groups:        []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header["Location"][0]
				cookies := response.Header["Set-Cookie"]

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
				for _, cookie := range cookies {
					request.Header.Add("Cookie", cookie)
				}
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				cookies = response.Header["Set-Cookie"]

				// Request to private app (authorized)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				require.NoError(t, err)
				for _, cookie := range cookies {
					request.Header.Add("Cookie", cookie)
				}
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
			},
		},
		{
			name: "private-app-unauthorized",
			run: func(t *testing.T, client *http.Client, m *mockoidc.MockOIDC, appServer *appServer) {
				m.QueueError(&mockoidc.ServerError{
					Code:  http.StatusUnauthorized,
					Error: mockoidc.InvalidRequest,
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header["Location"][0]
				cookies := response.Header["Set-Cookie"]

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusUnauthorized, response.StatusCode)

				// Request to private app (still unauthorized because login failed)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				require.NoError(t, err)
				for _, cookie := range cookies {
					request.Header.Add("Cookie", cookie)
				}
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
		},
		{
			name: "private-missing-csrf-token",
			run: func(t *testing.T, client *http.Client, m *mockoidc.MockOIDC, appServer *appServer) {
				m.QueueUser(&mockoidcCustom.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header["Location"][0]
				cookies := response.Header["Set-Cookie"]

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

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
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				require.NoError(t, err)
				for _, cookie := range cookies {
					request.Header.Add("Cookie", cookie)
				}
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
		},
		{
			name: "private-app-group-mismatch",
			run: func(t *testing.T, client *http.Client, m *mockoidc.MockOIDC, appServer *appServer) {
				m.QueueUser(&mockoidcCustom.MockUser{
					Email:  "manager@keboola.com",
					Groups: []string{"manager"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header["Location"][0]
				cookies := response.Header["Set-Cookie"]

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
				for _, cookie := range cookies {
					request.Header.Add("Cookie", cookie)
				}
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				wildcards.Assert(t, "%AYou do not have permission to access this resource.%A", string(body))

				// Request to private app
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				require.NoError(t, err)
				for _, cookie := range cookies {
					request.Header.Add("Cookie", cookie)
				}
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
		},
		{
			name: "private-app-unverified-email",
			run: func(t *testing.T, client *http.Client, m *mockoidc.MockOIDC, appServer *appServer) {
				m.QueueUser(&mockoidcCustom.MockUser{
					Email:         "admin@keboola.com",
					EmailVerified: pointer(false),
					Groups:        []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header["Location"][0]
				cookies := response.Header["Set-Cookie"]

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
				for _, cookie := range cookies {
					request.Header.Add("Cookie", cookie)
				}
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusInternalServerError, response.StatusCode)
				cookies = response.Header["Set-Cookie"]

				// Request to private app
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				require.NoError(t, err)
				for _, cookie := range cookies {
					request.Header.Add("Cookie", cookie)
				}
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
		},
		{
			name: "private-app-oidc-down",
			run: func(t *testing.T, client *http.Client, m *mockoidc.MockOIDC, appServer *appServer) {
				m.Shutdown()

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header["Location"][0]
				cookies := response.Header["Set-Cookie"]
				assert.Len(t, cookies, 1)
				wildcards.Assert(t, "_oauth2_proxy_csrf=%s; Path=/; Domain=oidc.data-apps.keboola.local; Expires=%s; HttpOnly; Secure", cookies[0])

				// Request to the OIDC provider
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err = client.Do(request)
				require.Error(t, err)
				require.Nil(t, response)

				// Request to private app (unauthorized)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				require.NoError(t, err)
				for _, cookie := range cookies {
					request.Header.Add("Cookie", cookie)
				}
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
			},
		},
		{
			name: "private-app-down",
			run: func(t *testing.T, client *http.Client, m *mockoidc.MockOIDC, appServer *appServer) {
				appServer.Close()

				m.QueueUser(&mockoidcCustom.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header["Location"][0]
				cookies := response.Header["Set-Cookie"]
				assert.Len(t, cookies, 1)
				wildcards.Assert(t, "_oauth2_proxy_csrf=%s; Path=/; Domain=oidc.data-apps.keboola.local; Expires=%s; HttpOnly; Secure", cookies[0])

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
				for _, cookie := range cookies {
					request.Header.Add("Cookie", cookie)
				}
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				cookies = response.Header["Set-Cookie"]
				assert.Len(t, cookies, 2)
				wildcards.Assert(t, "_oauth2_proxy_csrf=; Path=/; Domain=oidc.data-apps.keboola.local; Expires=%s; HttpOnly; Secure", cookies[0])
				wildcards.Assert(t, "_oauth2_proxy=%s; Path=/; Domain=oidc.data-apps.keboola.local; Expires=%s; HttpOnly; Secure", cookies[1])

				// Request to private app (authorized but down)
				request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				require.NoError(t, err)
				for _, cookie := range cookies {
					request.Header.Add("Cookie", cookie)
				}
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusBadGateway, response.StatusCode)
			},
		},
		{
			name: "public-app-websocket",
			run: func(t *testing.T, client *http.Client, m *mockoidc.MockOIDC, appServer *appServer) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()

				c, _, err := websocket.Dial(
					ctx,
					"wss://public.data-apps.keboola.local/ws",
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
			run: func(t *testing.T, client *http.Client, m *mockoidc.MockOIDC, appServer *appServer) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()

				_, _, err := websocket.Dial(
					ctx,
					"wss://oidc.data-apps.keboola.local/ws",
					&websocket.DialOptions{
						HTTPClient: client,
					},
				)
				require.Error(t, err)
			},
		},
		{
			name: "private-app-websocket",
			run: func(t *testing.T, client *http.Client, m *mockoidc.MockOIDC, appServer *appServer) {
				m.QueueUser(&mockoidcCustom.MockUser{
					Email:         "admin@keboola.com",
					EmailVerified: pointer(true),
					Groups:        []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header["Location"][0]
				cookies := response.Header["Set-Cookie"]

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
				for _, cookie := range cookies {
					request.Header.Add("Cookie", cookie)
				}
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				cookies = response.Header["Set-Cookie"]

				// Websocket request
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()

				c, _, err := websocket.Dial(
					ctx,
					"wss://oidc.data-apps.keboola.local/ws",
					&websocket.DialOptions{
						HTTPClient: client,
						HTTPHeader: http.Header{
							"Cookie": cookies,
						},
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
	}

	publicAppTestCaseFactory := func(method string) testCase {
		return testCase{
			name: "public-app-" + method,
			run: func(t *testing.T, client *http.Client, m *mockoidc.MockOIDC, appServer *appServer) {
				// Request to public app
				request, err := http.NewRequestWithContext(context.Background(), method, "https://public.data-apps.keboola.local/", nil)
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
			run: func(t *testing.T, client *http.Client, m *mockoidc.MockOIDC, appServer *appServer) {
				m.QueueUser(&mockoidcCustom.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				// Request to private app (unauthorized)
				request, err := http.NewRequestWithContext(context.Background(), method, "https://oidc.data-apps.keboola.local/", nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location := response.Header["Location"][0]
				cookies := response.Header["Set-Cookie"]
				assert.Len(t, cookies, 1)
				wildcards.Assert(t, "_oauth2_proxy_csrf=%s; Path=/; Domain=oidc.data-apps.keboola.local; Expires=%s; HttpOnly; Secure", cookies[0])

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
				for _, cookie := range cookies {
					request.Header.Add("Cookie", cookie)
				}
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				cookies = response.Header["Set-Cookie"]
				assert.Len(t, cookies, 2)
				wildcards.Assert(t, "_oauth2_proxy_csrf=; Path=/; Domain=oidc.data-apps.keboola.local; Expires=%s; HttpOnly; Secure", cookies[0])
				wildcards.Assert(t, "_oauth2_proxy=%s; Path=/; Domain=oidc.data-apps.keboola.local; Expires=%s; HttpOnly; Secure", cookies[1])

				// Request to private app (authorized)
				request, err = http.NewRequestWithContext(context.Background(), method, "https://oidc.data-apps.keboola.local/", nil)
				require.NoError(t, err)
				for _, cookie := range cookies {
					request.Header.Add("Cookie", cookie)
				}
				response, err = client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
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

			m := startOIDCProviderServer(t)
			defer m.Shutdown()

			tsURL, err := url.Parse(appServer.URL)
			require.NoError(t, err)

			apps := []DataApp{
				{
					ID:           "public",
					Name:         "Public app",
					UpstreamHost: tsURL.Host,
				},
				{
					ID:           "oidc",
					Name:         "OIDC Protected App",
					UpstreamHost: tsURL.Host,
					Providers: []options.Provider{
						{
							ID:                  "oidc",
							ClientID:            m.Config().ClientID,
							ClientSecret:        m.Config().ClientSecret,
							Type:                options.OIDCProvider,
							CodeChallengeMethod: providers.CodeChallengeMethodS256,
							AllowedGroups:       []string{"admin"},
							OIDCConfig: options.OIDCOptions{
								IssuerURL:      m.Issuer(),
								EmailClaim:     options.OIDCEmailClaim,
								GroupsClaim:    options.OIDCGroupsClaim,
								AudienceClaims: options.OIDCAudienceClaims,
								UserIDClaim:    options.OIDCEmailClaim,
							},
						},
					},
				},
			}

			handler := createProxyHandler(t, apps)

			proxy := httptest.NewUnstartedServer(handler)
			proxy.EnableHTTP2 = true
			proxy.StartTLS()

			proxyURL, err := url.Parse(proxy.URL)
			require.NoError(t, err)

			client := createHTTPClient(proxyURL)

			tc.run(t, client, m, appServer)
		})
	}
}

type appServer struct {
	*httptest.Server
	appRequests *[]*http.Request
}

func startAppServer(t *testing.T) *appServer {
	t.Helper()

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
		requests = append(requests, r)
		fmt.Fprint(w, "Hello, client")
	})

	ts := httptest.NewUnstartedServer(mux)
	ts.EnableHTTP2 = true
	ts.Start()
	return &appServer{ts, &requests}
}

func startOIDCProviderServer(t *testing.T) *mockoidc.MockOIDC {
	t.Helper()

	m, err := mockoidc.Run()
	require.NoError(t, err)

	return m
}

func createProxyHandler(t *testing.T, apps []DataApp) http.Handler {
	t.Helper()

	secret := make([]byte, 32)
	_, err := rand.Read(secret)
	require.NoError(t, err)

	cfg := config.NewConfig()
	cfg.CookieSecret = string(secret)

	d, _ := proxyDependencies.NewMockedServiceScope(t, cfg)

	router := NewRouter(context.Background(), d, apps)

	return router.CreateHandler()
}

func createHTTPClient(proxyURL *url.URL) *http.Client {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig.InsecureSkipVerify = true
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if strings.HasSuffix(addr, "data-apps.keboola.local:443") {
			addr = proxyURL.Host
		}
		return dialer.DialContext(ctx, network, addr)
	}

	return &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: transport,
	}
}

func pointer[T any](d T) *T {
	return &d
}
