// nolint: thelper // because it wants the run functions to start with t.Helper()
package http

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/oauth2-proxy/mockoidc"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/providers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/config"
	proxyDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/dependencies"
	mockoidcCustom "github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/mockoidc"
)

type testCase struct {
	name string
	run  func(t *testing.T, handler http.Handler, m *mockoidc.MockOIDC, appServer *appServer)
}

func TestAppProxyRouter(t *testing.T) {
	testCases := []testCase{
		{
			name: "missing-app-id",
			run: func(t *testing.T, handler http.Handler, m *mockoidc.MockOIDC, appServer *appServer) {
				// Request without app id
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "https://data-apps.keboola.local/", nil)
				handler.ServeHTTP(rec, req)
				assert.Equal(t, http.StatusNotFound, rec.Code)
				assert.Equal(t, `Unable to parse application ID from the URL.`, rec.Body.String())
			},
		},
		{
			name: "unknown-app-id",
			run: func(t *testing.T, handler http.Handler, m *mockoidc.MockOIDC, appServer *appServer) {
				// Request to unknown app
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "https://unknown.data-apps.keboola.local/", nil)
				handler.ServeHTTP(rec, req)
				assert.Equal(t, http.StatusNotFound, rec.Code)
				assert.Equal(t, `Application "unknown" not found.`, rec.Body.String())
			},
		},
		{
			name: "public-app-down",
			run: func(t *testing.T, handler http.Handler, m *mockoidc.MockOIDC, appServer *appServer) {
				appServer.Close()

				// Request to public app
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "https://public.data-apps.keboola.local/", nil)
				handler.ServeHTTP(rec, req)
				assert.Equal(t, http.StatusBadGateway, rec.Code)
			},
		},
		{
			name: "public-app-sub-url",
			run: func(t *testing.T, handler http.Handler, m *mockoidc.MockOIDC, appServer *appServer) {
				// Request to public app
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "https://public.data-apps.keboola.local/some/data/app/url?foo=bar", nil)
				req.Header.Set("User-Agent", "Internet Exploder")
				req.Header.Set("Content-Type", "application/json")
				handler.ServeHTTP(rec, req)
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, "Hello, client", rec.Body.String())

				require.Len(t, *appServer.appRequests, 1)
				appRequest := (*appServer.appRequests)[0]
				assert.Equal(t, "/some/data/app/url?foo=bar", appRequest.URL.String())
				assert.Equal(t, "Internet Exploder", appRequest.Header.Get("User-Agent"))
				assert.Equal(t, "application/json", appRequest.Header.Get("Content-Type"))
			},
		},
		{
			name: "private-app-verified-email",
			run: func(t *testing.T, handler http.Handler, m *mockoidc.MockOIDC, appServer *appServer) {
				m.QueueUser(&mockoidcCustom.MockUser{
					Email:         "admin@keboola.com",
					EmailVerified: pointer(true),
					Groups:        []string{"admin"},
				})

				client := createNoRedirectHTTPClient()

				// Request to private app (unauthorized)
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusFound, rec.Code)
				location := rec.Header()["Location"][0]
				cookies := rec.Header()["Set-Cookie"]

				// Request to the OIDC provider
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to proxy callback
				rec = httptest.NewRecorder()
				req = httptest.NewRequest(http.MethodGet, location, nil)
				for _, cookie := range cookies {
					req.Header.Add("Cookie", cookie)
				}
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusFound, rec.Code)
				cookies = rec.Header()["Set-Cookie"]

				// Request to private app (authorized)
				rec = httptest.NewRecorder()
				req = httptest.NewRequest(http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				for _, cookie := range cookies {
					req.Header.Add("Cookie", cookie)
				}
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "private-app-unauthorized",
			run: func(t *testing.T, handler http.Handler, m *mockoidc.MockOIDC, appServer *appServer) {
				m.QueueError(&mockoidc.ServerError{
					Code:  http.StatusUnauthorized,
					Error: mockoidc.InvalidRequest,
				})

				client := createNoRedirectHTTPClient()

				// Request to private app (unauthorized)
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusFound, rec.Code)
				location := rec.Header()["Location"][0]
				cookies := rec.Header()["Set-Cookie"]

				// Request to the OIDC provider
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusUnauthorized, response.StatusCode)

				// Request to private app (still unauthorized because login failed)
				rec = httptest.NewRecorder()
				req = httptest.NewRequest(http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				for _, cookie := range cookies {
					req.Header.Add("Cookie", cookie)
				}
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusFound, rec.Code)
			},
		},
		{
			name: "private-missing-csrf-token",
			run: func(t *testing.T, handler http.Handler, m *mockoidc.MockOIDC, appServer *appServer) {
				m.QueueUser(&mockoidcCustom.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				client := createNoRedirectHTTPClient()

				// Request to private app (unauthorized)
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusFound, rec.Code)
				location := rec.Header()["Location"][0]
				cookies := rec.Header()["Set-Cookie"]

				// Request to the OIDC provider
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to proxy callback (fails because of missing CSRF token)
				rec = httptest.NewRecorder()
				req = httptest.NewRequest(http.MethodGet, location, nil)
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusForbidden, rec.Code)
				wildcards.Assert(t, "%ALogin Failed: Unable to find a valid CSRF token. Please try again.%A", rec.Body.String())

				// Request to private app
				rec = httptest.NewRecorder()
				req = httptest.NewRequest(http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				for _, cookie := range cookies {
					req.Header.Add("Cookie", cookie)
				}
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusFound, rec.Code)
			},
		},
		{
			name: "private-app-group-mismatch",
			run: func(t *testing.T, handler http.Handler, m *mockoidc.MockOIDC, appServer *appServer) {
				m.QueueUser(&mockoidcCustom.MockUser{
					Email:  "manager@keboola.com",
					Groups: []string{"manager"},
				})

				client := createNoRedirectHTTPClient()

				// Request to private app (unauthorized)
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusFound, rec.Code)
				location := rec.Header()["Location"][0]
				cookies := rec.Header()["Set-Cookie"]

				// Request to the OIDC provider
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to proxy callback (fails because of missing group)
				rec = httptest.NewRecorder()
				req = httptest.NewRequest(http.MethodGet, location, nil)
				for _, cookie := range cookies {
					req.Header.Add("Cookie", cookie)
				}
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusForbidden, rec.Code)
				wildcards.Assert(t, "%AYou do not have permission to access this resource.%A", rec.Body.String())

				// Request to private app
				rec = httptest.NewRecorder()
				req = httptest.NewRequest(http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				for _, cookie := range cookies {
					req.Header.Add("Cookie", cookie)
				}
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusFound, rec.Code)
			},
		},
		{
			name: "private-app-unverified-email",
			run: func(t *testing.T, handler http.Handler, m *mockoidc.MockOIDC, appServer *appServer) {
				m.QueueUser(&mockoidcCustom.MockUser{
					Email:         "admin@keboola.com",
					EmailVerified: pointer(false),
					Groups:        []string{"admin"},
				})

				client := createNoRedirectHTTPClient()

				// Request to private app (unauthorized)
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusFound, rec.Code)
				location := rec.Header()["Location"][0]
				cookies := rec.Header()["Set-Cookie"]

				// Request to the OIDC provider
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to proxy callback (fails because of unverified email)
				rec = httptest.NewRecorder()
				req = httptest.NewRequest(http.MethodGet, location, nil)
				for _, cookie := range cookies {
					req.Header.Add("Cookie", cookie)
				}
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusInternalServerError, rec.Code)
				cookies = rec.Header()["Set-Cookie"]

				// Request to private app
				rec = httptest.NewRecorder()
				req = httptest.NewRequest(http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				for _, cookie := range cookies {
					req.Header.Add("Cookie", cookie)
				}
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusFound, rec.Code)
			},
		},
		{
			name: "private-app-oidc-down",
			run: func(t *testing.T, handler http.Handler, m *mockoidc.MockOIDC, appServer *appServer) {
				m.Shutdown()

				client := createNoRedirectHTTPClient()

				// Request to private app (unauthorized)
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusFound, rec.Code)
				location := rec.Header()["Location"][0]
				cookies := rec.Header()["Set-Cookie"]
				assert.Len(t, cookies, 1)
				wildcards.Assert(t, "_oauth2_proxy_csrf=%s; Path=/; Domain=oidc.data-apps.keboola.local; Expires=%s; HttpOnly; Secure", cookies[0])

				// Request to the OIDC provider
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.Error(t, err)
				require.Nil(t, response)

				// Request to private app (unauthorized)
				rec = httptest.NewRecorder()
				req = httptest.NewRequest(http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				for _, cookie := range cookies {
					req.Header.Add("Cookie", cookie)
				}
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusFound, rec.Code)
			},
		},
		{
			name: "private-app-down",
			run: func(t *testing.T, handler http.Handler, m *mockoidc.MockOIDC, appServer *appServer) {
				appServer.Close()

				m.QueueUser(&mockoidcCustom.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				client := createNoRedirectHTTPClient()

				// Request to private app (unauthorized)
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusFound, rec.Code)
				location := rec.Header()["Location"][0]
				cookies := rec.Header()["Set-Cookie"]
				assert.Len(t, cookies, 1)
				wildcards.Assert(t, "_oauth2_proxy_csrf=%s; Path=/; Domain=oidc.data-apps.keboola.local; Expires=%s; HttpOnly; Secure", cookies[0])

				// Request to the OIDC provider
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to proxy callback
				rec = httptest.NewRecorder()
				req = httptest.NewRequest(http.MethodGet, location, nil)
				for _, cookie := range cookies {
					req.Header.Add("Cookie", cookie)
				}
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusFound, rec.Code)
				cookies = rec.Header()["Set-Cookie"]
				assert.Len(t, cookies, 2)
				wildcards.Assert(t, "_oauth2_proxy_csrf=; Path=/; Domain=oidc.data-apps.keboola.local; Expires=%s; HttpOnly; Secure", cookies[0])
				wildcards.Assert(t, "_oauth2_proxy=%s; Path=/; Domain=oidc.data-apps.keboola.local; Expires=%s; HttpOnly; Secure", cookies[1])

				// Request to private app (authorized but down)
				rec = httptest.NewRecorder()
				req = httptest.NewRequest(http.MethodGet, "https://oidc.data-apps.keboola.local/", nil)
				for _, cookie := range cookies {
					req.Header.Add("Cookie", cookie)
				}
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusBadGateway, rec.Code)
			},
		},
	}

	publicAppTestCaseFactory := func(method string) testCase {
		return testCase{
			name: "public-app-" + method,
			run: func(t *testing.T, handler http.Handler, m *mockoidc.MockOIDC, appServer *appServer) {
				// Request to public app
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(method, "https://public.data-apps.keboola.local/", nil)
				handler.ServeHTTP(rec, req)
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, "Hello, client", rec.Body.String())
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
			run: func(t *testing.T, handler http.Handler, m *mockoidc.MockOIDC, appServer *appServer) {
				m.QueueUser(&mockoidcCustom.MockUser{
					Email:  "admin@keboola.com",
					Groups: []string{"admin"},
				})

				client := createNoRedirectHTTPClient()

				// Request to private app (unauthorized)
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(method, "https://oidc.data-apps.keboola.local/", nil)
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusFound, rec.Code)
				location := rec.Header()["Location"][0]
				cookies := rec.Header()["Set-Cookie"]
				assert.Len(t, cookies, 1)
				wildcards.Assert(t, "_oauth2_proxy_csrf=%s; Path=/; Domain=oidc.data-apps.keboola.local; Expires=%s; HttpOnly; Secure", cookies[0])

				// Request to the OIDC provider
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, location, nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				require.Equal(t, http.StatusFound, response.StatusCode)
				location = response.Header["Location"][0]

				// Request to proxy callback
				rec = httptest.NewRecorder()
				req = httptest.NewRequest(http.MethodGet, location, nil)
				for _, cookie := range cookies {
					req.Header.Add("Cookie", cookie)
				}
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusFound, rec.Code)
				cookies = rec.Header()["Set-Cookie"]
				assert.Len(t, cookies, 2)
				wildcards.Assert(t, "_oauth2_proxy_csrf=; Path=/; Domain=oidc.data-apps.keboola.local; Expires=%s; HttpOnly; Secure", cookies[0])
				wildcards.Assert(t, "_oauth2_proxy=%s; Path=/; Domain=oidc.data-apps.keboola.local; Expires=%s; HttpOnly; Secure", cookies[1])

				// Request to private app (authorized)
				rec = httptest.NewRecorder()
				req = httptest.NewRequest(method, "https://oidc.data-apps.keboola.local/", nil)
				for _, cookie := range cookies {
					req.Header.Add("Cookie", cookie)
				}
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
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

			appServer := startAppServer()
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
					Provider:     nil,
				},
				{
					ID:           "oidc",
					Name:         "OIDC Protected App",
					UpstreamHost: tsURL.Host,
					Provider: &options.Provider{
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
			}

			handler := createProxyHandler(t, apps)

			tc.run(t, handler, m, appServer)
		})
	}
}

type appServer struct {
	*httptest.Server
	appRequests *[]*http.Request
}

func startAppServer() *appServer {
	var requests []*http.Request
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r)
		fmt.Fprint(w, "Hello, client")
	}))
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

func createNoRedirectHTTPClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func pointer[T any](d T) *T {
	return &d
}
