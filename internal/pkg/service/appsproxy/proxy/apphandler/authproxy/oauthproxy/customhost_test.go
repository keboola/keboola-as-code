package oauthproxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
)

// The OIDC start on a custom host registers the callback of that host, keeps host-only cookies,
// and never takes the origin from a forwarded header.
func TestHandler_StartOnCustomHost(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, customHost, host, redirectURI string
		hostOnly                            bool
	}{
		{"hub", "", "app-123.hub.keboola.local", "https://app-123.hub.keboola.local/_proxy/callback", false},
		{"custom", "data.customer.org", "data.customer.org", "https://data.customer.org/_proxy/callback", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			issuer := testDiscoveryServer(t, []string{"openid"})
			cfg, app, _, _ := testProxyConfigEnv(t, `[]`)
			app.AppSlug = new("app")
			app.CustomHost = tc.customHost
			pw, err := pagewriter.New(pageWriterDeps{})
			require.NoError(t, err)

			h := NewHandler(t.Context(), log.NewDebugLogger(), client.New(), cfg, testSessionStore(t), nil, pw, app, testOIDC(issuer, nil), nil)
			require.NoError(t, h.initErr)

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+tc.host+config.InternalPrefix+"/start?rd=/", nil)
			req.Host = tc.host
			req.Header.Set("X-Forwarded-Host", "evil.example.net")
			req.Header.Set("X-Forwarded-Proto", "http")
			rec := httptest.NewRecorder()
			require.NoError(t, h.ServeHTTPOrError(rec, req))
			require.Equal(t, http.StatusFound, rec.Code)

			location, err := url.Parse(rec.Header().Get("Location"))
			require.NoError(t, err)
			assert.Equal(t, tc.redirectURI, location.Query().Get("redirect_uri"))
			assert.NotContains(t, rec.Header().Get("Location"), "evil.example.net")

			cookies := rec.Result().Cookies()
			require.NotEmpty(t, cookies)
			for _, c := range cookies {
				assert.Equal(t, tc.hostOnly, c.Domain == "", "cookie %s domain %q", c.Name, c.Domain)
			}
		})
	}
}

// A redirect_uri error from the provider on a custom host shows the callback URL to register. The .hub page is unchanged.
func TestHandler_RedirectURIErrorShowsCallbackOnCustomHost(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, customHost, host, providerError string
		wantURL                               bool
	}{
		{"custom", "data.customer.org", "data.customer.org", "redirect_uri_mismatch", true},
		{"custom other error", "data.customer.org", "data.customer.org", "access_denied", false},
		{"hub", "", "app-123.hub.keboola.local", "redirect_uri_mismatch", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			issuer := testDiscoveryServer(t, []string{"openid"})
			cfg, app, _, _ := testProxyConfigEnv(t, `[]`)
			app.AppSlug = new("app")
			app.CustomHost = tc.customHost
			pw, err := pagewriter.New(pageWriterDeps{})
			require.NoError(t, err)

			h := NewHandler(t.Context(), log.NewDebugLogger(), client.New(), cfg, testSessionStore(t), nil, pw, app, testOIDC(issuer, nil), nil)
			require.NoError(t, h.initErr)

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+tc.host+config.InternalPrefix+"/callback?error="+tc.providerError, nil)
			req.Host = tc.host
			rec := httptest.NewRecorder()
			require.NoError(t, h.ServeHTTPOrError(rec, req))

			assert.Equal(t, http.StatusForbidden, rec.Code)
			assert.Equal(t, tc.wantURL, strings.Contains(rec.Body.String(), "https://data.customer.org/_proxy/callback"), rec.Body.String())
		})
	}
}
