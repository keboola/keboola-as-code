package oauthproxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
)

func testProxyConfigEnv(t *testing.T, scopesSupported string) (config.Config, api.AppConfig, scopeResolver, string) {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/.well-known/openid-configuration" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"scopes_supported":` + scopesSupported + `}`))
	}))
	t.Cleanup(srv.Close)

	cfg := config.New()
	cfg.API.PublicURL, _ = url.Parse("https://hub.keboola.local")
	cfg.CookieSecretSalt = "salt"
	app := api.AppConfig{ID: "123", ProjectID: "456", AppSlug: new("app")}
	return cfg, app, newScopeResolver(log.NewDebugLogger(), client.New()), srv.URL
}

func testOIDC(issuer string, allowedRoles *[]string) provider.OIDC {
	return provider.OIDC{
		Base:         provider.Base{Info: provider.Info{ID: "oidc", Type: provider.TypeOIDC}},
		ClientID:     "client",
		ClientSecret: "secret",
		IssuerURL:    issuer,
		AllowedRoles: allowedRoles,
	}
}

func TestProxyConfig_SessionLifetime(t *testing.T) {
	t.Parallel()

	cfg, app, scopes, issuer := testProxyConfigEnv(t, `["openid"]`)

	cases := []struct {
		name            string
		auth            provider.OAuthProvider
		expectedRefresh time.Duration
	}{
		{name: "oidc", auth: testOIDC(issuer, nil), expectedRefresh: time.Hour},
		{name: "jumpcloud", auth: provider.JumpCloud{OIDC: testOIDC(issuer, nil)}, expectedRefresh: time.Hour},
		{
			name: "github",
			auth: provider.GitHub{
				Base:         provider.Base{Info: provider.Info{ID: "github", Type: provider.TypeGitHub}},
				ClientID:     "client",
				ClientSecret: "secret",
			},
			expectedRefresh: time.Hour,
		},
		{
			// GitLab refresh tokens are single-use, see docs/apps-proxy/login-session.md.
			name:            "gitlab",
			auth:            provider.GitLab{OIDC: testOIDC(issuer, nil)},
			expectedRefresh: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v, err := proxyConfig(t.Context(), cfg, nil, scopes, nil, nil, app, tc.auth, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedRefresh, v.Cookie.Refresh)
			assert.Equal(t, 24*time.Hour, v.Cookie.Expire)
		})
	}
}

func TestProxyConfig_Scope(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name            string
		scopesSupported string
		allowedRoles    *[]string
		gitlab          bool
		expected        string
	}{
		{
			name:            "offline_access listed",
			scopesSupported: `["openid","email","profile","offline_access"]`,
			expected:        "openid email profile offline_access",
		},
		{
			name:            "offline_access listed, allowed roles",
			scopesSupported: `["openid","email","profile","groups","offline_access"]`,
			allowedRoles:    &[]string{"admin"},
			expected:        "openid email profile groups offline_access",
		},
		{
			name:            "offline_access not listed: oauth2-proxy default",
			scopesSupported: `["openid","email","profile"]`,
			expected:        "",
		},
		{
			name:            "gitlab is not asked: oauth2-proxy default",
			scopesSupported: `["openid","email","profile","offline_access"]`,
			gitlab:          true,
			expected:        "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg, app, scopes, issuer := testProxyConfigEnv(t, tc.scopesSupported)
			var auth provider.OAuthProvider = testOIDC(issuer, tc.allowedRoles)
			if tc.gitlab {
				auth = provider.GitLab{OIDC: testOIDC(issuer, tc.allowedRoles)}
			}
			v, err := proxyConfig(t.Context(), cfg, nil, scopes, nil, nil, app, auth, nil)
			require.NoError(t, err)
			require.Len(t, v.Providers, 1)
			assert.Equal(t, tc.expected, v.Providers[0].Scope)
		})
	}
}
