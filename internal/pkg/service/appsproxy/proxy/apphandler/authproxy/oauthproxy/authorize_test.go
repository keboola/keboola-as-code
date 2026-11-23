package oauthproxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/testutil"
)

func TestHandler_AuthorizeRedirectScope(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name            string
		scopesSupported []string
		auth            func(issuer string) provider.OAuthProvider
		expected        string
	}{
		{
			name:            "allowed roles, groups not listed",
			scopesSupported: []string{"openid", "email", "profile"},
			auth:            func(issuer string) provider.OAuthProvider { return testOIDC(issuer, &[]string{"admin"}) },
			expected:        "openid email profile",
		},
		{
			name:            "allowed roles, groups listed",
			scopesSupported: []string{"openid", "email", "profile", "groups"},
			auth:            func(issuer string) provider.OAuthProvider { return testOIDC(issuer, &[]string{"admin"}) },
			expected:        "openid email profile groups",
		},
		{
			name:            "allowed roles, no scopes_supported",
			scopesSupported: nil,
			auth:            func(issuer string) provider.OAuthProvider { return testOIDC(issuer, &[]string{"admin"}) },
			expected:        "openid email profile",
		},
		{
			name:            "no allowed roles, groups and offline_access listed",
			scopesSupported: []string{"openid", "email", "profile", "groups", "offline_access"},
			auth:            func(issuer string) provider.OAuthProvider { return testOIDC(issuer, nil) },
			expected:        "openid email profile offline_access",
		},
		{
			name:            "jumpcloud, allowed roles, groups not listed",
			scopesSupported: []string{"openid", "email", "profile", "offline_access"},
			auth: func(issuer string) provider.OAuthProvider {
				return provider.JumpCloud{OIDC: testOIDC(issuer, &[]string{"admin"})}
			},
			expected: "openid email profile offline_access",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			issuer := testDiscoveryServer(t, tc.scopesSupported)
			cfg, app, _, _ := testProxyConfigEnv(t, `[]`)
			pw, err := pagewriter.New(pageWriterDeps{})
			require.NoError(t, err)

			h := NewHandler(t.Context(), log.NewDebugLogger(), client.New().WithTransport(testutil.IDPTransport()), cfg, SessionStore{}, nil, pw, app, tc.auth(issuer), nil)
			require.NoError(t, h.initErr)

			domain := app.CookieDomain(cfg.API.PublicURL)
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+domain+config.InternalPrefix+"/start?rd=/", nil)
			rec := httptest.NewRecorder()
			require.NoError(t, h.ServeHTTPOrError(rec, req))
			require.Equal(t, http.StatusFound, rec.Code)

			location, err := url.Parse(rec.Header().Get("Location"))
			require.NoError(t, err)
			assert.Equal(t, issuer+"/authorize", location.Scheme+"://"+location.Host+location.Path)
			assert.Equal(t, tc.expected, location.Query().Get("scope"))
		})
	}
}

// testDiscoveryServer serves only the discovery document, enough for the first redirect to the IdP.
func testDiscoveryServer(t *testing.T, scopesSupported []string) string {
	t.Helper()

	var issuer string
	issuer = testutil.StartIDPServer(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/.well-known/openid-configuration" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		doc := map[string]any{
			"issuer":                                issuer,
			"authorization_endpoint":                issuer + "/authorize",
			"token_endpoint":                        issuer + "/token",
			"jwks_uri":                              issuer + "/jwks",
			"userinfo_endpoint":                     issuer + "/userinfo",
			"response_types_supported":              []string{"code"},
			"subject_types_supported":               []string{"public"},
			"id_token_signing_alg_values_supported": []string{"RS256"},
			"code_challenge_methods_supported":      []string{"S256"},
		}
		if scopesSupported != nil {
			doc["scopes_supported"] = scopesSupported
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	}))
	return issuer
}
