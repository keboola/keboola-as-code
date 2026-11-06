package oauthproxy

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/client"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func TestScopeResolver(t *testing.T) {
	t.Parallel()

	withOffline := `{"scopes_supported":["openid","email","profile","offline_access"]}`
	withoutOffline := `{"scopes_supported":["openid","email","profile"]}`

	cases := []struct {
		name          string
		providerType  options.ProviderType
		allowedGroups []string
		status        int
		body          string
		expected      string
		expectFetch   bool
	}{
		{
			name:         "oidc, offline_access supported",
			providerType: options.OIDCProvider,
			status:       http.StatusOK,
			body:         withOffline,
			expected:     "openid email profile offline_access",
			expectFetch:  true,
		},
		{
			name:          "oidc with allowed groups, offline_access supported",
			providerType:  options.OIDCProvider,
			allowedGroups: []string{"admin"},
			status:        http.StatusOK,
			body:          withOffline,
			expected:      "openid email profile groups offline_access",
			expectFetch:   true,
		},
		{
			name:         "oidc, offline_access not supported",
			providerType: options.OIDCProvider,
			status:       http.StatusOK,
			body:         withoutOffline,
			expected:     "",
			expectFetch:  true,
		},
		{
			name:         "oidc, no scopes_supported",
			providerType: options.OIDCProvider,
			status:       http.StatusOK,
			body:         `{}`,
			expected:     "",
			expectFetch:  true,
		},
		{
			name:         "oidc, discovery fails",
			providerType: options.OIDCProvider,
			status:       http.StatusInternalServerError,
			body:         `{}`,
			expected:     "",
			expectFetch:  true,
		},
		{
			name:         "gitlab is not asked",
			providerType: options.GitLabProvider,
			status:       http.StatusOK,
			body:         withOffline,
			expected:     "",
			expectFetch:  false,
		},
		{
			name:         "github is not asked",
			providerType: options.GitHubProvider,
			status:       http.StatusOK,
			body:         withOffline,
			expected:     "",
			expectFetch:  false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var fetched atomic.Bool
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path != "/issuer/.well-known/openid-configuration" {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				fetched.Store(true)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(srv.Close)

			p := options.Provider{
				Type:          tc.providerType,
				AllowedGroups: tc.allowedGroups,
				OIDCConfig:    options.OIDCOptions{IssuerURL: srv.URL + "/issuer/"},
			}

			r := newScopeResolver(log.NewDebugLogger(), client.New())
			assert.Equal(t, tc.expected, r.scope(t.Context(), p))
			assert.Equal(t, tc.expectFetch, fetched.Load())
		})
	}
}
