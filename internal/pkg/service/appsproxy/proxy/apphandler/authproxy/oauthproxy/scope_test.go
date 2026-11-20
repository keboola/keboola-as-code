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

	const (
		base       = `"openid","email","profile"`
		full       = `{"scopes_supported":[` + base + `,"groups","offline_access"]}`
		withOffl   = `{"scopes_supported":[` + base + `,"offline_access"]}`
		withGroups = `{"scopes_supported":[` + base + `,"groups"]}`
		plain      = `{"scopes_supported":[` + base + `]}`
	)

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
			name:         "no allowed groups, nothing optional listed",
			providerType: options.OIDCProvider,
			status:       http.StatusOK,
			body:         plain,
			expected:     "openid email profile",
			expectFetch:  true,
		},
		{
			name:         "no allowed groups, groups listed",
			providerType: options.OIDCProvider,
			status:       http.StatusOK,
			body:         withGroups,
			expected:     "openid email profile",
			expectFetch:  true,
		},
		{
			name:         "no allowed groups, offline_access listed",
			providerType: options.OIDCProvider,
			status:       http.StatusOK,
			body:         withOffl,
			expected:     "openid email profile offline_access",
			expectFetch:  true,
		},
		{
			name:          "allowed groups, groups listed",
			providerType:  options.OIDCProvider,
			allowedGroups: []string{"admin"},
			status:        http.StatusOK,
			body:          withGroups,
			expected:      "openid email profile groups",
			expectFetch:   true,
		},
		{
			name:          "allowed groups, groups and offline_access listed",
			providerType:  options.OIDCProvider,
			allowedGroups: []string{"admin"},
			status:        http.StatusOK,
			body:          full,
			expected:      "openid email profile groups offline_access",
			expectFetch:   true,
		},
		{
			name:          "allowed groups, groups not listed",
			providerType:  options.OIDCProvider,
			allowedGroups: []string{"admin"},
			status:        http.StatusOK,
			body:          plain,
			expected:      "openid email profile",
			expectFetch:   true,
		},
		{
			name:          "allowed groups, groups not listed, offline_access listed",
			providerType:  options.OIDCProvider,
			allowedGroups: []string{"admin"},
			status:        http.StatusOK,
			body:          withOffl,
			expected:      "openid email profile offline_access",
			expectFetch:   true,
		},
		{
			name:          "allowed groups, no scopes_supported",
			providerType:  options.OIDCProvider,
			allowedGroups: []string{"admin"},
			status:        http.StatusOK,
			body:          `{}`,
			expected:      "openid email profile",
			expectFetch:   true,
		},
		{
			name:          "allowed groups, discovery fails",
			providerType:  options.OIDCProvider,
			allowedGroups: []string{"admin"},
			status:        http.StatusInternalServerError,
			body:          `{}`,
			expected:      "openid email profile",
			expectFetch:   true,
		},
		{
			name:         "github is not asked",
			providerType: options.GitHubProvider,
			status:       http.StatusOK,
			body:         full,
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
