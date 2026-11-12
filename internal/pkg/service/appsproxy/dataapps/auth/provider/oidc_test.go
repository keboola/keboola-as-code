package provider

import (
	"encoding/json"
	"testing"

	proxyOptions "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOIDC(t *testing.T) {
	t.Parallel()

	// Mock part of the API response
	providerJSON := `
{
  "id": "my-id",
  "name": "My Name",
  "type": "oidc",
  "clientId": "6779ef20e75817b79602",
  "clientSecret": "f2a1ed52710d4533bde25be6da03b6e3",
  "issuerUrl": "https://www.linkedin.com",
  "logoutUrl": "https://www.linkedin.com/oidc/logout",
  "allowedRoles": ["admin"]
}
`

	// Unmarshal, detect the target struct
	var providers Providers
	require.NoError(t, json.Unmarshal([]byte("["+providerJSON+"]"), &providers))
	require.Len(t, providers, 1)

	// Decoded content
	provider := providers[0]
	assert.Equal(t, OIDC{
		Base: Base{
			Info: Info{
				ID:   "my-id",
				Name: "My Name",
				Type: TypeOIDC,
			},
		},
		ClientID:     "6779ef20e75817b79602",
		ClientSecret: "f2a1ed52710d4533bde25be6da03b6e3",
		IssuerURL:    "https://www.linkedin.com",
		LogoutURL:    "https://www.linkedin.com/oidc/logout",
		AllowedRoles: &[]string{"admin"},
	}, provider)

	// OAuth2Proxy configuration
	oAuth2ProxyProvider, ok := provider.(OIDC)
	require.True(t, ok)
	proxyOpts, err := oAuth2ProxyProvider.ProxyProviderOptions()
	require.NoError(t, err)
	assert.Equal(t, proxyOptions.Provider{
		ID:                  "my-id",
		Type:                "oidc",
		Name:                "My Name",
		CodeChallengeMethod: "S256",
		Scope:               "openid email profile",
		ClientID:            "6779ef20e75817b79602",
		ClientSecret:        "f2a1ed52710d4533bde25be6da03b6e3",
		BackendLogoutURL:    "https://www.linkedin.com/oidc/logout",
		AllowedGroups:       []string{"admin"},
		AdditionalClaims:    []string{"sub", "email_verified"},
		OIDCConfig: proxyOptions.OIDCOptions{
			IssuerURL:      "https://www.linkedin.com",
			EmailClaim:     "email",
			GroupsClaim:    "groups",
			AudienceClaims: []string{"aud"},
			UserIDClaim:    "email",
		},
		LoginURLParameters: []proxyOptions.LoginURLParameter{
			{
				Name:    "prompt",
				Default: []string{"select_account"},
			},
		},
	}, proxyOpts)
}

func TestOIDC_AllowedRoles(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		allowedRoles   *[]string
		expectedGroups []string
		deniesAll      bool
	}{
		{name: "nil: no restriction"},
		{name: "empty: deny all", allowedRoles: &[]string{}, deniesAll: true},
		{name: "roles", allowedRoles: &[]string{"admin"}, expectedGroups: []string{"admin"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := OIDC{AllowedRoles: tc.allowedRoles}
			opts, err := p.ProxyProviderOptions()
			require.NoError(t, err)
			assert.Equal(t, tc.expectedGroups, opts.AllowedGroups)
			assert.Equal(t, tc.deniesAll, p.DeniesAll())
		})
	}
}

func TestOIDC_Scopes(t *testing.T) {
	t.Parallel()

	opts, err := OIDC{Scopes: []string{"groups", " ", "openid email"}, AllowedRoles: &[]string{"admin"}}.ProxyProviderOptions()
	require.NoError(t, err)
	assert.Equal(t, "openid email profile groups", opts.Scope)
}

func TestGitLab_IgnoresAllowedRoles(t *testing.T) {
	t.Parallel()

	p := GitLab{OIDC: OIDC{AllowedRoles: &[]string{}}, Groups: []string{"group"}}
	assert.False(t, p.DeniesAll())
	opts, err := p.ProxyProviderOptions()
	require.NoError(t, err)
	assert.Empty(t, opts.Scope)
}
