package provider

import (
	"encoding/json"
	"strings"
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

func TestOIDC_AttributeClaims(t *testing.T) {
	t.Parallel()

	tooMany := make([]string, 101)
	for i := range tooMany {
		tooMany[i] = "claim" + strings.Repeat("x", i)
	}
	hundred := tooMany[:100]

	cases := []struct {
		name    string
		claims  []string
		want    []string
		wantErr string
	}{
		{name: "none", claims: nil, want: nil},
		{name: "empty", claims: []string{}, want: nil},
		{name: "the order is kept", claims: []string{"department", "country", "https://example.com/team"}, want: []string{"department", "country", "https://example.com/team"}},
		{name: "a hundred", claims: hundred, want: hundred},
		{name: "more than a hundred", claims: tooMany, wantErr: "too many attribute claims"},
		{name: "a reserved name", claims: []string{"department", "email"}, wantErr: "bad or reserved"},
		{name: "a reserved name in another case", claims: []string{"Groups"}, wantErr: "bad or reserved"},
		{name: "a bad character", claims: []string{"depart ment"}, wantErr: "bad or reserved"},
		{name: "an empty name", claims: []string{""}, wantErr: "bad or reserved"},
		{name: "a too long name", claims: []string{strings.Repeat("a", 129)}, wantErr: "bad or reserved"},
		{name: "a duplicate", claims: []string{"department", "country", "department"}, wantErr: "duplicate"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := OIDC{AttributeClaims: tc.claims}.ValidAttributeClaims()
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestOIDC_AttributeClaims_ProxyProviderOptions(t *testing.T) {
	t.Parallel()

	t.Run("the allowlist is extracted at login, after the claims that apps-proxy always needs", func(t *testing.T) {
		t.Parallel()
		opts, err := OIDC{IssuerURL: "https://idp.example.com", AttributeClaims: []string{"department", "country"}}.ProxyProviderOptions()
		require.NoError(t, err)
		assert.Equal(t, []string{"sub", "email_verified", "department", "country"}, opts.AdditionalClaims)
	})

	t.Run("a bad allowlist adds nothing and does not break the provider", func(t *testing.T) {
		t.Parallel()
		opts, err := OIDC{IssuerURL: "https://idp.example.com", AttributeClaims: []string{"department", "email"}}.ProxyProviderOptions()
		require.NoError(t, err)
		assert.Equal(t, []string{"sub", "email_verified"}, opts.AdditionalClaims)
	})
}

func TestOIDC_AttributeClaims_JSON(t *testing.T) {
	t.Parallel()

	var providers Providers
	require.NoError(t, json.Unmarshal([]byte(`[{
  "id": "my-id", "name": "My Name", "type": "oidc", "clientId": "c", "clientSecret": "s", "issuerUrl": "https://idp.example.com",
  "attributeClaims": ["department", "country"]
}]`), &providers))
	require.Len(t, providers, 1)

	claims, err := providers[0].(AttributeClaimsProvider).ValidAttributeClaims()
	require.NoError(t, err)
	assert.Equal(t, []string{"department", "country"}, claims)
}
