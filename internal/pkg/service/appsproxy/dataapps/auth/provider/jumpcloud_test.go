package provider

import (
	"encoding/json"
	"testing"

	proxyOptions "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJumpCloud(t *testing.T) {
	t.Parallel()

	// Mock part of the API response
	providerJSON := `
{
  "id": "my-id",
  "name": "My Name",
  "type": "jumpcloud",
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
	assert.Equal(t, JumpCloud{
		OIDC: OIDC{
			Base: Base{
				Info: Info{
					ID:   "my-id",
					Name: "My Name",
					Type: TypeJumpCloud,
				},
			},
			ClientID:     "6779ef20e75817b79602",
			ClientSecret: "f2a1ed52710d4533bde25be6da03b6e3",
			IssuerURL:    "https://www.linkedin.com",
			LogoutURL:    "https://www.linkedin.com/oidc/logout",
			AllowedRoles: &[]string{"admin"},
		},
	}, provider)

	// OAuth2Proxy configuration
	oAuth2ProxyProvider, ok := provider.(JumpCloud)
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
				Default: []string{"login"},
			},
		},
	}, proxyOpts)
}
