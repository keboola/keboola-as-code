package provider

import (
	"encoding/json"
	"testing"

	proxyOptions "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGitLab(t *testing.T) {
	t.Parallel()

	// Mock part of the API response
	providerJSON := `
{
  "id": "my-id",
  "name": "My Name",
  "type": "gitlab",
  "clientId": "6779ef20e75817b79602",
  "clientSecret": "f2a1ed52710d4533bde25be6da03b6e3",
  "issuerUrl": "https://gitlab.com",
  "allowedRoles": ["admin"],
  "groups": ["group1", "group2"],
  "projects": ["project1", "project2"]
}
`

	// Unmarshal, detect the target struct
	var providers Providers
	require.NoError(t, json.Unmarshal([]byte("["+providerJSON+"]"), &providers))
	require.Len(t, providers, 1)

	// Decoded content
	provider := providers[0]
	assert.Equal(t, GitLab{
		OIDC: OIDC{
			Base: Base{
				Info: Info{
					ID:   "my-id",
					Name: "My Name",
					Type: TypeGitLab,
				},
			},
			ClientID:     "6779ef20e75817b79602",
			ClientSecret: "f2a1ed52710d4533bde25be6da03b6e3",
			IssuerURL:    "https://gitlab.com",
			AllowedRoles: &[]string{"admin"},
		},
		Groups:   []string{"group1", "group2"},
		Projects: []string{"project1", "project2"},
	}, provider)

	// OAuth2Proxy configuration
	oAuth2ProxyProvider, ok := provider.(GitLab)
	require.True(t, ok)
	proxyOpts, err := oAuth2ProxyProvider.ProxyProviderOptions()
	require.NoError(t, err)
	assert.Equal(t, proxyOptions.Provider{
		ID:                  "my-id",
		Type:                "gitlab",
		Name:                "My Name",
		CodeChallengeMethod: "S256",
		ClientID:            "6779ef20e75817b79602",
		ClientSecret:        "f2a1ed52710d4533bde25be6da03b6e3",
		AllowedGroups:       []string{"admin"},
		OIDCConfig: proxyOptions.OIDCOptions{
			IssuerURL:      "https://gitlab.com",
			EmailClaim:     "email",
			GroupsClaim:    "groups",
			AudienceClaims: []string{"aud"},
			UserIDClaim:    "email",
		},
		GitLabConfig: proxyOptions.GitLabOptions{
			Group:    []string{"group1", "group2"},
			Projects: []string{"project1", "project2"},
		},
		LoginURLParameters: []proxyOptions.LoginURLParameter{
			{
				Name:    "prompt",
				Default: []string{"login"},
			},
		},
	}, proxyOpts)
}
