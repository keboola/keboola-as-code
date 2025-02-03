package provider

import (
	"encoding/json"
	"testing"

	proxyOptions "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGitHub(t *testing.T) {
	t.Parallel()

	// Mock part of the API response
	providerJSON := `
{
  "id": "my-id",
  "name": "My Name",
  "type": "github",
  "organization": "my-org",
  "team": "my-team",
  "repository": "my-repo",
  "token": "my-token",
  "users": ["user1", "user2"]
}
`

	// Unmarshal, detect the target struct
	var providers Providers
	require.NoError(t, json.Unmarshal([]byte("["+providerJSON+"]"), &providers))
	require.Len(t, providers, 1)

	// Decoded content
	provider := providers[0]
	assert.Equal(t, GitHub{
		Base: Base{
			Info: Info{
				ID:   "my-id",
				Name: "My Name",
				Type: TypeGitHub,
			},
		},
		Organization: "my-org",
		Team:         "my-team",
		Repository:   "my-repo",
		Token:        "my-token",
		Users:        []string{"user1", "user2"},
	}, provider)

	// OAuth2Proxy configuration
	oAuth2ProxyProvider, ok := provider.(GitHub)
	require.True(t, ok)
	proxyOpts, err := oAuth2ProxyProvider.ProxyProviderOptions()
	require.NoError(t, err)
	assert.Equal(t, proxyOptions.Provider{
		ID:   "my-id",
		Type: "github",
		Name: "My Name",
		LoginURLParameters: []proxyOptions.LoginURLParameter{
			{
				Name:    "allow_signup",
				Default: []string{"false"},
			},
		},
		GitHubConfig: proxyOptions.GitHubOptions{
			Org:   "my-org",
			Team:  "my-team",
			Repo:  "my-repo",
			Token: "my-token",
			Users: []string{"user1", "user2"},
		},
	}, proxyOpts)
}
