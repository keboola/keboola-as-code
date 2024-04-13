package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOIDCProvider(t *testing.T) {
	t.Parallel()

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

	// Check content
	provider := providers[0]
	assert.Equal(t, OIDCProvider{
		BaseProvider: BaseProvider{
			ProviderInfo: ProviderInfo{
				ID:   "my-id",
				Name: "My Name",
				Type: ProviderTypeOIDC,
			},
		},
		ClientID:     "6779ef20e75817b79602",
		ClientSecret: "f2a1ed52710d4533bde25be6da03b6e3",
		IssuerURL:    "https://www.linkedin.com",
		LogoutURL:    "https://www.linkedin.com/oidc/logout",
		AllowedRoles: &[]string{"admin"},
	}, provider)
}
