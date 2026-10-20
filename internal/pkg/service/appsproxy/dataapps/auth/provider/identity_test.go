package provider

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOAuthProvider_IdentityProvider(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		provider    OAuthProvider
		expectedURL string
	}{
		{
			name:        "oidc",
			provider:    OIDC{IssuerURL: "https://login.example.com/tenant"},
			expectedURL: "https://login.example.com/tenant",
		},
		{
			name:        "oidc trailing slash",
			provider:    OIDC{IssuerURL: "https://login.example.com/tenant/"},
			expectedURL: "https://login.example.com/tenant",
		},
		{
			name:        "jumpcloud",
			provider:    JumpCloud{OIDC: OIDC{IssuerURL: "https://oauth.id.jumpcloud.com/"}},
			expectedURL: "https://oauth.id.jumpcloud.com",
		},
		{
			name:        "gitlab",
			provider:    GitLab{OIDC: OIDC{IssuerURL: "https://gitlab.com/"}},
			expectedURL: "https://gitlab.com",
		},
		{
			name:        "github",
			provider:    GitHub{},
			expectedURL: "https://github.com",
		},
		{
			name:        "github enterprise",
			provider:    GitHub{URL: "https://github.example.com/"},
			expectedURL: "https://github.example.com",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expectedURL, tc.provider.IdentityProviderURL())
		})
	}
}

func TestOIDC_RequestsEmailVerifiedClaim(t *testing.T) {
	t.Parallel()

	for _, p := range []OAuthProvider{OIDC{}, JumpCloud{}, GitLab{}} {
		opts, err := p.ProxyProviderOptions()
		require.NoError(t, err)
		assert.Contains(t, opts.AdditionalClaims, "email_verified")
	}
}
