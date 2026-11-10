package oauthproxy

import (
	"testing"
	"time"

	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
)

func TestUserIDClaim(t *testing.T) {
	t.Parallel()

	cases := []struct {
		providerType options.ProviderType
		expected     string
	}{
		// GitHub returns no ID token, so "sub" would resolve to nothing and the
		// session would carry no identity at all. Its login lives in the
		// session user field instead.
		{options.GitHubProvider, "user"},
		// Everything the Sandboxes API can configure is OIDC underneath —
		// GitLab and JumpCloud embed our OIDC provider — so a subject claim is
		// there to take.
		{options.OIDCProvider, "sub"},
		{options.GitLabProvider, "sub"},
		// Anything not configurable today still has to land on the subject
		// rather than on whatever a provider happens to expose.
		{options.GoogleProvider, "sub"},
		{"", "sub"},
	}

	for _, tc := range cases {
		t.Run(string(tc.providerType), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, userIDClaim(tc.providerType))
		})
	}
}

func TestUserIDClaim_NeverNamesTheEmailClaim(t *testing.T) {
	t.Parallel()

	// Session events must not carry the user's e-mail address, so no provider
	// may resolve its user id through the e-mail claim. This checks the claim
	// we ask for, not the value that comes back: an OIDC issuer is free to use
	// the e-mail address as its subject, and that is outside our control.
	for _, providerType := range []options.ProviderType{
		options.OIDCProvider,
		options.GitLabProvider,
		options.GitHubProvider,
		options.GoogleProvider,
		"",
	} {
		assert.NotEqual(t, options.OIDCEmailClaim, userIDClaim(providerType), providerType)
	}
}

func TestAttrsSessionTTL(t *testing.T) {
	t.Parallel()

	cfg := config.New()
	cfg.AppUserToken.AttrsSessionTTL = 3 * time.Hour

	cases := []struct {
		name     string
		provider provider.OAuthProvider
		expected time.Duration
	}{
		{name: "an OIDC provider with an allowlist", provider: provider.OIDC{AttributeClaims: []string{"department"}}, expected: 3 * time.Hour},
		{name: "an OIDC provider without an allowlist keeps the default session", provider: provider.OIDC{}, expected: 0},
		{name: "an empty allowlist", provider: provider.OIDC{AttributeClaims: []string{}}, expected: 0},
		{name: "a bad allowlist is not used, so the session is not shortened", provider: provider.OIDC{AttributeClaims: []string{"email"}}, expected: 0},
		{name: "a provider that has no allowlist", provider: provider.GitHub{}, expected: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, attrsSessionTTL(cfg, tc.provider))
		})
	}
}
