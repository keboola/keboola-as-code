package oauthproxy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
)

func TestUserIDClaim(t *testing.T) {
	t.Parallel()

	cases := []struct {
		providerType provider.Type
		expected     string
	}{
		// GitHub returns no ID token, so "sub" would always resolve to nothing
		// and the session would carry no identity at all.
		{provider.TypeGitHub, "user"},
		// Everything else is OIDC underneath: GitLab and JumpCloud embed the
		// OIDC provider, so they get a subject claim like plain OIDC does.
		{provider.TypeOIDC, "sub"},
		{provider.TypeGitLab, "sub"},
		{provider.TypeJumpCloud, "sub"},
		// A shared password identifies nobody. The claim is irrelevant, but it
		// must not fall back to something that names the user.
		{provider.TypeBasic, "sub"},
	}

	for _, tc := range cases {
		t.Run(string(tc.providerType), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, userIDClaim(tc.providerType))
		})
	}
}

func TestUserIDClaim_NeverTheEmail(t *testing.T) {
	t.Parallel()

	// Session tracking records who the provider says the user is, never their
	// e-mail address. oauth2-proxy defaults its own user id claim to "email",
	// so a claim named here by accident would quietly reintroduce it.
	for _, providerType := range []provider.Type{
		provider.TypeOIDC,
		provider.TypeGitLab,
		provider.TypeGitHub,
		provider.TypeJumpCloud,
		provider.TypeBasic,
	} {
		assert.NotEqual(t, "email", userIDClaim(providerType), providerType)
	}
}
