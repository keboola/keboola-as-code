package provider

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOAuthProvider_RefreshTokensSingleUse(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		provider OAuthProvider
		expected bool
	}{
		{name: "oidc", provider: OIDC{}, expected: false},
		{name: "jumpcloud", provider: JumpCloud{}, expected: false},
		{name: "github", provider: GitHub{}, expected: false},
		// See docs/apps-proxy/login-session.md, section "GitLab: no refresh".
		{name: "gitlab", provider: GitLab{}, expected: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, tc.provider.RefreshTokensSingleUse())
		})
	}
}
