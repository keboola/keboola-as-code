package oauthproxy

import (
	"testing"

	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
)

func TestDeniedReason(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		providerType options.ProviderType
		groups       []string
		want         pagewriter.AccessDeniedReason
	}{
		{name: "oidc with groups", providerType: options.OIDCProvider, groups: []string{"sales"}, want: pagewriter.AccessDeniedNotInGroups},
		{name: "oidc without groups", providerType: options.OIDCProvider, want: pagewriter.AccessDeniedNoGroupsClaim},
		{name: "gitlab with groups", providerType: options.GitLabProvider, groups: []string{"sales"}, want: pagewriter.AccessDeniedNotAllowed},
		{name: "gitlab without groups", providerType: options.GitLabProvider, want: pagewriter.AccessDeniedNotAllowed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, deniedReason(tc.providerType, tc.groups))
		})
	}
}
