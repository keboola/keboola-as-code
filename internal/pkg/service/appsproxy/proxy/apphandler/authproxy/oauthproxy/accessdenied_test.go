package oauthproxy

import (
	"testing"

	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
)

func TestDeniedReason(t *testing.T) {
	t.Parallel()

	assert.Equal(t, pagewriter.AccessDeniedNotInGroups, deniedReason(options.OIDCProvider, []string{"sales"}))
	assert.Equal(t, pagewriter.AccessDeniedNoGroupsClaim, deniedReason(options.OIDCProvider, nil))
	assert.Equal(t, pagewriter.AccessDeniedNotAllowed, deniedReason(options.GitLabProvider, []string{"sales"}))
	assert.Equal(t, pagewriter.AccessDeniedNotAllowed, deniedReason(options.GitLabProvider, nil))
}
