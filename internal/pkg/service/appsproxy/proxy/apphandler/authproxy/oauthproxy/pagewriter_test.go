package oauthproxy

import (
	"net/http"
	"net/http/httptest"
	"testing"

	middlewareapi "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/middleware"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/sessions"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
)

func TestSessionDeniedReason(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		scope    *middlewareapi.RequestScope
		expected pagewriter.AccessDeniedReason
	}{
		{name: "no scope", expected: pagewriter.AccessDeniedUnknown},
		{name: "no session", scope: &middlewareapi.RequestScope{}, expected: pagewriter.AccessDeniedUnknown},
		{
			name:     "session without groups",
			scope:    &middlewareapi.RequestScope{Session: &sessions.SessionState{Email: "a@b.c"}},
			expected: pagewriter.AccessDeniedNoGroupsClaim,
		},
		{
			name:     "session with other groups",
			scope:    &middlewareapi.RequestScope{Session: &sessions.SessionState{Email: "a@b.c", Groups: []string{"x"}}},
			expected: pagewriter.AccessDeniedNotInGroups,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
			if tc.scope != nil {
				req = middlewareapi.AddRequestScope(req, tc.scope)
			}
			assert.Equal(t, tc.expected, sessionDeniedReason(req))
		})
	}
}

func TestIsAuthorizationError(t *testing.T) {
	t.Parallel()

	assert.True(t, isAuthorizationError("Invalid session: unauthorized"))
	assert.True(t, isAuthorizationError("The session failed authorization checks"))
	assert.False(t, isAuthorizationError("CSRF token mismatch, potential attack"))
	assert.False(t, isAuthorizationError("Session validation failed"))
	assert.False(t, isAuthorizationError(""))
}
