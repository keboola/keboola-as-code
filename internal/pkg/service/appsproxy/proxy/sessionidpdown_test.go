package proxy_test

import (
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/oauth2-proxy/mockoidc"
	sessionsapi "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/sessions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// queueTokenError fails the next refresh: the oauth2 client retries a failed token request once with the other client auth style.
func queueTokenError(m *mockoidc.MockOIDC, se *mockoidc.ServerError) {
	m.QueueError(se)
	m.QueueError(se)
}

func ageSessionWithExpiredTokens(s *sessionsapi.SessionState) {
	s.CreatedAt = ago(2 * time.Hour)
	s.ExpiresOn = ago(time.Hour)
}

func TestSessionFreshness_IdPDownKeepsSessionAndShows503(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)
	client := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")
	env.rewriteOIDCSession(client, func(s *sessionsapi.SessionState) {
		require.NotEmpty(t, s.RefreshToken)
		ageSessionWithExpiredTokens(s)
	})
	before := env.loadOIDCSession(client)

	queueTokenError(env.providers[0], &mockoidc.ServerError{Code: http.StatusServiceUnavailable, Error: "temporarily_unavailable"})
	resp := env.send(client, oidcOrigin+"/some/path")
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Contains(t, string(body), "The identity provider is temporarily unavailable. Please try again in a moment.")
	exceptionID := regexp.MustCompile(`id="exceptionId">([^<]+)<`).FindStringSubmatch(string(body))
	require.Len(t, exceptionID, 2)
	assert.Contains(t, env.mocked.DebugLogger().AllMessages(), `"exceptionId":"`+exceptionID[1]+`"`)
	for _, name := range setCookieNames(resp) {
		assert.False(t, strings.HasPrefix(name, oauthCookieName), "unexpected Set-Cookie %q", name)
	}
	kept := env.loadOIDCSession(client)
	assert.Equal(t, before.RefreshToken, kept.RefreshToken)
	assert.Equal(t, before.CreatedAt.Unix(), kept.CreatedAt.Unix())

	// The IdP is back: the same cookie refreshes and the request is served.
	resp = env.send(client, oidcOrigin+"/some/path")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, setCookieNames(resp), oauthCookieName)
	assert.WithinDuration(t, time.Now(), *env.loadOIDCSession(client).CreatedAt, time.Minute)
}

func TestSessionFreshness_InvalidGrantAtRefreshLogsOut(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)
	client := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")
	env.rewriteOIDCSession(client, ageSessionWithExpiredTokens)

	queueTokenError(env.providers[0], &mockoidc.ServerError{Code: http.StatusBadRequest, Error: mockoidc.InvalidGrant})
	resp := env.send(client, oidcOrigin+"/some/path")

	require.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Location"), "/oidc/authorize?client_id=")
	assert.Contains(t, setCookieNames(resp), oauthCookieName)
}
