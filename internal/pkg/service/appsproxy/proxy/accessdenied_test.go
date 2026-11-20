package proxy_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func authorizePrompt(t *testing.T, resp *http.Response) string {
	t.Helper()
	require.Equal(t, http.StatusFound, resp.StatusCode)
	location, err := url.Parse(resp.Header.Get("Location"))
	require.NoError(t, err)
	require.Contains(t, location.Path, "/oidc/authorize")
	return location.Query().Get("prompt")
}

func TestSignIn_PromptOverride(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)

	assert.Equal(t, "login", authorizePrompt(t, env.send(env.client, oidcOrigin+"/_proxy/sign_in?prompt=login")))
	assert.Equal(t, "select_account", authorizePrompt(t, env.send(env.client, oidcOrigin+"/_proxy/sign_in?prompt=none")))
	assert.Equal(t, "select_account", authorizePrompt(t, env.send(env.client, oidcOrigin+"/_proxy/sign_in")))
}
