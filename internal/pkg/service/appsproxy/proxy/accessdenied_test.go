package proxy_test

import (
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const multiOrigin = "https://multi.hub.keboola.local"

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

func TestSelector_PassesPromptToProvider(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)

	resp := env.send(env.client, multiOrigin+"/_proxy/selection?prompt=login")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, readBody(t, resp), htmlLinkTo(multiOrigin+"/_proxy/selection?prompt=login&provider=oidc0"))

	env.syncClock()
	jar, err := cookiejar.New(nil)
	require.NoError(t, err)
	client := *env.client
	client.Jar = jar
	resp = env.send(&client, multiOrigin+"/_proxy/selection?prompt=login&provider=oidc0")
	require.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, multiOrigin+"/_proxy/sign_in?prompt=login", resp.Header.Get("Location"))
	assert.Equal(t, "login", authorizePrompt(t, env.send(&client, resp.Header.Get("Location"))))

	resp = env.send(env.client, multiOrigin+"/_proxy/selection?prompt=none&provider=oidc0")
	require.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, multiOrigin+"/_proxy/sign_in", resp.Header.Get("Location"))
}
