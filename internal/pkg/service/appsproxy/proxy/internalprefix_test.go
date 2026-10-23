package proxy_test

import (
	"net/http"
	"net/http/cookiejar"
	"strings"
	"testing"

	"github.com/oauth2-proxy/mockoidc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
)

const publicOrigin = "https://public-123.hub.keboola.local"

func (e *previewEnv) assertInternalNotFound(client *http.Client, target string, headers ...string) {
	e.t.Helper()
	before := len(*e.appServer.Requests)
	resp := e.send(client, target, headers...)
	assert.Equal(e.t, http.StatusNotFound, resp.StatusCode, target)
	assert.NotEqual(e.t, "Hello, client", readBody(e.t, resp), target)
	assert.Len(e.t, *e.appServer.Requests, before, "%s must not reach the app", target)
}

func (e *previewEnv) basicAuthLogin() *http.Client {
	e.t.Helper()
	e.syncClock()
	jar, err := cookiejar.New(nil)
	require.NoError(e.t, err)
	client := *e.client
	client.Jar = jar

	req, err := http.NewRequestWithContext(e.t.Context(), http.MethodPost, authOrigin+"/", strings.NewReader("password=abc"))
	require.NoError(e.t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	require.NoError(e.t, err)
	require.NoError(e.t, resp.Body.Close())
	require.Equal(e.t, http.StatusMovedPermanently, resp.StatusCode)
	return &client
}

func TestInternalPrefix_UnknownPathNotForwarded(t *testing.T) {
	t.Parallel()

	wsUpgrade := []string{"Connection", "Upgrade", "Upgrade", "websocket", "Sec-WebSocket-Version", "13", "Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="}

	t.Run("OAuth app, logged in", func(t *testing.T) {
		t.Parallel()
		env := startPreviewEnv(t)
		env.syncClock()
		client := env.oidcLogin(env.providers[0], &mockoidc.MockUser{Subject: "sub-1", Email: "a@example.com", Groups: []string{"admin"}}, oidcOrigin+"/")
		require.Equal(t, http.StatusOK, env.send(client, oidcOrigin+"/").StatusCode)

		env.assertInternalNotFound(client, oidcOrigin+"/_proxy/anything")
		env.assertInternalNotFound(client, oidcOrigin+"/_proxy/form")
		env.assertInternalNotFound(client, oidcOrigin+"/_proxy/anything", wsUpgrade...)
		env.assertInternalNotFound(client, oidcOrigin+"/a/%2E%2E/_proxy/anything")
		env.assertInternalNotFound(client, oidcOrigin+"/%2F_proxy/anything")
		env.assertInternalNotFound(client, oidcOrigin+"/_PROXY/anything")
		env.assertInternalNotFound(client, oidcOrigin+"/_proxy;x")
		env.assertInternalNotFound(client, oidcOrigin+"/_proxy/%2E%2E/x")
		env.assertInternalNotFound(client, oidcOrigin+"/_proxy%2F..%2Fx")
	})

	t.Run("OAuth app, anonymous", func(t *testing.T) {
		t.Parallel()
		env := startPreviewEnv(t)
		env.assertInternalNotFound(env.client, oidcOrigin+"/_proxy/anything")
	})

	t.Run("basic-auth app, logged in", func(t *testing.T) {
		t.Parallel()
		env := startPreviewEnv(t)
		client := env.basicAuthLogin()
		require.Equal(t, http.StatusOK, env.send(client, authOrigin+"/").StatusCode)

		env.assertInternalNotFound(client, authOrigin+"/_proxy/anything")
		env.assertInternalNotFound(client, authOrigin+"/_proxy/sign_in")
		env.assertInternalNotFound(client, authOrigin+"/_proxy/callback?continue_auth=true")
	})

	t.Run("basic-auth app, anonymous", func(t *testing.T) {
		t.Parallel()
		env := startPreviewEnv(t)
		env.assertInternalNotFound(env.client, authOrigin+"/_proxy/anything")
	})

	t.Run("app without providers", func(t *testing.T) {
		t.Parallel()
		env := startPreviewEnv(t)
		require.Equal(t, http.StatusOK, env.send(env.client, publicOrigin+"/").StatusCode)

		env.assertInternalNotFound(env.client, publicOrigin+"/_proxy")
		env.assertInternalNotFound(env.client, publicOrigin+"/_proxy/anything")
		env.assertInternalNotFound(env.client, publicOrigin+"/_proxy/kai-preview/refresh")
		env.assertInternalNotFound(env.client, publicOrigin+"/_proxy/anything", wsUpgrade...)
	})

	t.Run("app without providers, preview session", func(t *testing.T) {
		t.Parallel()
		env := startPreviewEnv(t)
		env.setDevMode("app-devmode", k8sapp.WorkloadRef{AppID: "devmode"}, true)
		env.waitForKeys(devmodeHost, "https://"+devmodeHost)
		cookie := env.session(devmodeHost, "https://"+devmodeHost)
		require.Equal(t, "Hello, client", readBody(t, env.get(devmodeHost, "/", cookie, nil)))

		before := len(*env.appServer.Requests)
		resp := env.get(devmodeHost, "/_proxy/anything", cookie, nil)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
		assert.Len(t, *env.appServer.Requests, before)

		resp = env.get(devmodeHost, "/_proxy/sign_out", cookie, nil)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
		assert.Len(t, *env.appServer.Requests, before, "sign-out never reaches the app")
		cleared := previewCookie(resp)
		require.NotNil(t, cleared, "sign-out clears the preview cookie")
		assert.Negative(t, cleared.MaxAge)
	})
}

func TestInternalPrefix_KnownAuthPathsServed(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)

	for origin, paths := range map[string][]string{
		oidcOrigin: {
			"/_proxy/selection",
			"/_proxy/callback",
			"/_proxy/sign_in",
			"/_proxy/start",
			"/_proxy/userinfo",
			"/_proxy/auth",
			"/_proxy/sign_out",
			"/_proxy/static/css/bulma.min.css",
		},
		authOrigin: {
			"/_proxy/form",
			"/_proxy/sign_out",
		},
	} {
		for _, path := range paths {
			before := len(*env.appServer.Requests)
			resp := env.send(env.client, origin+path)
			assert.NotEqual(t, http.StatusNotFound, resp.StatusCode, origin+path)
			assert.Len(t, *env.appServer.Requests, before, "%s must not reach the app", origin+path)
		}
	}
}

func TestInternalPrefix_LookalikePathForwarded(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)

	resp := env.send(env.client, publicOrigin+"/_proxyfoo")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "/_proxyfoo", env.lastAppRequest().URL.Path)
}

func TestInternalPrefix_MuxPathsNotForwarded(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)

	for _, path := range []string{
		"/_proxy/api/unknown",
		"/_proxy/api/v1/unknown",
		"/_proxy/api/v1/webhook/e2b",
		"/_proxy/assets/unknown",
	} {
		env.assertInternalNotFound(env.client, publicOrigin+path)
	}
}

func TestInternalPrefix_LookalikePathGetsSelector(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)

	for _, path := range []string{"/some/path", "/_proxyfoo"} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://multi.hub.keboola.local"+path, nil)
		require.NoError(t, err)
		req.AddCookie(&http.Cookie{Name: "_oauth2_provider", Value: "oidc1"})
		resp := env.do(req)
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, path)
		assert.Contains(t, readBody(t, resp), "_proxy/selection?provider=oidc0", path)
	}
}
