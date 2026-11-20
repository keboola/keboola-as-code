package proxy_test

import (
	"encoding/json"
	"html"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"testing"

	"github.com/oauth2-proxy/mockoidc"
	sessionsapi "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/sessions"
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

const deniedEmail = "bob@example.com"

// loginUntilDenied follows the login from startURL to the response of the callback that continues the auth.
func (e *previewEnv) loginUntilDenied(provider *mockoidc.MockOIDC, user *mockoidc.MockUser, startURL string) *http.Response {
	e.t.Helper()
	e.syncClock()
	jar, err := cookiejar.New(nil)
	require.NoError(e.t, err)
	client := *e.client
	client.Jar = jar

	provider.QueueUser(user)
	resp := e.send(&client, startURL)
	for resp.StatusCode == http.StatusFound {
		resp = e.send(&client, resp.Header.Get("Location"))
	}
	require.Equal(e.t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(e.t, err)
	return e.send(&client, extractMetaRefreshTag(e.t, body))
}

func (e *previewEnv) accessDeniedLog() map[string]any {
	e.t.Helper()
	var found map[string]any
	for _, line := range strings.Split(e.mocked.DebugLogger().AllMessages(), "\n") {
		if !strings.Contains(line, `"message":"access denied"`) {
			continue
		}
		require.Nil(e.t, found, "one access denied log line expected")
		require.NoError(e.t, json.Unmarshal([]byte(line), &found))
	}
	require.NotNil(e.t, found, "access denied log line expected")
	return found
}

func assertDeniedPage(t *testing.T, body, guidance, switchURL string) {
	t.Helper()
	assert.Contains(t, body, "<title>Access denied</title>")
	assert.Contains(t, body, html.EscapeString("You don't have access to this app"))
	assert.Contains(t, body, "You are signed in as <b>"+deniedEmail+"</b>")
	assert.Contains(t, body, html.EscapeString(guidance))
	assert.Contains(t, body, `<a class="btn btn--inline" href="`+html.EscapeString(switchURL)+`">Sign in with a different account</a>`)
	assert.NotContains(t, body, "Invalid session")
	assert.NotContains(t, body, "Try again")
}

func assertNoGroups(t *testing.T, text string) {
	t.Helper()
	for _, group := range []string{"sales", "admin", "manager"} {
		assert.NotContains(t, text, group)
	}
}

func TestAccessDenied_NotInGroupsAtCallback(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)

	user := &mockoidc.MockUser{Subject: "sub-bob", Email: deniedEmail, EmailVerified: new(true), Groups: []string{"sales"}}
	resp := env.loginUntilDenied(env.providers[0], user, oidcOrigin+"/")
	body := readBody(t, resp)

	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	assertDeniedPage(t, body, "Your account is not in a group that is allowed to open this application.", "/_proxy/sign_in?prompt=login")
	assertNoGroups(t, body)
	assert.NotContains(t, setCookieNames(resp), oauthCookieName)
	assert.Equal(t, map[string]any{
		"proxy.app.id":          "oidc",
		"proxy.access.provider": "oidc",
		"proxy.access.email":    deniedEmail,
		"proxy.access.reason":   "not_in_groups",
		"proxy.access.stage":    "callback",
	}, pick(env.accessDeniedLog(), "proxy.app.id", "proxy.access.provider", "proxy.access.email", "proxy.access.reason", "proxy.access.stage"))
	assertNoGroups(t, env.mocked.DebugLogger().AllMessages())
}

func TestAccessDenied_NoGroupsClaimAtCallback(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)

	user := &mockoidc.MockUser{Subject: "sub-bob", Email: deniedEmail, EmailVerified: new(true)}
	resp := env.loginUntilDenied(env.providers[0], user, oidcOrigin+"/")
	body := readBody(t, resp)

	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	assertDeniedPage(t, body, "Your identity provider did not send any groups for your account, so your access could not be checked.", "/_proxy/sign_in?prompt=login")
	assert.Equal(t, "no_groups_claim", env.accessDeniedLog()["proxy.access.reason"])
}

func TestAccessDenied_ExistingSession(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)
	env.syncClock()
	user := &mockoidc.MockUser{Subject: "sub-bob", Email: deniedEmail, EmailVerified: new(true), Groups: []string{"admin"}}
	client := env.oidcLogin(env.providers[0], user, oidcOrigin+"/")
	env.rewriteOIDCSession(client, func(s *sessionsapi.SessionState) {
		s.Groups = []string{"sales"}
	})

	resp := env.send(client, oidcOrigin+"/some/path")
	body := readBody(t, resp)

	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	assertDeniedPage(t, body, "Your account is not in a group that is allowed to open this application.", "/_proxy/sign_in?prompt=login&rd=%2Fsome%2Fpath")
	assertNoGroups(t, body)
	assert.Contains(t, setCookieNames(resp), oauthCookieName)
	log := env.accessDeniedLog()
	assert.Equal(t, "session", log["proxy.access.stage"])
	assert.Equal(t, "not_in_groups", log["proxy.access.reason"])
	assert.Equal(t, deniedEmail, log["proxy.access.email"])
}

func TestAccessDenied_MultiProviderOffersSelection(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)

	user := &mockoidc.MockUser{Subject: "sub-bob", Email: deniedEmail, EmailVerified: new(true), Groups: []string{"sales"}}
	resp := env.loginUntilDenied(env.providers[0], user, multiOrigin+"/_proxy/selection?provider=oidc0")
	body := readBody(t, resp)

	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	assertDeniedPage(t, body, "Your account is not in a group that is allowed to open this application.", "/_proxy/selection?prompt=login")
	assert.Equal(t, "oidc0", env.accessDeniedLog()["proxy.access.provider"])
}

func pick(m map[string]any, keys ...string) map[string]any {
	out := make(map[string]any, len(keys))
	for _, k := range keys {
		out[k] = m[k]
	}
	return out
}
