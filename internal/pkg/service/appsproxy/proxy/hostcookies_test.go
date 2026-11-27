package proxy_test

import (
	"encoding/json"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/oauth2-proxy/mockoidc"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/encryption"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/sessions"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/basicauth"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/oauthproxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/previewtest"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/selector"
)

func hostCookieUser() *mockoidc.MockUser {
	return &mockoidc.MockUser{Subject: "sub-1", Email: "admin@keboola.com", EmailVerified: new(true), Groups: []string{"admin"}}
}

// legacyToHostNames returns a client whose jar holds the cookies of client under the host-only names,
// as a browser that was served by a pod writing them. Tickets are signed for the new name.
// issuedAt, when set, replaces the timestamp the ticket was signed with.
func (e *previewEnv) legacyToHostNames(client *http.Client, host string, issuedAt *time.Time) *http.Client {
	e.t.Helper()
	u := &url.URL{Scheme: "https", Host: host, Path: "/"}
	secret := e.oidcCookieSecret()

	existing := client.Jar.Cookies(u)
	out := make([]*http.Cookie, 0, len(existing))
	for _, c := range existing {
		name := c.Name
		value := c.Value
		switch name {
		case sessions.CookieName:
			name = sessions.HostCookieName
		case selector.ProviderCookie:
			name = selector.HostProviderCookie
		case basicauth.CookieName:
			name = basicauth.HostCookieName
		case oauthproxy.CookieName + "_ticket", oauthproxy.CookieName + "_ticket_csrf":
			newName := oauthproxy.HostSessionCookieName + strings.TrimPrefix(name, oauthproxy.CookieName+"_ticket")
			plain, ts, ok := encryption.Validate(c, secret, 24*time.Hour)
			require.True(e.t, ok, name)
			if issuedAt != nil {
				ts = *issuedAt
			}
			var err error
			value, err = encryption.SignedValue(secret, newName, plain, ts)
			require.NoError(e.t, err)
			name = newName
		}
		out = append(out, &http.Cookie{Name: name, Value: value, Path: "/", Secure: true, HttpOnly: true})
	}

	jar, err := cookiejar.New(nil)
	require.NoError(e.t, err)
	jar.SetCookies(u, out)
	moved := *client
	moved.Jar = jar
	return &moved
}

func (e *previewEnv) assertNoProxyCookiesReachApp(path string) {
	e.t.Helper()
	header := strings.Join(e.lastAppRequestTo(path).Header.Values("Cookie"), ";")
	for _, name := range cookieNames(header) {
		assert.False(e.t, strings.HasPrefix(name, "__Host-kbc-"), name)
		assert.False(e.t, strings.HasPrefix(name, "_oauth2_proxy"), name)
		assert.NotEqual(e.t, sessions.CookieName, name)
	}
}

func TestHostCookies_OIDCSessionUnderNewNames(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)
	client := env.oidcLogin(env.providers[0], hostCookieUser(), oidcOrigin+"/")
	require.Equal(t, http.StatusOK, env.send(client, oidcOrigin+"/").StatusCode)

	moved := env.legacyToHostNames(client, oidcHost, nil)
	require.False(t, slices.ContainsFunc(moved.Jar.Cookies(&url.URL{Scheme: "https", Host: oidcHost, Path: "/"}), func(c *http.Cookie) bool { return c.Name == oauthproxy.CookieName+"_ticket" }))

	resp := env.send(moved, oidcOrigin+"/some/path")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	env.assertNoProxyCookiesReachApp("/some/path")
}

func TestHostCookies_TicketKeepsItsOriginalAge(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)
	client := env.oidcLogin(env.providers[0], hostCookieUser(), oidcOrigin+"/")

	old := time.Now().Add(-25 * time.Hour)
	moved := env.legacyToHostNames(client, oidcHost, &old)
	resp := env.send(moved, oidcOrigin+"/some/path")
	require.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Location"), "/oidc/authorize?client_id=")
}

func TestHostCookies_TicketCopiedWithoutResigningIsRejected(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)
	client := env.oidcLogin(env.providers[0], hostCookieUser(), oidcOrigin+"/")

	u := &url.URL{Scheme: "https", Host: oidcHost, Path: "/"}
	existing := client.Jar.Cookies(u)
	copied := make([]*http.Cookie, 0, len(existing))
	for _, c := range existing {
		if c.Name == oauthproxy.CookieName+"_ticket" {
			c = &http.Cookie{Name: oauthproxy.HostSessionCookieName, Value: c.Value, Path: "/", Secure: true}
		}
		copied = append(copied, c)
	}
	jar, err := cookiejar.New(nil)
	require.NoError(t, err)
	jar.SetCookies(u, slices.DeleteFunc(copied, func(c *http.Cookie) bool { return c.Name == sessions.CookieName }))
	moved := *client
	moved.Jar = jar

	resp := env.send(&moved, oidcOrigin+"/some/path")
	assert.Equal(t, http.StatusFound, resp.StatusCode)
}

func TestHostCookies_LoginStartedUnderNewNamesCompletes(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)
	jar, err := cookiejar.New(nil)
	require.NoError(t, err)
	client := *env.client
	client.Jar = jar
	env.providers[0].QueueUser(hostCookieUser())

	resp := env.send(&client, oidcOrigin+"/")
	require.Equal(t, http.StatusFound, resp.StatusCode)
	resp = env.send(&client, resp.Header.Get("Location"))
	require.Equal(t, http.StatusFound, resp.StatusCode)
	callback := resp.Header.Get("Location")

	// The pod that started the login wrote the CSRF cookie under the new name.
	moved := env.legacyToHostNames(&client, oidcHost, nil)
	names := cookieNamesIn(moved, oidcHost)
	require.Contains(t, names, oauthproxy.HostSessionCookieName+"_csrf")
	require.NotContains(t, names, oauthproxy.CookieName+"_ticket_csrf")

	resp = env.send(moved, callback)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp = env.send(moved, extractMetaRefreshTag(t, []byte(readBody(t, resp))))
	require.Equal(t, http.StatusFound, resp.StatusCode, "no CSRF error")

	var expired *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == oauthproxy.HostSessionCookieName+"_csrf" {
			expired = c
		}
	}
	require.NotNil(t, expired, "the used CSRF cookie is expired under its new name")
	assertExpiredHostCookie(t, expired)

	assert.Equal(t, http.StatusOK, env.send(moved, oidcOrigin+"/").StatusCode, "no login loop")
}

func cookieNamesIn(client *http.Client, host string) []string {
	cookies := client.Jar.Cookies(&url.URL{Scheme: "https", Host: host, Path: "/"})
	names := make([]string, 0, len(cookies))
	for _, c := range cookies {
		names = append(names, c.Name)
	}
	return names
}

func assertExpiredHostCookie(t *testing.T, c *http.Cookie) {
	t.Helper()
	assert.Empty(t, c.Value, c.Name)
	assert.Negative(t, c.MaxAge, c.Name)
	assert.Equal(t, "/", c.Path, c.Name)
	assert.Empty(t, c.Domain, "%s: a __Host- cookie has no Domain", c.Name)
	assert.True(t, c.Secure, c.Name)
}

func TestHostCookies_SignOutExpiresNewNamesAndEndsSession(t *testing.T) {
	t.Parallel()
	events := make(chan sessions.Event, 64)
	stream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var event sessions.Event
		if assert.NoError(t, json.NewDecoder(req.Body).Decode(&event)) {
			events <- event
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(stream.Close)
	env := startPreviewEnv(t, func(cfg *config.Config, _ *previewtest.JWKSServer) {
		cfg.Sessions.StreamURL = stream.URL
	})
	env.syncClock()
	client := env.oidcLogin(env.providers[0], hostCookieUser(), oidcOrigin+"/")
	require.Equal(t, http.StatusOK, env.send(client, oidcOrigin+"/", "Accept", "text/html").StatusCode)

	moved := env.legacyToHostNames(client, oidcHost, nil)
	names := cookieNamesIn(moved, oidcHost)
	require.Contains(t, names, sessions.HostCookieName)
	require.Contains(t, names, selector.HostProviderCookie)
	require.Contains(t, names, oauthproxy.HostSessionCookieName)

	resp := env.send(moved, oidcOrigin+"/_proxy/sign_out")
	assert.Equal(t, http.StatusFound, resp.StatusCode)
	expired := map[string]*http.Cookie{}
	for _, c := range resp.Cookies() {
		if strings.HasPrefix(c.Name, "__Host-kbc-") {
			expired[c.Name] = c
		}
	}
	for _, name := range []string{sessions.HostCookieName, selector.HostProviderCookie, oauthproxy.HostSessionCookieName} {
		if assert.Contains(t, expired, name) {
			assertExpiredHostCookie(t, expired[name])
		}
	}

	assert.Eventually(t, func() bool {
		for {
			select {
			case event := <-events:
				if event.EventType == sessions.EventSessionEnd {
					return true
				}
			default:
				return false
			}
		}
	}, 5*time.Second, 10*time.Millisecond, "no session_end event for a session carried under the new name")
}

func TestHostCookies_BasicAuthUnderNewName(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)
	client := env.basicAuthLogin()
	moved := env.legacyToHostNames(client, authHost, nil)
	require.Contains(t, cookieNamesIn(moved, authHost), basicauth.HostCookieName)

	resp := env.send(moved, authOrigin+"/some/path")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	env.assertNoProxyCookiesReachApp("/some/path")

	resp = env.send(moved, authOrigin+"/_proxy/sign_out")
	require.Equal(t, http.StatusFound, resp.StatusCode)
	var cleared *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == basicauth.HostCookieName {
			cleared = c
		}
	}
	require.NotNil(t, cleared)
	assertExpiredHostCookie(t, cleared)
}
