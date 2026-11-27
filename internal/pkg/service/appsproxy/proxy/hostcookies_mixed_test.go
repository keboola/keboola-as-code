package proxy_test

import (
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/oauth2-proxy/mockoidc"
	sessionsapi "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/sessions"
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

var oidcURL = &url.URL{Scheme: "https", Host: oidcHost, Path: "/"} //nolint:gochecknoglobals // test fixture

func hostOnly(name, value string) *http.Cookie {
	return &http.Cookie{Name: name, Value: value, Path: "/", Secure: true, HttpOnly: true}
}

func cookieIn(client *http.Client, u *url.URL, name string) *http.Cookie {
	for _, c := range client.Jar.Cookies(u) {
		if c.Name == name {
			return c
		}
	}
	return nil
}

func expiredCookie(resp *http.Response, name string) *http.Cookie {
	for _, c := range resp.Cookies() {
		if c.Name == name && c.MaxAge < 0 {
			return c
		}
	}
	return nil
}

func setCookie(resp *http.Response, name string) *http.Cookie {
	for _, c := range resp.Cookies() {
		if c.Name == name && c.MaxAge >= 0 && c.Value != "" {
			return c
		}
	}
	return nil
}

// startLoginAtIDP starts a login on the "oidc" app and stops before the IdP: the jar holds a legacy CSRF cookie.
func (e *previewEnv) startLoginAtIDP() (*http.Client, string) {
	e.t.Helper()
	jar, err := cookiejar.New(nil)
	require.NoError(e.t, err)
	client := *e.client
	client.Jar = jar
	resp := e.send(&client, oidcOrigin+"/")
	require.Equal(e.t, http.StatusFound, resp.StatusCode)
	return &client, resp.Header.Get("Location")
}

// resignAt returns the cookie signed under name with the given timestamp.
func (e *previewEnv) resignAt(c *http.Cookie, name string, ts time.Time) *http.Cookie {
	e.t.Helper()
	secret := e.oidcCookieSecret()
	plain, _, ok := encryption.Validate(c, secret, 24*time.Hour)
	require.True(e.t, ok, c.Name)
	v, err := encryption.SignedValue(secret, name, plain, ts)
	require.NoError(e.t, err)
	return hostOnly(name, v)
}

func (e *previewEnv) withCookies(client *http.Client, u *url.URL, cookies ...*http.Cookie) *http.Client {
	e.t.Helper()
	jar, err := cookiejar.New(nil)
	require.NoError(e.t, err)
	jar.SetCookies(u, cookies)
	c := *client
	c.Jar = jar
	return &c
}

func TestHostCookies_WebsocketWithHostCSRF(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)
	client := env.oidcLogin(env.providers[0], hostCookieUser(), oidcOrigin+"/")

	other, _ := env.startLoginAtIDP()
	hostCSRF := cookieIn(env.legacyToHostNames(other, oidcHost, nil), oidcURL, oauthproxy.HostSessionCookieName+"_csrf")
	require.NotNil(t, hostCSRF)
	client.Jar.SetCookies(oidcURL, []*http.Cookie{hostOnly(hostCSRF.Name, hostCSRF.Value)})

	env.dialWebsocket(client, nil)

	c, resp, err := websocket.Dial(t.Context(), "wss://"+oidcHost+"/ws-idle", &websocket.DialOptions{HTTPClient: client})
	require.NoError(t, err)
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	_ = c.CloseNow()
}

func TestHostCookies_NormalRequestKeepsHostCSRF(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)
	client := env.oidcLogin(env.providers[0], hostCookieUser(), oidcOrigin+"/")
	other, _ := env.startLoginAtIDP()
	hostCSRF := cookieIn(env.legacyToHostNames(other, oidcHost, nil), oidcURL, oauthproxy.HostSessionCookieName+"_csrf")
	require.NotNil(t, hostCSRF)
	client.Jar.SetCookies(oidcURL, []*http.Cookie{hostOnly(hostCSRF.Name, hostCSRF.Value)})

	resp := env.send(client, oidcOrigin+"/some/path")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Nil(t, expiredCookie(resp, hostCSRF.Name), "an in-flight login on another pod is not touched")
	assert.NotNil(t, cookieIn(client, oidcURL, hostCSRF.Name))
}

func TestHostCookies_RefreshExpiresHostTicket(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)
	client := env.oidcLogin(env.providers[0], hostCookieUser(), oidcOrigin+"/")
	env.rewriteOIDCSession(client, func(s *sessionsapi.SessionState) {
		require.NotEmpty(t, s.RefreshToken)
		s.CreatedAt = ago(2 * time.Hour)
	})
	moved := env.legacyToHostNames(client, oidcHost, nil)

	resp := env.send(moved, oidcOrigin+"/some/path")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotNil(t, setCookie(resp, ticketCookieName), "AP1a writes the legacy ticket")
	assert.NotNil(t, expiredCookie(resp, oauthproxy.HostSessionCookieName), "and expires the host-only one")
	assert.Nil(t, cookieIn(moved, oidcURL, oauthproxy.HostSessionCookieName))
	assert.NotNil(t, cookieIn(moved, oidcURL, ticketCookieName))
	assert.Equal(t, http.StatusOK, env.send(moved, oidcOrigin+"/some/path").StatusCode)
}

func TestHostCookies_NewLoginExpiresHostTicket(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)
	client := env.oidcLogin(env.providers[0], hostCookieUser(), oidcOrigin+"/")
	moved := env.legacyToHostNames(client, oidcHost, nil)
	require.NotNil(t, cookieIn(moved, oidcURL, oauthproxy.HostSessionCookieName))

	env.providers[0].QueueUser(hostCookieUser())
	resp := env.send(moved, oidcOrigin+"/_proxy/start?rd=%2F")
	require.Equal(t, http.StatusFound, resp.StatusCode)
	resp = env.send(moved, resp.Header.Get("Location"))
	require.Equal(t, http.StatusFound, resp.StatusCode)
	resp = env.send(moved, resp.Header.Get("Location"))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp = env.send(moved, extractMetaRefreshTag(t, []byte(readBody(t, resp))))
	require.Equal(t, http.StatusFound, resp.StatusCode)

	require.NotNil(t, setCookie(resp, ticketCookieName))
	assert.NotNil(t, expiredCookie(resp, oauthproxy.HostSessionCookieName))
	assert.Nil(t, cookieIn(moved, oidcURL, oauthproxy.HostSessionCookieName))
}

func TestHostCookies_ClearExpiresHostTicket(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)
	client := env.oidcLogin(env.providers[0], hostCookieUser(), oidcOrigin+"/")
	moved := env.legacyToHostNames(client, oidcHost, nil)
	for _, key := range env.ticketKeys() {
		env.redis.Del(key)
	}

	resp := env.send(moved, oidcOrigin+"/some/path")
	require.Equal(t, http.StatusFound, resp.StatusCode)
	assert.NotNil(t, expiredCookie(resp, oauthproxy.HostSessionCookieName), "a session cleared outside sign-out leaves no host-only ticket")
}

func TestHostCookies_ProviderSwitchExpiresHostProvider(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, multiOrigin+"/_proxy/selection?provider=oidc1", nil)
	require.NoError(t, err)
	req.AddCookie(&http.Cookie{Name: selector.HostProviderCookie, Value: "oidc0"})

	resp := env.do(req)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	require.NotNil(t, setCookie(resp, selector.ProviderCookie))
	assert.NotNil(t, expiredCookie(resp, selector.HostProviderCookie))
}

func TestHostCookies_FreshHostCSRFBeatsStaleLegacy(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)

	abandoned, _ := env.startLoginAtIDP()
	staleLegacy := cookieIn(abandoned, oidcURL, oauthproxy.CookieName+"_ticket_csrf")
	require.NotNil(t, staleLegacy)

	env.providers[0].QueueUser(hostCookieUser())
	fresh, authorize := env.startLoginAtIDP()
	resp := env.send(fresh, authorize)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	callback := resp.Header.Get("Location")

	moved := env.legacyToHostNames(fresh, oidcHost, nil)
	stale := env.resignAt(staleLegacy, staleLegacy.Name, time.Now().Add(-5*time.Minute))
	moved.Jar.SetCookies(oidcURL, []*http.Cookie{stale})

	resp = env.send(moved, callback)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp = env.send(moved, extractMetaRefreshTag(t, []byte(readBody(t, resp))))
	require.Equal(t, http.StatusFound, resp.StatusCode, "the newer CSRF cookie is used")
	assert.NotNil(t, expiredCookie(resp, oauthproxy.HostSessionCookieName+"_csrf"))
}

func TestHostCookies_SignOutExpiresEveryHostNameEvenWhenAbsent(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)
	client := env.oidcLogin(env.providers[0], hostCookieUser(), oidcOrigin+"/")

	resp := env.send(client, oidcOrigin+"/_proxy/sign_out")
	require.Equal(t, http.StatusFound, resp.StatusCode)
	for _, name := range []string{
		sessions.HostCookieName,
		oauthproxy.HostSessionCookieName,
		oauthproxy.HostSessionCookieName + "_csrf",
		selector.HostProviderCookie,
		basicauth.HostCookieName,
	} {
		c := expiredCookie(resp, name)
		if assert.NotNil(t, c, name) {
			assertExpiredHostCookie(t, c)
		}
	}
	assert.Equal(t, 1, countSetCookies(resp, oauthproxy.HostSessionCookieName), "no duplicates")
}

func countSetCookies(resp *http.Response, name string) int {
	n := 0
	for _, line := range resp.Header.Values("Set-Cookie") {
		if strings.HasPrefix(line, name+"=") {
			n++
		}
	}
	return n
}

func TestHostCookies_MultiProviderUnderHostNames(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)
	env.syncClock()
	jar, err := cookiejar.New(nil)
	require.NoError(t, err)
	client := *env.client
	client.Jar = jar
	env.providers[0].QueueUser(&mockoidc.MockUser{Subject: "sub-m", Email: "m@keboola.com", EmailVerified: new(true), Groups: []string{"manager"}})
	resp := env.send(&client, multiOrigin+"/_proxy/selection?provider=oidc0")
	for range 3 {
		require.Equal(t, http.StatusFound, resp.StatusCode)
		resp = env.send(&client, resp.Header.Get("Location"))
	}
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp = env.send(&client, extractMetaRefreshTag(t, []byte(readBody(t, resp))))
	require.Equal(t, http.StatusFound, resp.StatusCode)
	require.Equal(t, http.StatusOK, env.send(&client, multiOrigin+"/").StatusCode, "control: legacy names work")

	u := &url.URL{Scheme: "https", Host: "multi.hub.keboola.local", Path: "/"}
	secret := multiCookieSecret(env)
	var out []*http.Cookie
	for _, c := range client.Jar.Cookies(u) {
		switch c.Name {
		case selector.ProviderCookie:
			out = append(out, hostOnly(selector.HostProviderCookie, c.Value))
		case ticketCookieName:
			plain, ts, ok := encryption.Validate(c, secret, 24*time.Hour)
			require.True(t, ok)
			v, err := encryption.SignedValue(secret, oauthproxy.HostSessionCookieName, plain, ts)
			require.NoError(t, err)
			out = append(out, hostOnly(oauthproxy.HostSessionCookieName, v))
		}
	}
	require.Len(t, out, 2)
	moved := env.withCookies(&client, u, out...)

	assert.Equal(t, http.StatusOK, env.send(moved, multiOrigin+"/").StatusCode, "the provider picker is not shown")
	assert.False(t, slices.ContainsFunc(client.Jar.Cookies(u), func(c *http.Cookie) bool { return c.Name == "" }))
}

func TestHostCookies_WebsocketRefreshExpiresHostTicket(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)
	client := env.oidcLogin(env.providers[0], hostCookieUser(), oidcOrigin+"/")
	env.rewriteOIDCSession(client, func(s *sessionsapi.SessionState) {
		require.NotEmpty(t, s.RefreshToken)
		s.CreatedAt = ago(2 * time.Hour)
	})
	moved := env.legacyToHostNames(client, oidcHost, nil)

	_, resp := dialIdleWebsocket(t, moved)
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.NotNil(t, setCookie(resp, ticketCookieName), "the refresh writes the legacy ticket on the 101")
	assert.NotNil(t, expiredCookie(resp, oauthproxy.HostSessionCookieName), "and expires the host-only one")
	assert.Nil(t, cookieIn(moved, oidcURL, oauthproxy.HostSessionCookieName))
	assert.NotNil(t, cookieIn(moved, oidcURL, ticketCookieName))
}

func TestHostCookies_BasicAuthExpiresHostTwin(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)
	moved := env.legacyToHostNames(env.basicAuthLogin(), authHost, nil)
	require.NotNil(t, cookieIn(moved, &url.URL{Scheme: "https", Host: authHost, Path: "/"}, basicauth.HostCookieName))

	resp := env.send(moved, authOrigin+"/some/path")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotNil(t, setCookie(resp, basicauth.CookieName), "the sliding cookie is written under the legacy name")
	assert.NotNil(t, expiredCookie(resp, basicauth.HostCookieName))
}

func TestHostCookies_SessionCookieExpiresHostTwin(t *testing.T) {
	t.Parallel()
	stream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	t.Cleanup(stream.Close)
	env := startPreviewEnv(t, func(cfg *config.Config, _ *previewtest.JWKSServer) {
		cfg.Sessions.StreamURL = stream.URL
	})
	env.syncClock()
	client := env.oidcLogin(env.providers[0], hostCookieUser(), oidcOrigin+"/")
	require.Equal(t, http.StatusOK, env.send(client, oidcOrigin+"/", "Accept", "text/html").StatusCode)
	moved := env.legacyToHostNames(client, oidcHost, nil)
	require.NotNil(t, cookieIn(moved, oidcURL, sessions.HostCookieName))

	// The cookie deadline moves on the next navigation, which writes the legacy cookie.
	env.clock.Advance(20 * time.Minute)
	resp := env.send(moved, oidcOrigin+"/", "Accept", "text/html")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotNil(t, setCookie(resp, sessions.CookieName))
	assert.NotNil(t, expiredCookie(resp, sessions.HostCookieName))
}
