package proxy_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/oauth2-proxy/mockoidc"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	sessionsapi "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/sessions"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/sessions/cookie"
	redisstore "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/sessions/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/previewtest"
)

const (
	oauthCookieName  = "_oauth2_proxy"
	ticketCookieName = "_oauth2_proxy_ticket"
)

// forEachSessionStore runs the test in an env with the cookie store and in one with the redis store.
// The test reads its *testing.T from env.t.
func forEachSessionStore(t *testing.T, test func(env *previewEnv), mutate ...envMutator) {
	t.Helper()
	for _, store := range []struct {
		name  string
		start func(t *testing.T, mutate ...envMutator) *previewEnv
	}{
		{name: "cookie", start: startPreviewEnv},
		{name: "redis", start: startRedisEnv},
	} {
		t.Run(store.name, func(t *testing.T) {
			t.Parallel()
			test(store.start(t, mutate...))
		})
	}
}

func (e *previewEnv) oidcCookieName() string {
	if e.redisStore != nil {
		return ticketCookieName
	}
	return oauthCookieName
}

func freshnessUser() *mockoidc.MockUser {
	return &mockoidc.MockUser{Subject: "sub-1", Email: "admin@keboola.com", EmailVerified: new(true), Groups: []string{"admin"}}
}

// oidcSessionStore reads and writes the oauth2-proxy session of the "oidc" app,
// so a test can move a session back in time.
func (e *previewEnv) oidcSessionStore() sessionsapi.SessionStore {
	e.t.Helper()
	h := sha256.New()
	h.Write([]byte("oidc/oidc/" + e.d.Config().CookieSecretSalt))
	cookieOpts := &options.Cookie{
		Name:     e.oidcCookieName(),
		Secret:   fmt.Sprintf("%x", h.Sum(nil)[:16]),
		Domains:  []string{oidcHost},
		Path:     "/",
		Expire:   24 * time.Hour,
		Secure:   true,
		HTTPOnly: true,
		SameSite: "strict",
	}
	if e.redisStore != nil {
		store, err := redisstore.NewRedisSessionStore(&options.SessionOptions{Type: options.RedisSessionStoreType, Redis: options.RedisStoreOptions{Client: e.redisStore}}, cookieOpts)
		require.NoError(e.t, err)
		return store
	}
	store, err := cookie.NewCookieSessionStore(&options.SessionOptions{Type: options.CookieSessionStoreType}, cookieOpts)
	require.NoError(e.t, err)
	return store
}

func (e *previewEnv) oidcRequest(client *http.Client) *http.Request {
	e.t.Helper()
	req := httptest.NewRequestWithContext(e.t.Context(), http.MethodGet, oidcOrigin+"/", nil)
	for _, c := range client.Jar.Cookies(&url.URL{Scheme: "https", Host: oidcHost, Path: "/"}) {
		req.AddCookie(c)
	}
	return req
}

func (e *previewEnv) loadOIDCSession(client *http.Client) *sessionsapi.SessionState {
	e.t.Helper()
	session, err := e.oidcSessionStore().Load(e.oidcRequest(client))
	require.NoError(e.t, err)
	return session
}

// rewriteOIDCSession re-signs the session cookie in the client's jar after mutate.
// The cookie signature carries CreatedAt, so moving it back ages the cookie too.
// The request carries the current cookie, so the redis store keeps the same ticket and key.
func (e *previewEnv) rewriteOIDCSession(client *http.Client, mutate func(s *sessionsapi.SessionState)) {
	e.t.Helper()
	session := e.loadOIDCSession(client)
	mutate(session)
	rec := httptest.NewRecorder()
	require.NoError(e.t, e.oidcSessionStore().Save(rec, e.oidcRequest(client), session))
	client.Jar.SetCookies(&url.URL{Scheme: "https", Host: oidcHost, Path: "/"}, rec.Result().Cookies())
}

// requireWebsocketCloseLog waits for the log line apps-proxy writes when it closes a websocket at its deadline.
func (e *previewEnv) requireWebsocketCloseLog(reason string) {
	e.t.Helper()
	require.Eventually(e.t, func() bool {
		for _, line := range strings.Split(e.mocked.DebugLogger().AllMessages(), "\n") {
			if strings.Contains(line, `"message":"websocket closed at its deadline: `+reason+`"`) && strings.Contains(line, `"proxy.app.id":"oidc"`) {
				return true
			}
		}
		return false
	}, 5*time.Second, 50*time.Millisecond, "no websocket close log line with reason %q", reason)
}

func ago(d time.Duration) *time.Time {
	v := time.Now().Add(-d)
	return &v
}

func setCookieNames(resp *http.Response) []string {
	cookies := resp.Cookies()
	names := make([]string, 0, len(cookies))
	for _, c := range cookies {
		names = append(names, c.Name)
	}
	return names
}

func dialIdleWebsocket(t *testing.T, client *http.Client) (*websocket.Conn, *http.Response) {
	t.Helper()
	c, resp, err := websocket.Dial(t.Context(), "wss://"+oidcHost+"/ws-idle", &websocket.DialOptions{HTTPClient: client})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.CloseNow() })
	return c, resp
}

// waitForClose blocks until the proxy closes the websocket and returns how long that took.
func waitForClose(t *testing.T, c *websocket.Conn, limit time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := c.Read(t.Context()); err != nil {
				return
			}
		}
	}()
	select {
	case <-done:
		return time.Since(start)
	case <-time.After(limit):
		require.Fail(t, "websocket was not closed by the proxy")
		return 0
	}
}

func TestSessionFreshness_RefreshOnUpgradeSetsCookieOn101(t *testing.T) {
	t.Parallel()
	forEachSessionStore(t, func(env *previewEnv) {
		t := env.t
		client := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")

		createdAt := ago(2 * time.Hour)
		env.rewriteOIDCSession(client, func(s *sessionsapi.SessionState) {
			require.NotEmpty(t, s.RefreshToken)
			s.CreatedAt = createdAt
		})

		_, resp := dialIdleWebsocket(t, client)
		require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
		assert.Contains(t, setCookieNames(resp), env.oidcCookieName())

		refreshed := env.loadOIDCSession(client)
		assert.WithinDuration(t, time.Now(), *refreshed.CreatedAt, time.Minute)
	})
}

func TestSessionFreshness_SessionWithoutRefreshTokenIsNotRefreshed(t *testing.T) {
	t.Parallel()
	forEachSessionStore(t, func(env *previewEnv) {
		t := env.t
		client := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")

		// Expired tokens would end the session if it were re-validated, as on GitHub's refresh path.
		env.rewriteOIDCSession(client, func(s *sessionsapi.SessionState) {
			s.RefreshToken = ""
			s.CreatedAt = ago(2 * time.Hour)
			s.ExpiresOn = ago(time.Hour)
		})

		resp := env.send(client, oidcOrigin+"/some/path")
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.NotContains(t, setCookieNames(resp), env.oidcCookieName())
	})
}

func TestSessionFreshness_SessionWithoutRefreshTokenEndsAfter24h(t *testing.T) {
	t.Parallel()
	forEachSessionStore(t, func(env *previewEnv) {
		t := env.t
		client := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")

		env.rewriteOIDCSession(client, func(s *sessionsapi.SessionState) {
			s.RefreshToken = ""
			s.CreatedAt = ago(24*time.Hour + time.Minute)
		})

		resp := env.send(client, oidcOrigin+"/some/path")
		require.Equal(t, http.StatusFound, resp.StatusCode)
		assert.Contains(t, resp.Header.Get("Location"), "/oidc/authorize?client_id=")
	})
}

func TestSessionFreshness_WebsocketClosedAtCookieExpiry(t *testing.T) {
	t.Parallel()
	forEachSessionStore(t, func(env *previewEnv) {
		t := env.t
		client := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")

		env.rewriteOIDCSession(client, func(s *sessionsapi.SessionState) {
			s.RefreshToken = ""
			s.CreatedAt = ago(24*time.Hour - 5*time.Second)
		})

		c, resp := dialIdleWebsocket(t, client)
		require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
		assert.Less(t, waitForClose(t, c, 20*time.Second), 15*time.Second)
		env.requireWebsocketCloseLog("cookie expiry")

		// The reconnect finds the cookie expired.
		require.Equal(t, http.StatusFound, env.send(client, oidcOrigin+"/some/path").StatusCode)
	})
}

func TestSessionFreshness_WebsocketClosedAtRefreshAndReconnectRefreshes(t *testing.T) {
	t.Parallel()
	forEachSessionStore(t, func(env *previewEnv) {
		t := env.t
		client := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")

		env.rewriteOIDCSession(client, func(s *sessionsapi.SessionState) {
			require.NotEmpty(t, s.RefreshToken)
			s.CreatedAt = ago(time.Hour - time.Second)
		})

		c, resp := dialIdleWebsocket(t, client)
		require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
		assert.NotContains(t, setCookieNames(resp), env.oidcCookieName())
		assert.Less(t, waitForClose(t, c, 30*time.Second), 20*time.Second)
		env.requireWebsocketCloseLog("refresh due")

		_, resp = dialIdleWebsocket(t, client)
		require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
		assert.Contains(t, setCookieNames(resp), env.oidcCookieName())
	})
}

func TestSessionFreshness_WebsocketWithFreshSessionStaysOpen(t *testing.T) {
	t.Parallel()
	forEachSessionStore(t, func(env *previewEnv) {
		t := env.t
		client := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")

		c, resp := dialIdleWebsocket(t, client)
		require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)

		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		_, _, err := c.Read(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestSessionFreshness_WebsocketClosedAtWsTimeoutIsLogged(t *testing.T) {
	t.Parallel()
	forEachSessionStore(t, func(env *previewEnv) {
		t := env.t
		client := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")

		c, resp := dialIdleWebsocket(t, client)
		require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
		assert.Less(t, waitForClose(t, c, 20*time.Second), 15*time.Second)
		env.requireWebsocketCloseLog("ws timeout")
	}, func(cfg *config.Config, _ *previewtest.JWKSServer) {
		cfg.Upstream.WsTimeout = 2 * time.Second
	})
}
