package proxy_test

import (
	"crypto/rand"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/oauth2-proxy/mockoidc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/sessions"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/basicauth"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/oauthproxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/previewtest"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/session"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/selector"
)

const publicHost = "public-123.hub.keboola.local"

// appCookies are set by the app itself and must reach it unchanged.
const appCookies = "_streamlit_xsrf=2|abc|def|123; _oauth2_proxyapp=kept; session=a=b=c"

// setCookieRecorder records the name of every cookie apps-proxy sets.
type setCookieRecorder struct {
	next  http.RoundTripper
	lock  sync.Mutex
	names map[string]bool
}

func (r *setCookieRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := r.next.RoundTrip(req)
	if resp == nil {
		return resp, err
	}
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, c := range resp.Cookies() {
		r.names[c.Name] = true
	}
	return resp, err
}

func (r *setCookieRecorder) sorted() []string {
	r.lock.Lock()
	defer r.lock.Unlock()
	names := make([]string, 0, len(r.names))
	for name := range r.names {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// recordSetCookies makes every later client of the env record the cookies apps-proxy sets.
func (e *previewEnv) recordSetCookies() *setCookieRecorder {
	e.t.Helper()
	recorder := &setCookieRecorder{next: e.client.Transport, names: map[string]bool{}}
	client := *e.client
	client.Transport = recorder
	e.client = &client
	return recorder
}

// largeSessionUser has enough groups to split the oauth2-proxy session cookie into parts.
func largeSessionUser() *mockoidc.MockUser {
	groups := make([]string, 0, 201)
	groups = append(groups, "admin")
	for range 200 {
		groups = append(groups, rand.Text())
	}
	return &mockoidc.MockUser{Subject: "sub-1", Email: "admin@keboola.com", EmailVerified: new(true), Groups: groups}
}

func cookieNames(header string) []string {
	var names []string
	for c := range strings.SplitSeq(header, ";") {
		name, _, _ := strings.Cut(strings.TrimSpace(c), "=")
		if name != "" {
			names = append(names, name)
		}
	}
	return names
}

// withAllCookies returns a copy of the client whose jar for host holds its own cookies,
// every cookie apps-proxy ever set and the extra cookies.
func withAllCookies(t *testing.T, client *http.Client, host string, recorded []string, extra ...*http.Cookie) *http.Client {
	t.Helper()
	u := &url.URL{Scheme: "https", Host: host, Path: "/"}
	cookies := extra
	if client.Jar != nil {
		cookies = append(cookies, client.Jar.Cookies(u)...)
	}
	for _, name := range recorded {
		if !slices.ContainsFunc(cookies, func(c *http.Cookie) bool { return c.Name == name }) {
			cookies = append(cookies, &http.Cookie{Name: name, Value: "stale"})
		}
	}
	jar, err := cookiejar.New(nil)
	require.NoError(t, err)
	jar.SetCookies(u, cookies)
	c := *client
	c.Jar = jar
	return &c
}

func (e *previewEnv) lastAppRequestTo(path string) *http.Request {
	e.t.Helper()
	requests := *e.appServer.Requests
	for i := len(requests) - 1; i >= 0; i-- {
		if requests[i].URL.Path == path {
			return requests[i]
		}
	}
	require.Fail(e.t, "no app request to "+path)
	return nil
}

// assertAppGetsOnlyItsCookies checks that a plain request and a WebSocket handshake reach the app with only the app's cookies.
func (e *previewEnv) assertAppGetsOnlyItsCookies(flow string, client *http.Client, host string) {
	t := e.t
	t.Helper()

	resp := e.send(client, "https://"+host+"/some/path", "Cookie", appCookies)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	received := e.lastAppRequestTo("/some/path").Header.Values("Cookie")
	t.Logf("%s, HTTP: cookies received by the app: %v", flow, cookieNames(strings.Join(received, ";")))
	assert.Equal(t, []string{appCookies}, received, "%s, HTTP", flow)

	header := http.Header{}
	header.Set("Cookie", appCookies)
	c, resp, err := websocket.Dial(t.Context(), "wss://"+host+"/ws", &websocket.DialOptions{HTTPClient: client, HTTPHeader: header})
	require.NoError(t, err)
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	var msg any
	require.NoError(t, wsjson.Read(t.Context(), c, &msg))
	require.NoError(t, c.Close(websocket.StatusNormalClosure, ""))
	received = e.lastAppRequestTo("/ws").Header.Values("Cookie")
	t.Logf("%s, WebSocket: cookies received by the app: %v", flow, cookieNames(strings.Join(received, ";")))
	assert.Equal(t, []string{appCookies}, received, "%s, WebSocket handshake", flow)
}

// TestUpstreamCookies runs every flow in which apps-proxy sets a cookie,
// then sends all of them back: none may reach the app.
func TestUpstreamCookies(t *testing.T) {
	t.Parallel()
	stream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	t.Cleanup(stream.Close)
	env := startPreviewEnv(t, func(cfg *config.Config, _ *previewtest.JWKSServer) {
		cfg.Sessions.StreamURL = stream.URL
	})
	recorder := env.recordSetCookies()
	env.syncClock()

	oidcClient := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")
	require.Equal(t, http.StatusOK, env.send(oidcClient, oidcOrigin+"/", "Accept", "text/html").StatusCode)
	splitClient := env.oidcLogin(env.providers[0], largeSessionUser(), oidcOrigin+"/")
	require.Equal(t, http.StatusOK, env.send(splitClient, oidcOrigin+"/", "Accept", "text/html").StatusCode)
	signedOut := withAllCookies(t, splitClient, oidcHost, nil)
	require.Equal(t, http.StatusFound, env.send(signedOut, oidcOrigin+selector.SignOutPath).StatusCode)

	basicClient := env.basicAuthLogin()
	require.Equal(t, http.StatusOK, env.send(basicClient, authOrigin+"/", "Accept", "text/html").StatusCode)

	env.setDevMode("app-auth", authRef, true)
	env.waitForKeys(authHost, authOrigin)
	previewCookie := env.session(authHost, authOrigin)

	recorded := recorder.sorted()
	t.Logf("cookies set by apps-proxy: %v", recorded)
	for _, name := range []string{
		oauthproxy.CookieName,
		oauthproxy.CookieName + "_0",
		oauthproxy.CookieName + "_1",
		oauthproxy.CookieName + "_csrf",
		selector.ProviderCookie,
		sessions.CookieName,
		basicauth.CookieName,
		session.CookieName,
	} {
		require.Contains(t, recorded, name, "no flow above set this cookie")
	}

	env.assertAppGetsOnlyItsCookies("preview session", withAllCookies(t, env.client, authHost, recorded, previewCookie), authHost)

	env.assertAppGetsOnlyItsCookies("OIDC", withAllCookies(t, oidcClient, oidcHost, recorded), oidcHost)

	// A stale unsplit cookie would hide the split parts from oauth2-proxy.
	withoutUnsplit := slices.DeleteFunc(slices.Clone(recorded), func(name string) bool { return name == oauthproxy.CookieName })
	env.assertAppGetsOnlyItsCookies("OIDC split session", withAllCookies(t, splitClient, oidcHost, withoutUnsplit), oidcHost)

	env.assertAppGetsOnlyItsCookies("no auth", withAllCookies(t, env.client, publicHost, recorded), publicHost)

	env.setDevMode("app-auth", authRef, false)
	env.assertAppGetsOnlyItsCookies("basic auth", withAllCookies(t, basicClient, authHost, recorded), authHost)
}

// TestUpstreamCookies_RedisStore checks that the redis store's ticket and CSRF cookies never reach the app.
func TestUpstreamCookies_RedisStore(t *testing.T) {
	t.Parallel()
	env := startRedisEnv(t)
	recorder := env.recordSetCookies()
	env.syncClock()

	oidcClient := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")
	require.Equal(t, http.StatusOK, env.send(oidcClient, oidcOrigin+"/", "Accept", "text/html").StatusCode)

	recorded := recorder.sorted()
	t.Logf("cookies set by apps-proxy: %v", recorded)
	require.Contains(t, recorded, ticketCookieName)
	require.Contains(t, recorded, ticketCookieName+"_csrf")
	require.NotNil(t, jarCookie(oidcClient, ticketCookieName))

	env.assertAppGetsOnlyItsCookies("OIDC redis store", withAllCookies(t, oidcClient, oidcHost, recorded), oidcHost)
}
