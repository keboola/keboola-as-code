package proxy_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/jonboulle/clockwork"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	proxyDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/kaipreview"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/oauthproxy/logging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/previewtest"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/session"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/testutil"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	authHost    = "basic-auth.hub.keboola.local"
	authOrigin  = "https://" + authHost
	oidcHost    = "oidc.hub.keboola.local"
	devmodeHost = "dev-devmode.hub.keboola.local"
)

var authRef = k8sapp.WorkloadRef{AppID: "auth"} //nolint:gochecknoglobals // test fixture

type osAssignedPorts struct{}

func (osAssignedPorts) GeneratePorts(context.Context) {}
func (osAssignedPorts) GetFreePort() int              { return 0 }

type previewEnv struct {
	t         *testing.T
	clock     *clockwork.FakeClock
	signer    *previewtest.Signer
	jwks      *previewtest.JWKSServer
	client    *http.Client
	appServer *testutil.AppServer
	d         proxyDependencies.ServiceScope
	mocked    proxyDependencies.Mocked
}

func startPreviewEnv(t *testing.T, mutate ...func(cfg *config.Config, jwks *previewtest.JWKSServer)) *previewEnv {
	t.Helper()
	ctx := t.Context()
	clock := clockwork.NewFakeClockAt(time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC))
	signer := previewtest.NewSigner(t, "2026-09")
	jwks := previewtest.NewJWKSServer(t, signer.JWK())

	pm := osAssignedPorts{}
	appsAPI := testutil.StartDataAppsAPI(t, pm)
	t.Cleanup(func() { appsAPI.Close() })
	appServer := testutil.StartAppServer(t, pm)
	t.Cleanup(func() { appServer.Close() })
	providers := testAuthProviders(t, pm)
	appURL := testutil.AppServerURL(t, appServer)
	apps := testDataApps(appURL, providers)
	appsAPI.Register(apps)

	secret := make([]byte, 32)
	_, err := rand.Read(secret)
	require.NoError(t, err)
	cfg := config.New()
	cfg.API.PublicURL, _ = url.Parse("https://hub.keboola.local")
	cfg.CookieSecretSalt = string(secret)
	cfg.CsrfTokenSalt = string(secret)
	cfg.SandboxesAPI.URL = appsAPI.URL
	cfg.Preview.JWKSURL = jwks.JWKSURL()
	cfg.Preview.Issuer = previewtest.Issuer
	cfg.Preview.SessionSigningKey = strings.Repeat("s", 64)
	for _, m := range mutate {
		m(&cfg, jwks)
	}

	d, mocked := proxyDependencies.NewMockedServiceScopeWithK8sObjects(
		t, ctx, cfg, makeDefaultK8sObjects(apps, appURL.String()),
		dependencies.WithRealHTTPClient(), dependencies.WithClock(clock),
	)
	t.Cleanup(func() {
		d.Process().Shutdown(context.WithoutCancel(ctx), errors.New("bye bye"))
		d.Process().WaitForShutdown()
	})

	loggerWriter := logging.NewLoggerWriter(d.Logger(), "info")
	logger.SetOutput(loggerWriter)
	logger.SetErrOutput(loggerWriter)
	handler := proxy.NewHandler(ctx, d)

	var lc net.ListenConfig
	l, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &httptest.Server{Listener: l, Config: &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ErrorLog: log.NewStdErrorLogger(d.Logger())}}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	proxyURL, err := url.Parse(srv.URL)
	require.NoError(t, err)
	client := createHTTPClient(t, proxyURL)
	client.Jar = nil

	registerDefaultK8sApps(t, d.AppStateWatcher())
	return &previewEnv{t: t, clock: clock, signer: signer, jwks: jwks, client: client, appServer: appServer, d: d, mocked: mocked}
}

func (e *previewEnv) setDevMode(k8sName string, ref k8sapp.WorkloadRef, enabled bool) {
	e.t.Helper()
	gvr := k8sapp.AppGVR()
	if ref.IsSandbox() {
		gvr = k8sapp.SandboxGVR()
	}
	patch := fmt.Appendf(nil, `{"spec":{"devMode":{"enabled":%t}}}`, enabled)
	_, err := e.mocked.TestFakeK8sClient().Resource(gvr).Namespace("keboola").Patch(e.t.Context(), k8sName, k8stypes.MergePatchType, patch, metav1.PatchOptions{})
	require.NoError(e.t, err)
	require.Eventually(e.t, func() bool {
		info, ok := e.d.AppStateWatcher().GetState(e.t.Context(), ref)
		return ok && info.DevMode == enabled
	}, 5*time.Second, 50*time.Millisecond)
}

func (e *previewEnv) link(sub string) string {
	return e.signer.Mint(e.t, previewtest.ValidClaims(e.clock.Now(), sub))
}

func (e *previewEnv) do(req *http.Request) *http.Response {
	e.t.Helper()
	resp, err := e.client.Do(req)
	require.NoError(e.t, err)
	e.t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func (e *previewEnv) redeem(host, token string, headers map[string]string) *http.Response {
	e.t.Helper()
	form := url.Values{preview.TokenFormField: {token}}
	req, err := http.NewRequestWithContext(e.t.Context(), http.MethodPost, "https://"+host+preview.Path, strings.NewReader(form.Encode()))
	require.NoError(e.t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Origin", "null")
	for k, v := range headers {
		if v == "" {
			req.Header.Del(k)
			continue
		}
		req.Header.Set(k, v)
	}
	return e.do(req)
}

func (e *previewEnv) waitForKeys(host, origin string) {
	e.t.Helper()
	require.Eventually(e.t, func() bool {
		return e.redeem(host, e.link(origin), nil).StatusCode == http.StatusSeeOther
	}, 10*time.Second, 50*time.Millisecond)
}

func (e *previewEnv) session(host, origin string) *http.Cookie {
	e.t.Helper()
	resp := e.redeem(host, e.link(origin), nil)
	require.Equal(e.t, http.StatusSeeOther, resp.StatusCode)
	c := previewCookie(resp)
	require.NotNil(e.t, c)
	return c
}

func (e *previewEnv) get(host, path string, cookie *http.Cookie, headers map[string]string) *http.Response {
	e.t.Helper()
	req, err := http.NewRequestWithContext(e.t.Context(), http.MethodGet, "https://"+host+path, nil)
	require.NoError(e.t, err)
	if cookie != nil {
		req.AddCookie(cookie)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return e.do(req)
}

func previewCookie(resp *http.Response) *http.Cookie {
	for _, c := range resp.Cookies() {
		if c.Name == session.CookieName {
			return c
		}
	}
	return nil
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(b)
}

func TestPreviewLanding(t *testing.T) {
	t.Parallel()
	e := startPreviewEnv(t)

	resp := e.get(authHost, preview.Path, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
	assert.Equal(t, "no-referrer", resp.Header.Get("Referrer-Policy"))
	m := regexp.MustCompile(`^default-src 'none'; script-src 'nonce-([A-Za-z0-9]{24})'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'$`).FindStringSubmatch(resp.Header.Get("Content-Security-Policy"))
	require.NotNil(t, m, resp.Header.Get("Content-Security-Policy"))
	assert.Contains(t, readBody(t, resp), `nonce="`+m[1]+`"`)
	assert.Nil(t, previewCookie(resp))
}

func TestPreviewLanding_FrameAncestors(t *testing.T) {
	t.Parallel()
	e := startPreviewEnv(t, func(cfg *config.Config, _ *previewtest.JWKSServer) {
		cfg.Preview.AllowedFrameAncestors = []string{"https://connection.keboola.com"}
	})
	resp := e.get(authHost, preview.Path, nil, nil)
	assert.True(t, strings.HasSuffix(resp.Header.Get("Content-Security-Policy"), "frame-ancestors https://connection.keboola.com"))
}

//nolint:paralleltest,tparallel // subtests share e (clock, dev-mode, log buffer) and must run in sequence
func TestPreviewRedeem(t *testing.T) {
	t.Parallel()
	e := startPreviewEnv(t)
	e.setDevMode("app-auth", authRef, true)
	e.waitForKeys(authHost, authOrigin)

	t.Run("success-sets-cookie-and-redirects", func(t *testing.T) {
		resp := e.redeem(authHost, e.link(authOrigin), nil)
		require.Equal(t, http.StatusSeeOther, resp.StatusCode)
		assert.Equal(t, "/", resp.Header.Get("Location"))
		c := previewCookie(resp)
		require.NotNil(t, c)
		assert.Equal(t, "/", c.Path)
		assert.Empty(t, c.Domain)
		assert.True(t, c.Secure)
		assert.True(t, c.HttpOnly)
		assert.Equal(t, http.SameSiteNoneMode, c.SameSite)
		assert.True(t, c.Partitioned)
		assert.Equal(t, int(min(session.IdleTTL, session.MaxTTL).Seconds()), c.MaxAge)
		assert.NotContains(t, strings.Join(resp.Header.Values("Set-Cookie"), "\n"), "Domain=")

		app := e.get(authHost, "/", c, nil)
		require.Equal(t, http.StatusOK, app.StatusCode)
		assert.Equal(t, "Hello, client", readBody(t, app))
	})

	t.Run("link-is-reusable-within-its-lifetime", func(t *testing.T) {
		token := e.link(authOrigin)
		assert.Equal(t, http.StatusSeeOther, e.redeem(authHost, token, nil).StatusCode)
		assert.Equal(t, http.StatusSeeOther, e.redeem(authHost, token, nil).StatusCode)
	})

	t.Run("sec-fetch-site", func(t *testing.T) {
		for _, v := range []string{"", "cross-site", "same-site", "none"} {
			resp := e.redeem(authHost, e.link(authOrigin), map[string]string{"Sec-Fetch-Site": v})
			assert.Equal(t, http.StatusForbidden, resp.StatusCode, v)
			assert.Nil(t, previewCookie(resp), v)
		}
		resp := e.redeem(authHost, e.link(authOrigin), map[string]string{"Origin": ""})
		assert.Equal(t, http.StatusSeeOther, resp.StatusCode, "Origin is not required")
	})

	t.Run("invalid-link-gets-uniform-401", func(t *testing.T) {
		for name, token := range map[string]string{
			"other-app": e.link("https://" + devmodeHost),
			"renamed":   e.link("https://old-auth.hub.keboola.local"),
			"garbage":   "not-a-jwt",
			"empty":     "",
		} {
			resp := e.redeem(authHost, token, nil)
			assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, name)
			assert.Contains(t, readBody(t, resp), preview.InvalidLinkMessage, name)
			assert.Nil(t, previewCookie(resp), name)
		}
	})

	t.Run("sub-exact-match", func(t *testing.T) {
		assert.Equal(t, http.StatusSeeOther, e.redeem(authHost, e.link(authOrigin), nil).StatusCode)
		assert.Equal(t, http.StatusUnauthorized, e.redeem(authHost, e.link("HTTPS://BASIC-AUTH.HUB.KEBOOLA.LOCAL:443/"), nil).StatusCode)
		assert.Equal(t, http.StatusUnauthorized, e.redeem(authHost, e.link(authOrigin+":443"), nil).StatusCode)
		assert.Equal(t, http.StatusUnauthorized, e.redeem(authHost, e.link(authOrigin+"/"), nil).StatusCode)
		assert.Equal(t, http.StatusUnauthorized, e.redeem(authHost, e.link("https://BASIC-AUTH.hub.keboola.local"), nil).StatusCode)

		e.setDevMode("app-12345", k8sapp.WorkloadRef{AppID: "12345"}, true)
		assert.Equal(t, http.StatusSeeOther, e.redeem("lowercase-12345.hub.keboola.local", e.link("https://lowercase-12345.hub.keboola.local"), nil).StatusCode)
		assert.Equal(t, http.StatusUnauthorized, e.redeem("lowercase-12345.hub.keboola.local", e.link("https://LOWERCASE-12345.hub.keboola.local"), nil).StatusCode)

		assert.Equal(t, http.StatusUnauthorized, e.redeem(authHost, e.link(authOrigin+"/x"), nil).StatusCode)
		assert.Equal(t, http.StatusUnauthorized, e.redeem(authHost, e.link("http://"+authHost), nil).StatusCode)
	})

	t.Run("token-in-query-is-ignored", func(t *testing.T) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, authOrigin+preview.Path+"?token="+e.link(authOrigin), strings.NewReader(""))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Sec-Fetch-Site", "same-origin")
		assert.Equal(t, http.StatusUnauthorized, e.do(req).StatusCode)
	})

	t.Run("oversized-body", func(t *testing.T) {
		resp := e.redeem(authHost, strings.Repeat("a", preview.MaxRedeemBodySize+1), nil)
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("non-canonical-host-redirects-first", func(t *testing.T) {
		resp := e.get("old-auth.hub.keboola.local", preview.Path, nil, nil)
		assert.Equal(t, http.StatusPermanentRedirect, resp.StatusCode)
		assert.Equal(t, authOrigin+preview.Path, resp.Header.Get("Location"))
	})

	t.Run("logs-jti-not-token", func(t *testing.T) {
		e.mocked.DebugLogger().Truncate()
		token := e.link(authOrigin)
		require.Equal(t, http.StatusSeeOther, e.redeem(authHost, token, nil).StatusCode)
		logs := e.mocked.DebugLogger().AllMessages()
		assert.Contains(t, logs, `"preview.linkJti":"0123456789abcdef0123456789abcdef"`)
		assert.Contains(t, logs, `"preview.kid":"2026-09"`)
		assert.NotContains(t, logs, token)
		assert.NotContains(t, logs, strings.Split(token, ".")[1])
	})

	t.Run("expired-link", func(t *testing.T) {
		token := e.link(authOrigin)
		e.clock.Advance(91 * time.Second)
		assert.Equal(t, http.StatusUnauthorized, e.redeem(authHost, token, nil).StatusCode)
	})

	t.Run("non-dev-host", func(t *testing.T) {
		e.setDevMode("app-auth", authRef, false)
		resp := e.redeem(authHost, e.link(authOrigin), nil)
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		assert.Contains(t, readBody(t, resp), preview.InvalidLinkMessage)
		e.setDevMode("app-auth", authRef, true)
	})

	assert.Empty(t, e.mocked.DebugLogger().ErrorMessages())
}

func TestPreviewRedeem_JWKSDownAtStart(t *testing.T) {
	t.Parallel()
	e := startPreviewEnv(t, func(_ *config.Config, jwks *previewtest.JWKSServer) {
		jwks.SetStatus(http.StatusServiceUnavailable)
	})
	e.setDevMode("app-auth", authRef, true)
	require.Eventually(t, func() bool { return e.jwks.Hits() >= 1 }, 5*time.Second, 10*time.Millisecond)

	app := e.get(authHost, "/", nil, nil)
	require.Equal(t, http.StatusOK, app.StatusCode, "the proxy serves apps while JWKS is down")
	assert.Contains(t, readBody(t, app), `autocomplete="current-password"`)

	assert.Equal(t, http.StatusUnauthorized, e.redeem(authHost, e.link(authOrigin), nil).StatusCode)

	e.jwks.SetStatus(http.StatusOK)
	e.clock.Advance(61 * time.Second)
	assert.Equal(t, http.StatusSeeOther, e.redeem(authHost, e.link(authOrigin), nil).StatusCode, "an unknown kid refetches once the minute has passed")
}

func TestPreviewDisabled(t *testing.T) {
	t.Parallel()
	e := startPreviewEnv(t, func(cfg *config.Config, _ *previewtest.JWKSServer) {
		cfg.Preview.JWKSURL = ""
	})
	e.setDevMode("app-auth", authRef, true)
	assert.Equal(t, http.StatusNotFound, e.get(authHost, preview.Path, nil, nil).StatusCode)
	assert.Equal(t, http.StatusNotFound, e.redeem(authHost, e.link(authOrigin), nil).StatusCode)
	assert.Equal(t, int64(0), e.jwks.Hits(), "no fetcher runs when disabled")

	app := e.get(authHost, "/", nil, nil)
	assert.Contains(t, readBody(t, app), `autocomplete="current-password"`)
}

func TestPreviewSession(t *testing.T) {
	t.Parallel()

	t.Run("upstream-gets-no-identity-and-no-preview-cookie", func(t *testing.T) {
		t.Parallel()
		e := startPreviewEnv(t)
		e.setDevMode("app-auth", authRef, true)
		e.waitForKeys(authHost, authOrigin)
		c := e.session(authHost, authOrigin)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, authOrigin+"/some/path", nil)
		require.NoError(t, err)
		req.Header.Add("Cookie", "app_cookie=keep; "+session.CookieName+"="+c.Value)
		req.Header.Set("X-Kbc-User-Email", "forged@example.com")
		resp := e.do(req)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		requests := *e.appServer.Requests
		require.NotEmpty(t, requests)
		last := requests[len(requests)-1]
		_, err = last.Cookie(session.CookieName)
		require.ErrorIs(t, err, http.ErrNoCookie)
		kept, err := last.Cookie("app_cookie")
		require.NoError(t, err)
		assert.Equal(t, "keep", kept.Value)
		for name := range last.Header {
			assert.NotContains(t, strings.ToLower(name), "x-kbc-user-", name)
		}
	})

	t.Run("cookie-stripped-on-prod-app-too", func(t *testing.T) {
		t.Parallel()
		e := startPreviewEnv(t)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+devmodeHost+"/", nil)
		require.NoError(t, err)
		req.Header.Add("Cookie", "app_cookie=keep; "+session.CookieName+"=anything")
		require.Equal(t, http.StatusOK, e.do(req).StatusCode)
		requests := *e.appServer.Requests
		require.NotEmpty(t, requests)
		_, err = requests[len(requests)-1].Cookie(session.CookieName)
		assert.ErrorIs(t, err, http.ErrNoCookie)
	})

	t.Run("prod-switch", func(t *testing.T) {
		t.Parallel()
		e := startPreviewEnv(t)
		e.setDevMode("app-auth", authRef, true)
		e.waitForKeys(authHost, authOrigin)
		c := e.session(authHost, authOrigin)
		assert.Equal(t, "Hello, client", readBody(t, e.get(authHost, "/", c, nil)))

		e.setDevMode("app-auth", authRef, false)
		resp := e.get(authHost, "/", c, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		body := readBody(t, resp)
		assert.Contains(t, body, `autocomplete="current-password"`)
		assert.NotContains(t, body, "Hello, client")

		e.setDevMode("app-auth", authRef, true)
		assert.Equal(t, "Hello, client", readBody(t, e.get(authHost, "/", c, nil)), "a still-valid cookie works again in dev mode")
	})

	t.Run("sign-out", func(t *testing.T) {
		t.Parallel()
		e := startPreviewEnv(t)
		e.setDevMode("app-auth", authRef, true)
		e.waitForKeys(authHost, authOrigin)
		c := e.session(authHost, authOrigin)
		before := len(*e.appServer.Requests)

		resp := e.get(authHost, "/_proxy/sign_out", c, nil)
		assert.NotEqual(t, "Hello, client", readBody(t, resp))
		assert.Len(t, *e.appServer.Requests, before, "sign-out never reaches the app")
		cleared := previewCookie(resp)
		require.NotNil(t, cleared, "sign-out clears the preview cookie")
		assert.Negative(t, cleared.MaxAge)
	})

	t.Run("sign-out-without-auth-handlers", func(t *testing.T) {
		t.Parallel()
		e := startPreviewEnv(t)
		e.setDevMode("app-devmode", k8sapp.WorkloadRef{AppID: "devmode"}, true)
		e.waitForKeys(devmodeHost, "https://"+devmodeHost)
		c := e.session(devmodeHost, "https://"+devmodeHost)

		resp := e.get(devmodeHost, "/_proxy/sign_out", c, nil)
		cleared := previewCookie(resp)
		require.NotNil(t, cleared, "sign-out clears the preview cookie even on an app with no auth handlers")
		assert.Negative(t, cleared.MaxAge)
	})

	t.Run("oidc-callback", func(t *testing.T) {
		t.Parallel()
		e := startPreviewEnv(t)
		e.setDevMode("app-oidc", k8sapp.WorkloadRef{AppID: "oidc"}, true)
		e.waitForKeys(oidcHost, "https://"+oidcHost)
		c := e.session(oidcHost, "https://"+oidcHost)
		before := len(*e.appServer.Requests)

		resp := e.get(oidcHost, "/_proxy/callback?code=x&state=y", c, nil)
		assert.NotEqual(t, "Hello, client", readBody(t, resp))
		assert.Len(t, *e.appServer.Requests, before, "the OIDC callback never reaches the app")
	})

	t.Run("iframe-load-after-redeem-gets-the-app-not-the-kai-shim", func(t *testing.T) {
		t.Parallel()
		e := startPreviewEnv(t)
		e.setDevMode("app-auth", authRef, true)
		e.waitForKeys(authHost, authOrigin)
		c := e.session(authHost, authOrigin)
		resp := e.get(authHost, "/", c, map[string]string{"Sec-Fetch-Dest": "iframe", "Accept": "text/html"})
		assert.Equal(t, "Hello, client", readBody(t, resp))
	})

	t.Run("invalid-or-missing-cookie-gets-normal-login", func(t *testing.T) {
		t.Parallel()
		e := startPreviewEnv(t)
		e.setDevMode("app-auth", authRef, true)
		for name, c := range map[string]*http.Cookie{
			"none":    nil,
			"garbage": {Name: session.CookieName, Value: "garbage"},
		} {
			body := readBody(t, e.get(authHost, "/", c, nil))
			assert.Contains(t, body, `autocomplete="current-password"`, name)
		}
	})

	t.Run("slides-and-caps", func(t *testing.T) {
		t.Parallel()
		e := startPreviewEnv(t)
		e.setDevMode("app-auth", authRef, true)
		e.waitForKeys(authHost, authOrigin)
		idle, maxTTL := session.IdleTTL, session.MaxTTL
		start := e.clock.Now()
		c := e.session(authHost, authOrigin)

		e.clock.Advance(session.SlideInterval - time.Second)
		assert.Nil(t, previewCookie(e.get(authHost, "/", c, nil)), "no slide before the slide interval")

		slid := 0
		for e.clock.Now().Add(idle / 2).Before(start.Add(maxTTL)) {
			e.clock.Advance(idle / 2)
			resp := e.get(authHost, "/", c, nil)
			require.Equal(t, "Hello, client", readBody(t, resp), e.clock.Since(start).String())
			if next := previewCookie(resp); next != nil {
				c = next
				slid++
			}
		}
		assert.Positive(t, slid, "the session slid at least once")
		assert.False(t, c.Expires.After(start.Add(maxTTL)), "never past the cap")

		e.clock.Advance(start.Add(maxTTL + time.Second).Sub(e.clock.Now()))
		assert.Contains(t, readBody(t, e.get(authHost, "/", c, nil)), `autocomplete="current-password"`)
	})

	t.Run("idle-expiry", func(t *testing.T) {
		t.Parallel()
		e := startPreviewEnv(t)
		e.setDevMode("app-auth", authRef, true)
		e.waitForKeys(authHost, authOrigin)
		c := e.session(authHost, authOrigin)
		e.clock.Advance(session.IdleTTL + time.Second)
		assert.Contains(t, readBody(t, e.get(authHost, "/", c, nil)), `autocomplete="current-password"`)
	})

	t.Run("websocket", func(t *testing.T) {
		t.Parallel()
		e := startPreviewEnv(t)
		e.setDevMode("app-auth", authRef, true)
		e.waitForKeys(authHost, authOrigin)
		c := e.session(authHost, authOrigin)
		cookieHeader := http.Header{"Cookie": {session.CookieName + "=" + c.Value}}

		conn, resp, err := websocket.Dial(t.Context(), "wss://"+authHost+"/ws", &websocket.DialOptions{HTTPClient: e.client, HTTPHeader: cookieHeader})
		require.NoError(t, err)
		assert.Nil(t, previewCookie(resp))
		var v any
		require.NoError(t, wsjson.Read(t.Context(), conn, &v))
		require.NoError(t, conn.Close(websocket.StatusNormalClosure, ""))

		e.clock.Advance(session.IdleTTL / 2)
		conn, resp, err = websocket.Dial(t.Context(), "wss://"+authHost+"/ws", &websocket.DialOptions{HTTPClient: e.client, HTTPHeader: cookieHeader})
		require.NoError(t, err)
		assert.NotNil(t, previewCookie(resp), "an upgrade slides the session like any request")
		require.NoError(t, wsjson.Read(t.Context(), conn, &v))
		require.NoError(t, conn.Close(websocket.StatusNormalClosure, ""))

		_, _, err = websocket.Dial(t.Context(), "wss://"+authHost+"/ws", &websocket.DialOptions{HTTPClient: e.client})
		assert.Error(t, err, "no session: the upgrade gets the login page, not a websocket")
	})

	t.Run("sandbox", func(t *testing.T) {
		t.Parallel()
		e := startPreviewEnv(t)
		e.setDevMode("app-auth", authRef, true)
		setupDraftSandbox("auth", authOrigin, "draft-auth", "https://draft-auth.hub.keboola.local")(t, e.mocked.TestFakeK8sClient(), e.d.AppStateWatcher())
		e.waitForKeys(authHost, authOrigin)
		draftHost := "draft-auth.hub.keboola.local"
		draftOrigin := "https://" + draftHost
		draftRef := k8sapp.WorkloadRef{AppID: "auth", SandboxName: "draft-auth"}

		assert.Equal(t, http.StatusUnauthorized, e.redeem(draftHost, e.link(draftOrigin), nil).StatusCode, "Sandbox without spec.devMode.enabled")

		e.setDevMode("draft-auth", draftRef, true)
		assert.Equal(t, http.StatusUnauthorized, e.redeem(draftHost, e.link(authOrigin), nil).StatusCode, "parent app link on the draft host")
		draftCookie := e.session(draftHost, draftOrigin)
		assert.Equal(t, "Hello, client", readBody(t, e.get(draftHost, "/", draftCookie, nil)))

		assert.Contains(t, readBody(t, e.get(authHost, "/", draftCookie, nil)), `autocomplete="current-password"`, "draft cookie on the parent host")
		appCookie := e.session(authHost, authOrigin)
		assert.Contains(t, readBody(t, e.get(draftHost, "/", appCookie, nil)), `autocomplete="current-password"`, "parent cookie on the draft host")

		e.setDevMode("draft-auth", draftRef, false)
		assert.Contains(t, readBody(t, e.get(draftHost, "/", draftCookie, nil)), `autocomplete="current-password"`, "gate applies to Sandbox hosts")
	})

	// kai-preview-coexistence: the pre-existing kai-preview iframe-auth flow
	// keeps working once the preview session gate is wired in, both on its own
	// and side-by-side with a preview session on the same dev-mode app.
	t.Run("kai-preview-coexistence", func(t *testing.T) {
		t.Parallel()
		e := startPreviewEnv(t)
		e.setDevMode("app-auth", authRef, true)
		e.waitForKeys(authHost, authOrigin)

		sessionKey := e.mocked.TestConfig().KaiPreview.SessionSigningKey
		kaiJWT, err := kaipreview.MintSessionJWT(sessionKey, e.clock, "auth", "123", 4*time.Hour)
		require.NoError(t, err)
		kaiCookie := &http.Cookie{Name: kaipreview.SessionCookieName, Value: kaiJWT}

		// a. A valid kai cookie alone reaches the app, on a plain request and on
		// an iframe document load (which would otherwise get the bootstrap shim).
		resp := e.get(authHost, "/", kaiCookie, nil)
		assert.Equal(t, "Hello, client", readBody(t, resp))
		kaiOnlyRequests := *e.appServer.Requests
		require.NotEmpty(t, kaiOnlyRequests)
		kaiOnlyCookie, err := kaiOnlyRequests[len(kaiOnlyRequests)-1].Cookie(kaipreview.SessionCookieName)
		require.NoError(t, err)

		resp = e.get(authHost, "/", kaiCookie, map[string]string{"Sec-Fetch-Dest": "iframe", "Accept": "text/html"})
		assert.Equal(t, "Hello, client", readBody(t, resp), "iframe load with a valid kai session reaches the app, not the bootstrap shim")

		// b. A valid kai cookie plus a garbage preview cookie still reaches the
		// app via the kai-preview path.
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, authOrigin+"/", nil)
		require.NoError(t, err)
		req.AddCookie(kaiCookie)
		req.AddCookie(&http.Cookie{Name: session.CookieName, Value: "garbage"})
		assert.Equal(t, "Hello, client", readBody(t, e.do(req)))

		// c. A valid preview cookie plus a valid kai cookie reaches the app via
		// the preview session gate. The preview cookie is stripped; the kai
		// cookie reaches the upstream exactly as it does on a kai-only request.
		previewSessionCookie := e.session(authHost, authOrigin)
		req, err = http.NewRequestWithContext(t.Context(), http.MethodGet, authOrigin+"/", nil)
		require.NoError(t, err)
		req.AddCookie(kaiCookie)
		req.AddCookie(previewSessionCookie)
		resp = e.do(req)
		assert.Equal(t, "Hello, client", readBody(t, resp))
		bothRequests := *e.appServer.Requests
		require.NotEmpty(t, bothRequests)
		last := bothRequests[len(bothRequests)-1]
		_, err = last.Cookie(session.CookieName)
		require.ErrorIs(t, err, http.ErrNoCookie)
		bothKaiCookie, err := last.Cookie(kaipreview.SessionCookieName)
		require.NoError(t, err)
		assert.Equal(t, kaiOnlyCookie.Value, bothKaiCookie.Value, "kai cookie reaches upstream exactly as on a kai-only request")

		// d. A /_proxy/kai-preview/* endpoint request is served by the kai
		// handler the same way whether or not a preview cookie is present,
		// and never reaches the app.
		requestsBefore := len(*e.appServer.Requests)

		without, err := http.NewRequestWithContext(t.Context(), http.MethodGet, authOrigin+"/_proxy/kai-preview/bootstrap", nil)
		require.NoError(t, err)
		respWithout := e.do(without)
		bodyWithout := readBody(t, respWithout)
		assert.NotEqual(t, "Hello, client", bodyWithout)
		assert.Len(t, *e.appServer.Requests, requestsBefore)

		with, err := http.NewRequestWithContext(t.Context(), http.MethodGet, authOrigin+"/_proxy/kai-preview/bootstrap", nil)
		require.NoError(t, err)
		with.AddCookie(previewSessionCookie)
		respWith := e.do(with)
		bodyWith := readBody(t, respWith)
		assert.NotEqual(t, "Hello, client", bodyWith)
		assert.Len(t, *e.appServer.Requests, requestsBefore)

		assert.Equal(t, respWithout.StatusCode, respWith.StatusCode)
		assert.Equal(t, bodyWithout, bodyWith)
	})
}
