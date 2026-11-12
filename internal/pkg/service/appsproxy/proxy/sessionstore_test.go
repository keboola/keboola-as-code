package proxy_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/oauth2-proxy/mockoidc"
	sessionsapi "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/sessions"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/previewtest"
)

var oidcCookieURL = &url.URL{Scheme: "https", Host: oidcHost, Path: "/"} //nolint:gochecknoglobals // test fixture

func manyGroupsUser(n int) *mockoidc.MockUser {
	groups := make([]string, 0, n)
	groups = append(groups, "admin")
	for i := range n - 1 {
		groups = append(groups, fmt.Sprintf("group-with-a-long-name-%03d", i))
	}
	return &mockoidc.MockUser{Subject: "sub-1", Email: "admin@keboola.com", EmailVerified: new(true), Groups: groups}
}

func fixedCookieSalt(cfg *config.Config, _ *previewtest.JWKSServer) {
	cfg.CookieSecretSalt = strings.Repeat("c", 32)
}

func (e *previewEnv) ticketKeys() []string {
	e.t.Helper()
	var keys []string
	for _, k := range e.redis.Keys() {
		if strings.HasPrefix(k, ticketCookieName+"-") && !strings.HasSuffix(k, ".lock") {
			keys = append(keys, k)
		}
	}
	return keys
}

// clientWithCookies returns a client of this env holding a copy of the other client's oidc cookies.
func (e *previewEnv) clientWithCookies(other *http.Client) *http.Client {
	e.t.Helper()
	jar, err := cookiejar.New(nil)
	require.NoError(e.t, err)
	client := *e.client
	client.Jar = jar
	if other != nil {
		jar.SetCookies(oidcCookieURL, other.Jar.Cookies(oidcCookieURL))
	}
	return &client
}

func expiresCookie(resp *http.Response, name string) bool {
	for _, c := range resp.Cookies() {
		if c.Name == name && (c.MaxAge < 0 || (!c.Expires.IsZero() && c.Expires.Before(time.Now()))) {
			return true
		}
	}
	return false
}

func jarCookie(client *http.Client, name string) *http.Cookie {
	for _, c := range client.Jar.Cookies(oidcCookieURL) {
		if c.Name == name {
			return c
		}
	}
	return nil
}

func TestRedisStore_LoginRoundTrip(t *testing.T) {
	t.Parallel()
	env := startRedisEnv(t)
	client := env.oidcLogin(env.providers[0], manyGroupsUser(200), oidcOrigin+"/")

	jarCookies := client.Jar.Cookies(oidcCookieURL)
	names := make([]string, 0, len(jarCookies))
	for _, c := range jarCookies {
		names = append(names, c.Name)
	}
	assert.NotContains(t, names, oauthCookieName)
	for _, name := range names {
		assert.NotRegexp(t, `^_oauth2_proxy_\d+$`, name)
	}
	ticket := jarCookie(client, ticketCookieName)
	require.NotNil(t, ticket)
	assert.Less(t, len(ticket.Name)+1+len(ticket.Value), 512)

	keys := env.ticketKeys()
	require.Len(t, keys, 1)
	assert.Equal(t, 24*time.Hour, env.redis.TTL(keys[0]))

	resp := env.send(client, oidcOrigin+"/some/path")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	appReq := env.lastAppRequest()
	assert.Equal(t, "admin@keboola.com", appReq.Header.Get("X-Kbc-User-Email"))
	assert.Contains(t, appReq.Header.Get("X-Kbc-User-Roles"), "group-with-a-long-name-198")
}

func TestRedisStore_OutageKeepsSession(t *testing.T) {
	t.Parallel()
	env := startRedisEnv(t)
	client := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")

	env.redis.Close()
	resp := env.send(client, oidcOrigin+"/some/path")
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.False(t, expiresCookie(resp, ticketCookieName), "the session cookie must be kept")
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "Sign-in is temporarily unavailable. Please try again in a moment.")
	require.NotNil(t, jarCookie(client, ticketCookieName))

	require.NoError(t, env.redis.Restart())
	resp = env.send(client, oidcOrigin+"/some/path")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestCookieStore_IgnoresRedisStoreTicket(t *testing.T) {
	t.Parallel()
	cookieEnv := startPreviewEnv(t, fixedCookieSalt)
	redisEnv := startRedisEnv(t, fixedCookieSalt)

	// A ticket at a cookie-mode replica: login page, the ticket stays.
	redisClient := redisEnv.oidcLogin(redisEnv.providers[0], freshnessUser(), oidcOrigin+"/")
	resp := cookieEnv.send(cookieEnv.clientWithCookies(redisClient), oidcOrigin+"/some/path")
	require.Equal(t, http.StatusFound, resp.StatusCode)
	assert.False(t, expiresCookie(resp, ticketCookieName))
}

func TestRedisStore_RebuildDoesNotLeak(t *testing.T) { //nolint:paralleltest // compares the goroutine count of the whole process
	env := startRedisEnv(t)
	client := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")

	rebuild := func(i int) {
		app := env.appsAPI.Apps["oidc"]
		app.Name = fmt.Sprintf("oidc-%d", i)
		env.appsAPI.Apps["oidc"] = app
		resp := env.send(client, oidcOrigin+"/some/path")
		require.Equal(t, http.StatusOK, resp.StatusCode)
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	for i := range 5 {
		rebuild(i)
	}
	baseConns := env.redisStore.PoolStats().TotalConns
	baseGoroutines := runtime.NumGoroutine()

	for i := 5; i < 55; i++ {
		rebuild(i)
	}

	assert.LessOrEqual(t, env.redisStore.PoolStats().TotalConns, baseConns+2)
	assert.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= baseGoroutines+20
	}, 10*time.Second, 100*time.Millisecond, "goroutines grew from %d to %d", baseGoroutines, runtime.NumGoroutine())
}

func TestRedisStore_HandlerBuildWithStoreDown(t *testing.T) {
	t.Parallel()
	env := startRedisEnv(t)

	env.redis.Close()
	resp := env.send(env.clientWithCookies(nil), oidcOrigin+"/some/path")
	require.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Location"), "/oidc/authorize?client_id=")

	require.NoError(t, env.redis.Restart())
	client := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")
	resp = env.send(client, oidcOrigin+"/some/path")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRedisStore_Eviction(t *testing.T) {
	t.Parallel()
	env := startRedisEnv(t)
	client := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")

	keys := env.ticketKeys()
	require.Len(t, keys, 1)
	env.redis.Del(keys[0])

	resp := env.send(client, oidcOrigin+"/some/path")
	require.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Location"), "/oidc/authorize?client_id=")
	assert.True(t, expiresCookie(resp, ticketCookieName))
}

func TestRedisStore_SignOutDeletesKey(t *testing.T) {
	t.Parallel()
	env := startRedisEnv(t)
	client := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")
	require.Len(t, env.ticketKeys(), 1)

	env.send(client, oidcOrigin+"/_proxy/sign_out")
	assert.Empty(t, env.ticketKeys())
}

func countTrue(values []bool) int {
	n := 0
	for _, v := range values {
		if v {
			n++
		}
	}
	return n
}

func TestRedisStore_ParallelRefreshRefreshesOnce(t *testing.T) {
	t.Parallel()
	env := startRedisEnv(t)
	client := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")
	env.rewriteOIDCSession(client, func(s *sessionsapi.SessionState) {
		require.NotEmpty(t, s.RefreshToken)
		s.CreatedAt = ago(2 * time.Hour)
	})
	// A request may wait for the refresh lock up to the fork's obtain timeout (35 s).
	waitingClient := *client
	waitingClient.Timeout = time.Minute

	const n = 10
	start := make(chan struct{})
	var wg sync.WaitGroup
	statuses := make([]int, n)
	errs := make([]error, n)
	refreshed := make([]bool, n)
	cleared := make([]bool, n)
	for i := range n {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, oidcOrigin+"/some/path", nil)
		require.NoError(t, err)
		wg.Go(func() {
			<-start
			resp, err := waitingClient.Do(req)
			if err != nil {
				errs[i] = err
				return
			}
			statuses[i] = resp.StatusCode
			cleared[i] = expiresCookie(resp, ticketCookieName)
			refreshed[i] = !cleared[i] && slices.Contains(setCookieNames(resp), ticketCookieName)
			_ = resp.Body.Close()
		})
	}
	close(start)
	wg.Wait()

	// No request may log the user out. A slow runner can give a 503 (store busy), never a 302.
	for i := range n {
		require.NoError(t, errs[i], "request %d", i)
		assert.Contains(t, []int{http.StatusOK, http.StatusServiceUnavailable}, statuses[i], "request %d", i)
		assert.False(t, cleared[i], "request %d cleared the session cookie", i)
	}
	assert.LessOrEqual(t, countTrue(refreshed), 1, "at most one request refreshes and saves the session")
	if !slices.Contains(statuses, http.StatusServiceUnavailable) {
		assert.Equal(t, 1, countTrue(refreshed), "exactly one request refreshes and saves the session")
	}
	session := env.loadOIDCSession(client)
	if countTrue(refreshed) == 1 {
		assert.WithinDuration(t, time.Now(), *session.CreatedAt, time.Minute)
		assert.Equal(t, 24*time.Hour, env.redis.TTL(env.ticketKeys()[0]))
	}
}

func (e *previewEnv) counterValue(name string) int64 {
	e.t.Helper()
	for _, m := range e.mocked.TestTelemetry().Metrics(e.t) {
		if m.Name != name {
			continue
		}
		if sum, ok := m.Data.(metricdata.Sum[int64]); ok && len(sum.DataPoints) > 0 {
			return sum.DataPoints[0].Value
		}
	}
	return 0
}

func TestHealthCheck_IgnoresSessionStore(t *testing.T) {
	t.Parallel()
	env := startRedisEnv(t)
	oidcClient := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")
	basicClient := env.basicAuthLogin()
	require.Eventually(t, env.d.SessionStoreChecker().Up, 5*time.Second, 10*time.Millisecond)

	env.redis.Close()

	resp := env.send(env.clientWithCookies(nil), "https://hub.keboola.local/health-check")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, http.StatusOK, env.send(basicClient, authOrigin+"/").StatusCode)
	assert.Equal(t, http.StatusOK, env.send(env.clientWithCookies(nil), publicOrigin+"/").StatusCode)

	resp = env.send(oidcClient, oidcOrigin+"/some/path")
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Equal(t, int64(1), env.counterValue("keboola.go.appsproxy.sessionstore.unavailable"))
}

// stopStoreBeforeNextSet closes miniredis right before the next SET command reaches it.
type stopStoreBeforeNextSet struct {
	mr    *miniredis.Miniredis
	armed atomic.Bool
}

func (h *stopStoreBeforeNextSet) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *stopStoreBeforeNextSet) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "set" && h.armed.CompareAndSwap(true, false) {
			h.mr.Close()
		}
		return next(ctx, cmd)
	}
}

func (h *stopStoreBeforeNextSet) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestRedisStore_SaveAfterRefreshFails_KeepsSession(t *testing.T) {
	t.Parallel()
	env := startRedisEnv(t)
	client := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")
	env.rewriteOIDCSession(client, func(s *sessionsapi.SessionState) {
		require.NotEmpty(t, s.RefreshToken)
		s.CreatedAt = ago(2 * time.Hour)
	})
	hook := &stopStoreBeforeNextSet{mr: env.redis}
	env.redisStore.AddHook(hook)
	hook.armed.Store(true)

	resp := env.send(client, oidcOrigin+"/some/path")
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.False(t, expiresCookie(resp, ticketCookieName), "the session cookie must be kept")
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "Sign-in is temporarily unavailable. Please try again in a moment.")
	assert.False(t, hook.armed.Load(), "the store stopped before the save of the refreshed session")

	require.NoError(t, env.redis.Restart())
	// The refresh lock could not be released while the store was down. It expires on its TTL
	// (the fork's default, 30 s); miniredis only lets TTLs run out on FastForward.
	assert.Len(t, slices.DeleteFunc(env.redis.Keys(), func(k string) bool { return !strings.HasSuffix(k, ".lock") }), 1)
	env.redis.FastForward(31 * time.Second)
	resp = env.send(client, oidcOrigin+"/some/path")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
