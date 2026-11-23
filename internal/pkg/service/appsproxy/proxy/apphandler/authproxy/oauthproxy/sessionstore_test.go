package oauthproxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jonboulle/clockwork"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	sessionsapi "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/sessions"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/sessionstore"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type pageWriterDeps struct{}

func (pageWriterDeps) Clock() clockwork.Clock { return clockwork.NewRealClock() }
func (pageWriterDeps) Logger() log.Logger     { return log.NewNopLogger() }

func testSessionStore(t *testing.T) SessionStore {
	t.Helper()
	store, _ := testSessionStoreWithTelemetry(t)
	return store
}

func testSessionStoreWithTelemetry(t *testing.T) (SessionStore, telemetry.ForTest) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	tel := telemetry.NewForTest(t)
	checker := sessionstore.NewChecker(log.NewNopLogger(), clockwork.NewFakeClock(), client)
	return SessionStore{Client: client, Metrics: sessionstore.NewMetrics(tel.Meter(), client, checker)}, tel
}

func unavailableCount(t *testing.T, tel telemetry.ForTest) int64 {
	t.Helper()
	for _, m := range tel.Metrics(t) {
		if m.Name != "keboola.go.appsproxy.sessionstore.unavailable" {
			continue
		}
		sum, ok := m.Data.(metricdata.Sum[int64])
		require.True(t, ok, "%T", m.Data)
		var total int64
		for _, dp := range sum.DataPoints {
			total += dp.Value
		}
		return total
	}
	return 0
}

func TestProxyConfig_StoreErrorOnCanceledRequest(t *testing.T) {
	t.Parallel()

	pw, err := pagewriter.New(pageWriterDeps{})
	require.NoError(t, err)
	store, tel := testSessionStoreWithTelemetry(t)
	cfg, app, scopes, issuer := testProxyConfigEnv(t, `["openid"]`)
	v, err := proxyConfig(t.Context(), log.NewNopLogger(), cfg, store, scopes, nil, pw, app, testOIDC(issuer, nil), nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(errors.New("client went away"))
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "https://app.hub.keboola.local/", nil)
	v.OnSessionStoreError(rec, req, errors.Errorf("load: %w: %w", sessionsapi.ErrStoreUnavailable, context.Canceled))
	assert.Equal(t, 499, rec.Code)
	assert.Empty(t, rec.Body.String())
	assert.Zero(t, unavailableCount(t, tel))
}

func TestProxyConfig_StoreUnavailableWritesErrorPage(t *testing.T) {
	t.Parallel()

	store, tel := testSessionStoreWithTelemetry(t)
	pw, err := pagewriter.New(pageWriterDeps{})
	require.NoError(t, err)

	cfg, app, scopes, issuer := testProxyConfigEnv(t, `["openid"]`)
	v, err := proxyConfig(t.Context(), log.NewNopLogger(), cfg, store, scopes, nil, pw, app, testOIDC(issuer, nil), nil)
	require.NoError(t, err)
	require.NotNil(t, v.OnSessionStoreError)

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://app.hub.keboola.local/", nil)
	v.OnSessionStoreError(rec, req, errors.Errorf("load: %w", sessionsapi.ErrStoreUnavailable))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "Sign-in is temporarily unavailable. Please try again in a moment.")
	assert.Empty(t, rec.Result().Cookies())
	assert.Equal(t, int64(1), unavailableCount(t, tel))
}

func TestProxyConfig_SessionStore(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	cfg, app, scopes, issuer := testProxyConfigEnv(t, `["openid","email","profile","offline_access"]`)
	v, err := proxyConfig(t.Context(), log.NewNopLogger(), cfg, SessionStore{Client: client}, scopes, nil, nil, app, testOIDC(issuer, nil), nil)
	require.NoError(t, err)
	assert.Equal(t, options.RedisSessionStoreType, v.Session.Type)
	assert.Same(t, client, v.Session.Redis.Client)
	assert.Equal(t, "_oauth2_proxy_ticket", v.Cookie.Name)
	assert.Equal(t, time.Hour, v.Cookie.Refresh)
	assert.Equal(t, 24*time.Hour, v.Cookie.Expire)
	require.Len(t, v.Providers, 1)
	assert.Contains(t, v.Providers[0].Scope, "offline_access")
}

func TestProxyConfig_BuildsWithStoreDown(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	mr.Close()

	cfg, app, scopes, issuer := testProxyConfigEnv(t, `["openid"]`)
	_, err := proxyConfig(t.Context(), log.NewNopLogger(), cfg, SessionStore{Client: client}, scopes, nil, nil, app, testOIDC(issuer, nil), nil)
	require.NoError(t, err)
}
