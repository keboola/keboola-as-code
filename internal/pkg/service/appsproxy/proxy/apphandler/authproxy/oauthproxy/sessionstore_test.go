package oauthproxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jonboulle/clockwork"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	sessionsapi "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/sessions"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type pageWriterDeps struct{}

func (pageWriterDeps) Clock() clockwork.Clock { return clockwork.NewRealClock() }
func (pageWriterDeps) Logger() log.Logger     { return log.NewNopLogger() }

func TestProxyConfig_RedisStoreUnavailableWritesErrorPage(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	pw, err := pagewriter.New(pageWriterDeps{})
	require.NoError(t, err)

	cfg, app, scopes, issuer := testProxyConfigEnv(t, `["openid"]`)
	v, err := proxyConfig(t.Context(), cfg, SessionStore{Client: client}, scopes, nil, pw, app, testOIDC(issuer, nil), nil, nil)
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
}

func TestProxyConfig_CookieStoreByDefault(t *testing.T) {
	t.Parallel()

	cfg, app, scopes, issuer := testProxyConfigEnv(t, `["openid","offline_access"]`)
	v, err := proxyConfig(t.Context(), cfg, SessionStore{}, scopes, nil, nil, app, testOIDC(issuer, nil), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, options.CookieSessionStoreType, v.Session.Type)
	assert.Equal(t, "_oauth2_proxy", v.Cookie.Name)
	assert.Nil(t, v.Session.Redis.Client)
}

func TestProxyConfig_RedisStore(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	cfg, app, scopes, issuer := testProxyConfigEnv(t, `["openid","email","profile","offline_access"]`)
	v, err := proxyConfig(t.Context(), cfg, SessionStore{Client: client}, scopes, nil, nil, app, testOIDC(issuer, nil), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, options.RedisSessionStoreType, v.Session.Type)
	assert.Same(t, client, v.Session.Redis.Client)
	assert.Equal(t, "_oauth2_proxy_ticket", v.Cookie.Name)
	assert.Equal(t, time.Hour, v.Cookie.Refresh)
	assert.Equal(t, 24*time.Hour, v.Cookie.Expire)
	require.Len(t, v.Providers, 1)
	assert.Contains(t, v.Providers[0].Scope, "offline_access")
}

func TestProxyConfig_RedisStoreBuildsWithStoreDown(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	mr.Close()

	cfg, app, scopes, issuer := testProxyConfigEnv(t, `["openid"]`)
	_, err := proxyConfig(t.Context(), cfg, SessionStore{Client: client}, scopes, nil, nil, app, testOIDC(issuer, nil), nil, nil)
	require.NoError(t, err)
}

// The cookie store clears every cookie matching its own name pattern, so the two
// store types must not share a name while old and new replicas run side by side.
func TestStoreCookieNames_Disjoint(t *testing.T) {
	t.Parallel()

	cookieStorePattern := regexp.MustCompile(`^_oauth2_proxy(_\d+)?$`)
	assert.False(t, cookieStorePattern.MatchString(redisSessionCookieName))
	assert.True(t, cookieStorePattern.MatchString(options.NewOptions().Cookie.Name))
}
