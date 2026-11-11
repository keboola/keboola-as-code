package oauthproxy

import (
	"regexp"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProxyConfig_CookieStoreByDefault(t *testing.T) {
	t.Parallel()

	cfg, app, scopes, issuer := testProxyConfigEnv(t, `["openid","offline_access"]`)
	v, err := proxyConfig(t.Context(), cfg, nil, scopes, nil, nil, app, testOIDC(issuer, nil), nil)
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
	v, err := proxyConfig(t.Context(), cfg, client, scopes, nil, nil, app, testOIDC(issuer, nil), nil)
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
	_, err := proxyConfig(t.Context(), cfg, client, scopes, nil, nil, app, testOIDC(issuer, nil), nil)
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
