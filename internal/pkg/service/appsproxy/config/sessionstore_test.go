package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

func TestSessionStoreConfig_Defaults(t *testing.T) {
	t.Parallel()

	cfg := config.New()
	assert.Equal(t, "sentinel", cfg.SessionStore.SentinelUsername)

	valid := requiredConfig(t)
	require.NoError(t, configmap.ValidateAndNormalize(&valid))
}

func TestSessionStoreConfig_Required(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		mutate  func(c *config.SessionStore)
		wantErr string
	}{
		{name: "no sentinel addrs", mutate: func(c *config.SessionStore) { c.SentinelAddrs = nil }, wantErr: "sessionStore.sentinelAddrs"},
		{name: "no master name", mutate: func(c *config.SessionStore) { c.SentinelMasterName = "" }, wantErr: "sessionStore.sentinelMasterName"},
		{name: "no sentinel username", mutate: func(c *config.SessionStore) { c.SentinelUsername = "" }, wantErr: "sessionStore.sentinelUsername"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := requiredConfig(t)
			tc.mutate(&cfg.SessionStore)
			err := configmap.ValidateAndNormalize(&cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestSessionStoreConfig_DefaultsAreNotEnough(t *testing.T) {
	t.Parallel()

	cfg := requiredConfig(t)
	cfg.SessionStore = config.New().SessionStore
	err := configmap.ValidateAndNormalize(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sessionStore.sentinelAddrs")
	assert.Contains(t, err.Error(), "sessionStore.sentinelMasterName")
}

// TestSessionStoreConfig_ENV pins the ENV names down: they are what the kbc-stacks chart sets
// in apps/apps-proxy/templates/secret-config.yaml.
// APPS_PROXY_SESSION_STORE_TYPE is no longer a key; stacks may still set it, and it must be ignored.
func TestSessionStoreConfig_ENV(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		addrs     string
		wantAddrs []string
	}{
		{name: "one address, as the chart sets it", addrs: "apps-proxy-valkey-sentinel:26379", wantAddrs: []string{"apps-proxy-valkey-sentinel:26379"}},
		{name: "address list", addrs: "a:26379,b:26379", wantAddrs: []string{"a:26379", "b:26379"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := requiredConfig(t)
			envs := env.Empty()
			envs.Set("APPS_PROXY_SESSION_STORE_TYPE", "redis")
			envs.Set("APPS_PROXY_SESSION_STORE_SENTINEL_ADDRS", tc.addrs)
			envs.Set("APPS_PROXY_SESSION_STORE_SENTINEL_MASTER_NAME", "mymaster")
			envs.Set("APPS_PROXY_SESSION_STORE_SENTINEL_USERNAME", "sentinel")
			envs.Set("APPS_PROXY_SESSION_STORE_SENTINEL_PASSWORD", "sentinel-secret")
			envs.Set("APPS_PROXY_SESSION_STORE_REDIS_USERNAME", "appsproxy")
			envs.Set("APPS_PROXY_SESSION_STORE_REDIS_PASSWORD", "appsproxy-secret")
			require.NoError(t, configmap.GenerateAndBind(configmap.GenerateAndBindConfig{
				EnvNaming: env.NewNamingConvention("APPS_PROXY_"),
				Envs:      envs,
			}, &cfg))

			assert.Equal(t, tc.wantAddrs, cfg.SessionStore.SentinelAddrs)
			assert.Equal(t, "mymaster", cfg.SessionStore.SentinelMasterName)
			assert.Equal(t, "sentinel", cfg.SessionStore.SentinelUsername)
			assert.Equal(t, "sentinel-secret", cfg.SessionStore.SentinelPassword)
			assert.Equal(t, "appsproxy", cfg.SessionStore.RedisUsername)
			assert.Equal(t, "appsproxy-secret", cfg.SessionStore.RedisPassword)
		})
	}
}

func TestSessionStoreConfig_ENVSentinelUsername(t *testing.T) {
	t.Parallel()

	cfg := requiredConfig(t)
	cfg.SessionStore.SentinelUsername = ""
	envs := env.Empty()
	envs.Set("APPS_PROXY_SESSION_STORE_SENTINEL_USERNAME", "sentinel")
	require.NoError(t, configmap.GenerateAndBind(configmap.GenerateAndBindConfig{
		EnvNaming: env.NewNamingConvention("APPS_PROXY_"),
		Envs:      envs,
	}, &cfg))
	assert.Equal(t, "sentinel", cfg.SessionStore.SentinelUsername)
}
