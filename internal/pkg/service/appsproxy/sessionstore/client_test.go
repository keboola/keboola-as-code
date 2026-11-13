package sessionstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
)

func redisConfig() config.SessionStore {
	cfg := config.New().SessionStore
	cfg.Type = config.SessionStoreRedis
	cfg.SentinelAddrs = []string{"a:26379", "b:26379"}
	cfg.SentinelMasterName = "mymaster"
	cfg.SentinelPassword = "sentinel-secret"
	cfg.RedisUsername = "appsproxy"
	cfg.RedisPassword = "appsproxy-secret"
	return cfg
}

func TestNewClient_CookieIsNil(t *testing.T) {
	t.Parallel()
	assert.Nil(t, NewClient(config.New().SessionStore))
}

func TestNewClient_RedisOpensNoConnection(t *testing.T) {
	t.Parallel()
	client := NewClient(redisConfig())
	require.NotNil(t, client)
	t.Cleanup(func() { _ = client.Close() })
	assert.Zero(t, client.PoolStats().TotalConns)
}

func TestFailoverOptions(t *testing.T) {
	t.Parallel()

	cfg := redisConfig()
	cfg.DialTimeout = 100 * time.Millisecond
	cfg.ReadTimeout = 200 * time.Millisecond
	cfg.WriteTimeout = 300 * time.Millisecond
	cfg.PoolSize = 7

	o := failoverOptions(cfg)
	assert.Equal(t, []string{"a:26379", "b:26379"}, o.SentinelAddrs)
	assert.Equal(t, "mymaster", o.MasterName)
	assert.Equal(t, "sentinel", o.SentinelUsername)
	assert.Equal(t, "sentinel-secret", o.SentinelPassword)
	assert.Equal(t, "appsproxy", o.Username)
	assert.Equal(t, "appsproxy-secret", o.Password)
	assert.Equal(t, 1, o.MaxRetries)
	assert.Equal(t, 100*time.Millisecond, o.DialTimeout)
	assert.Equal(t, 200*time.Millisecond, o.ReadTimeout)
	assert.Equal(t, 300*time.Millisecond, o.WriteTimeout)
	assert.Equal(t, 7, o.PoolSize)
}

func TestFailoverOptions_ZeroRetriesDisablesRetries(t *testing.T) {
	t.Parallel()

	cfg := redisConfig()
	cfg.MaxRetries = 0
	assert.Equal(t, -1, failoverOptions(cfg).MaxRetries)
}
