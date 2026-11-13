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

	o := failoverOptions(redisConfig())
	assert.Equal(t, []string{"a:26379", "b:26379"}, o.SentinelAddrs)
	assert.Equal(t, "mymaster", o.MasterName)
	assert.Equal(t, "sentinel", o.SentinelUsername)
	assert.Equal(t, "sentinel-secret", o.SentinelPassword)
	assert.Equal(t, "appsproxy", o.Username)
	assert.Equal(t, "appsproxy-secret", o.Password)
	assert.Equal(t, 1, o.MaxRetries)
	assert.Equal(t, 500*time.Millisecond, o.DialTimeout)
	assert.Equal(t, 500*time.Millisecond, o.ReadTimeout)
	assert.Equal(t, 500*time.Millisecond, o.WriteTimeout)
	assert.Equal(t, 20, o.PoolSize)
}
