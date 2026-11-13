package sessionstore

import (
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
)

// A short timeout and one retry let a request fail to the 503 page within about a second.
const (
	dialTimeout  = 500 * time.Millisecond
	readTimeout  = 500 * time.Millisecond
	writeTimeout = 500 * time.Millisecond
	poolSize     = 20
	maxRetries   = 1
)

func NewClient(cfg config.SessionStore) *redis.Client {
	if cfg.Type != config.SessionStoreRedis {
		return nil
	}
	return redis.NewFailoverClient(failoverOptions(cfg))
}

func failoverOptions(cfg config.SessionStore) *redis.FailoverOptions {
	return &redis.FailoverOptions{
		MasterName:       cfg.SentinelMasterName,
		SentinelAddrs:    cfg.SentinelAddrs,
		SentinelUsername: cfg.SentinelUsername,
		SentinelPassword: cfg.SentinelPassword,
		Username:         cfg.RedisUsername,
		Password:         cfg.RedisPassword,
		DialTimeout:      dialTimeout,
		ReadTimeout:      readTimeout,
		WriteTimeout:     writeTimeout,
		PoolSize:         poolSize,
		MaxRetries:       maxRetries,
	}
}
