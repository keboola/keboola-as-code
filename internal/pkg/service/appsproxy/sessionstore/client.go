package sessionstore

import (
	"github.com/redis/go-redis/v9"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
)

func NewClient(cfg config.SessionStore) *redis.Client {
	if cfg.Type != config.SessionStoreRedis {
		return nil
	}
	return redis.NewFailoverClient(failoverOptions(cfg))
}

func failoverOptions(cfg config.SessionStore) *redis.FailoverOptions {
	// go-redis reads MaxRetries 0 as its default of 3; -1 disables retries.
	maxRetries := -1
	if cfg.MaxRetries > 0 {
		maxRetries = cfg.MaxRetries
	}
	return &redis.FailoverOptions{
		MasterName:       cfg.SentinelMasterName,
		SentinelAddrs:    cfg.SentinelAddrs,
		SentinelUsername: cfg.SentinelUsername,
		SentinelPassword: cfg.SentinelPassword,
		Username:         cfg.Username,
		Password:         cfg.Password,
		DialTimeout:      cfg.DialTimeout,
		ReadTimeout:      cfg.ReadTimeout,
		WriteTimeout:     cfg.WriteTimeout,
		PoolSize:         cfg.PoolSize,
		MaxRetries:       maxRetries,
	}
}
