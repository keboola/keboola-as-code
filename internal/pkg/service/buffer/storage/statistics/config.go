package statistics

import (
	"time"
)

const (
	DefaultSyncInterval                = 1 * time.Second
	DefaultSyncTimeout                 = 30 * time.Second
	DefaultL2CacheInvalidationInterval = time.Second
)

type Config struct {
	Collector SyncConfig    `configKey:"sync"`
	L2Cache   L2CacheConfig `configKey:"cache"`
}

type SyncConfig struct {
	SyncInterval time.Duration `configKey:"interval" configUsage:"Statistics synchronization interval, from memory to the etcd." validate:"required"`
	SyncTimeout  time.Duration `configKey:"timeout" configUsage:"Statistics synchronization timeout."  validate:"required"`
}

type L2CacheConfig struct {
	InvalidationInterval time.Duration `configKey:"invalidationInterval" configUsage:"Statistics L2 in-memory cache invalidation interval."`
}

func NewConfig() Config {
	return Config{
		Collector: SyncConfig{
			SyncInterval: DefaultSyncInterval,
			SyncTimeout:  DefaultSyncTimeout,
		},
		L2Cache: L2CacheConfig{
			InvalidationInterval: DefaultL2CacheInvalidationInterval,
		},
	}
}
