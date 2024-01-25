package statistics

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
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
	SyncInterval duration.Duration `configKey:"interval" configUsage:"Statistics synchronization interval, from memory to the etcd." validate:"required,minDuration=100ms,maxDuration=5s"`
	SyncTimeout  duration.Duration `configKey:"timeout" configUsage:"Statistics synchronization timeout."  validate:"required,minDuration=1s,maxDuration=1m"`
}

type L2CacheConfig struct {
	InvalidationInterval duration.Duration `configKey:"invalidationInterval" configUsage:"Statistics L2 in-memory cache invalidation interval." validate:"required,minDuration=100ms,maxDuration=5s"`
}

func NewConfig() Config {
	return Config{
		Collector: SyncConfig{
			SyncInterval: duration.From(DefaultSyncInterval),
			SyncTimeout:  duration.From(DefaultSyncTimeout),
		},
		L2Cache: L2CacheConfig{
			InvalidationInterval: duration.From(DefaultL2CacheInvalidationInterval),
		},
	}
}
