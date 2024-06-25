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
	Collector SyncConfig  `configKey:"sync"`
	Cache     CacheConfig `configKey:"cache"`
}

type CacheConfig struct {
	L2 L2CacheConfig `configKey:"L2"`
}

type SyncConfig struct {
	Enabled      bool              // the timer may cause problems with tests, so the collector can be disabled
	SyncInterval duration.Duration `configKey:"interval" configUsage:"Statistics synchronization interval, from memory to the etcd." validate:"required,minDuration=100ms,maxDuration=5s"`
	SyncTimeout  duration.Duration `configKey:"timeout" configUsage:"Statistics synchronization timeout." validate:"required,minDuration=1s,maxDuration=1m"`
}

type L2CacheConfig struct {
	Enabled              bool              `configKey:"enabled" configUsage:"Enable statistics L2 in-memory cache, otherwise only L1 cache is used."`
	InvalidationInterval duration.Duration `configKey:"interval" configUsage:"Statistics L2 in-memory cache invalidation interval." validate:"required,minDuration=100ms,maxDuration=5s"`
}

func NewConfig() Config {
	return Config{
		Collector: SyncConfig{
			Enabled:      true,
			SyncInterval: duration.From(DefaultSyncInterval),
			SyncTimeout:  duration.From(DefaultSyncTimeout),
		},
		Cache: CacheConfig{
			L2: L2CacheConfig{
				Enabled:              true,
				InvalidationInterval: duration.From(DefaultL2CacheInvalidationInterval),
			},
		},
	}
}
