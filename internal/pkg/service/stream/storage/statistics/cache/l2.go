package cache

import (
	"context"
	"fmt"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/repository"
)

// L2 implements the repository.Provider interface.
//
// The L2 cache is implemented on top of the L1 cache, it caches final aggregated value for the object.
//   - Obtaining statistics does not require any further calculations if the key is found.
//   - If the key is not cached, it is obtained from the L1 cache.
//   - The cache is invalidated according to the configured invalidation interval.
//   - The maximum delay is the sum of the invalidation interval and a few milliseconds delay from the L1 cache.
//   - L2 is faster than L1, but the data is older.
//
// The L2 cache is primarily used by the [quota.Checker] to check limits on each received record.
type L2 struct {
	provider
	logger  log.Logger
	l1Cache *L1

	cancel context.CancelFunc
	wg     *sync.WaitGroup

	enabled   bool
	cacheLock *sync.RWMutex
	cache     l2CachePerObjectKey
	revision  int64
}

type l2CachePerObjectKey map[string]statistics.Aggregated

func NewL2Cache(d dependencies, l1Cache *L1, config statistics.L2CacheConfig) (*L2, error) {
	c := &L2{
		logger:    d.Logger().WithComponent("stats.cache.L2"),
		l1Cache:   l1Cache,
		wg:        &sync.WaitGroup{},
		enabled:   config.Enabled,
		cacheLock: &sync.RWMutex{},
		cache:     make(l2CachePerObjectKey),
	}

	// Setup context for graceful shutdown
	var ctx context.Context
	ctx, c.cancel = context.WithCancel(context.Background())

	// Periodically invalidates the cache.
	if c.enabled {
		c.wg.Add(1)
		ticker := d.Clock().Ticker(config.InvalidationInterval.Duration())
		go func() {
			defer c.wg.Done()
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					c.Clear()
				}
			}
		}()
	}

	// Setup getters
	c.provider = repository.NewProvider(c.aggregate)

	return c, nil
}

func (c *L2) Stop() {
	c.cancel()
	c.wg.Wait()
}

func (c *L2) Revision() int64 {
	c.cacheLock.RLock()
	defer c.cacheLock.RUnlock()

	if c.revision == 0 {
		// There is no cached key, load the revision from L1
		return c.l1Cache.cache.Revision()
	}

	return c.revision
}

func (c *L2) Clear() {
	c.cacheLock.Lock()
	c.cache = make(l2CachePerObjectKey, len(c.cache))
	c.revision = 0
	c.cacheLock.Unlock()
}

func (c *L2) aggregate(ctx context.Context, objectKey fmt.Stringer) (out statistics.Aggregated, err error) {
	var found bool

	cacheKey := objectKey.String()

	// Load stats from the fast L2 cache
	if c.enabled {
		c.cacheLock.RLock()
		out, found = c.cache[cacheKey]
		c.cacheLock.RUnlock()
	}

	// If not found, then calculate statistics from the slower L1 cache.
	if !found {
		var rev int64
		out, rev = c.l1Cache.aggregateWithRev(ctx, objectKey)

		// Cache the result
		if c.enabled {
			c.cacheLock.Lock()
			c.cache[cacheKey] = out
			if c.revision == 0 {
				// Store the first revision = the lowest revision.
				// The value is cleared on cache invalidation.
				c.revision = rev
			}
			c.cacheLock.Unlock()
		}
	}

	return out, nil
}
