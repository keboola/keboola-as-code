package cache

import (
	"context"
	"fmt"
	"github.com/benbjohnson/clock"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/statistics/repository"
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

	cacheLock *sync.RWMutex
	cache     l2CachePerObjectKey
	revision  int64
}

type l2CachePerObjectKey map[string]statistics.Aggregated

func NewL2Cache(logger log.Logger, clk clock.Clock, l1Cache *L1, config statistics.L2CacheConfig) (*L2, error) {
	c := &L2{
		logger:    logger.AddPrefix("[stats-cache-L2]"),
		l1Cache:   l1Cache,
		wg:        &sync.WaitGroup{},
		cacheLock: &sync.RWMutex{},
		cache:     make(l2CachePerObjectKey),
	}

	// Setup context for graceful shutdown
	var ctx context.Context
	ctx, c.cancel = context.WithCancel(context.Background())

	// Periodically invalidates the cache.
	c.wg.Add(1)
	ticker := clk.Ticker(config.InvalidationInterval)
	go func() {
		defer c.wg.Done()
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.clearCache()
			}
		}
	}()

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

func (c *L2) clearCache() {
	c.cacheLock.Lock()
	c.cache = make(l2CachePerObjectKey, len(c.cache))
	c.revision = 0
	c.cacheLock.Unlock()
}

func (c *L2) aggregate(ctx context.Context, objectKey fmt.Stringer) (out statistics.Aggregated, err error) {
	// Load stats from the fast L2 cache
	k := objectKey.String()
	c.cacheLock.RLock()
	out, found := c.cache[k]
	c.cacheLock.RUnlock()

	// If not found, then calculate statistics from the slower L1 cache.
	if !found {
		var rev int64
		out, rev = c.l1Cache.aggregateWithRev(ctx, objectKey)

		c.cacheLock.Lock()
		c.cache[k] = out
		if c.revision == 0 {
			// Store the first revision = the lowest revision.
			// The value is cleared on cache invalidation.
			c.revision = rev
		}
		c.cacheLock.Unlock()
	}

	return out, nil
}
