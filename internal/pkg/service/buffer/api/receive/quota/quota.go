// Package quota provides limitation of a receiver buffered records size in etcd.
// This will prevent etcd from filling up if data cannot be sent to file storage fast enough
// or if it is not possible to upload data at all.
package quota

import (
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	commonErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// MinErrorLogInterval defines the minimum interval between logged quota errors, per receiver and API node.
	// It prevents repeating errors from flooding the log.
	MinErrorLogInterval = 5 * time.Minute
)

type Checker struct {
	clock  clock.Clock
	config config.APIConfig
	stats  *statistics.CacheNode

	// nextLogAt prevents errors from flooding the log
	nextLogAtLock *sync.RWMutex
	nextLogAt     map[key.ReceiverKey]time.Time

	// cache computed stats, to improve import throughput
	cacheLock *sync.RWMutex
	cache     bufferedSizeMap
}
type bufferedSizeMap = map[key.ReceiverKey]datasize.ByteSize

type dependencies interface {
	APIConfig() config.APIConfig
	Clock() clock.Clock
	StatsCache() *statistics.CacheNode
}

func New(ctx context.Context, wg *sync.WaitGroup, d dependencies) *Checker {
	c := &Checker{
		clock:         d.Clock(),
		config:        d.APIConfig(),
		stats:         d.StatsCache(),
		nextLogAtLock: &sync.RWMutex{},
		nextLogAt:     make(map[key.ReceiverKey]time.Time),
		cacheLock:     &sync.RWMutex{},
		cache:         make(bufferedSizeMap),
	}

	// Periodically invalidates the cache.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := c.clock.Ticker(c.config.ReceiverBufferSizeCacheTTL)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.ClearCache()
			}
		}
	}()

	return c
}

// Check checks whether the size of records that one receiver can buffer in etcd has not been exceeded.
func (c *Checker) Check(k key.ReceiverKey) error {
	// Load bufferedBytes from the fast cache.
	c.cacheLock.RLock()
	buffered, found := c.cache[k]
	c.cacheLock.RUnlock()

	// If not found, then calculate statistics from the slower cache.
	if !found {
		buffered = c.stats.ReceiverStats(k).Buffered.RecordsSize
		c.cacheLock.Lock()
		c.cache[k] = buffered
		c.cacheLock.Unlock()
	}

	if limit := c.config.ReceiverBufferSize; buffered > limit {
		return commonErrors.NewInsufficientStorageError(c.shouldLogError(k), errors.Errorf(
			`no free space in the buffer: receiver "%s" has "%s" buffered for upload, limit is "%s"`,
			k.ReceiverID, buffered.HumanReadable(), limit.HumanReadable(),
		))
	}

	return nil
}

func (c *Checker) ClearCache() {
	c.cacheLock.Lock()
	c.cache = make(bufferedSizeMap, len(c.cache))
	c.cacheLock.Unlock()
}

// shouldLogError method determines if the quota error should be logged.
func (c *Checker) shouldLogError(k key.ReceiverKey) bool {
	now := c.clock.Now()

	c.nextLogAtLock.RLock()
	logTime := c.nextLogAt[k] // first time it returns zero time
	c.nextLogAtLock.RUnlock()

	if logTime.Before(now) {
		c.nextLogAtLock.Lock()
		c.nextLogAt[k] = now.Add(MinErrorLogInterval)
		c.nextLogAtLock.Unlock()
		return true
	}
	return false
}
