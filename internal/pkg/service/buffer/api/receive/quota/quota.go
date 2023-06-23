// Package quota provides limitation of a receiver buffered records size in etcd.
// This will prevent etcd from filling up if data cannot be sent to file storage fast enough
// or if it is not possible to upload data at all.
package quota

import (
	"context"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Quota struct {
	clock  clock.Clock
	config config.APIConfig
	stats  *statistics.CacheNode

	// cache computed stats, to improve import throughput
	cache     bufferedSizeMap
	cacheLock *sync.RWMutex
}
type bufferedSizeMap = map[key.ReceiverKey]datasize.ByteSize

type dependencies interface {
	APIConfig() config.APIConfig
	Clock() clock.Clock
	StatsCache() *statistics.CacheNode
}

func New(ctx context.Context, wg *sync.WaitGroup, d dependencies) *Quota {
	q := &Quota{
		clock:     d.Clock(),
		config:    d.APIConfig(),
		stats:     d.StatsCache(),
		cacheLock: &sync.RWMutex{},
		cache:     make(bufferedSizeMap),
	}

	// Periodically invalidates the cache.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := q.clock.Ticker(q.config.ReceiverBufferSizeCacheTTL)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				q.cacheLock.Lock()
				q.cache = make(bufferedSizeMap, len(q.cache))
				q.cacheLock.Unlock()
			}
		}
	}()

	return q
}

// Check checks whether the size of records that one receiver can buffer in etcd has not been exceeded.
func (i *Quota) Check(k key.ReceiverKey) error {
	// Load bufferedBytes from the fast cache.
	i.cacheLock.RLock()
	buffered, found := i.cache[k]
	i.cacheLock.RUnlock()

	// If not found, then calculate statistics from the slower cache.
	if !found {
		buffered = i.stats.ReceiverStats(k).Buffered.RecordsSize
		i.cacheLock.Lock()
		i.cache[k] = buffered
		i.cacheLock.Unlock()
	}

	if limit := i.config.ReceiverBufferSize; buffered > limit {
		return errors.Errorf(
			`no free space in the buffer: receiver "%s" has "%s" buffered for upload, limit is "%s"`,
			k.ReceiverID, buffered.HumanReadable(), limit.HumanReadable(),
		)
	}

	return nil
}
