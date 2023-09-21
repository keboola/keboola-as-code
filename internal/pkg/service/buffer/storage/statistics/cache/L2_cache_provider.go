package cache

import (
	"context"
	"fmt"
	"sync"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

type L2CacheProvider struct {
	*getters
	clock    clock.Clock
	logger   log.Logger
	cachedL1 *L1CacheProvider

	cacheLock *sync.RWMutex
	cache     l2CachePerObjectKey
}

type l2CachePerObjectKey map[string]Aggregated

type l2CachedProviderDeps interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	ServiceConfig() config.ServiceConfig
}

func NewL2CacheProvider(cachedL1 *L1CacheProvider, d l2CachedProviderDeps) (*L2CacheProvider, error) {
	p := &L2CacheProvider{
		clock:     d.Clock(),
		logger:    d.Logger().AddPrefix("[stats-cache-L2]"),
		cachedL1:  cachedL1,
		cacheLock: &sync.RWMutex{},
		cache:     make(l2CachePerObjectKey),
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background()) // nolint: contextcheck
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func(ctx context.Context) {
		p.logger.InfoCtx(ctx, "received shutdown request")
		cancel()
		wg.Wait()
		p.logger.InfoCtx(ctx, "shutdown done")
	})

	// Periodically invalidates the cache.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := p.clock.Ticker(d.ServiceConfig().StatisticsL2CacheTTL)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.ClearCache()
			}
		}
	}()

	// Setup common getters
	p.getters = newGetters(p.statsFromCache)

	return p, nil
}

func (p *L2CacheProvider) ClearCache() {
	p.cacheLock.Lock()
	p.cache = make(l2CachePerObjectKey, len(p.cache))
	p.cacheLock.Unlock()
}

func (p *L2CacheProvider) statsFromCache(ctx context.Context, objectKey fmt.Stringer) (out Aggregated, err error) {
	// Load stats from the fast cache
	k := objectKey.String()
	p.cacheLock.RLock()
	out, found := p.cache[k]
	p.cacheLock.RUnlock()

	// If not found, then calculate statistics from the slower cache.
	if !found {
		if out, err = p.cachedL1.statsFromCache(ctx, objectKey); err != nil {
			return out, err
		}
		p.cacheLock.Lock()
		p.cache[k] = out
		p.cacheLock.Unlock()
	}

	return out, nil
}
