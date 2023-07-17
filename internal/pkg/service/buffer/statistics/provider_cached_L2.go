package statistics

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

type CachedL2Provider struct {
	*getters
	clock    clock.Clock
	logger   log.Logger
	cachedL1 *CachedL1Provider

	cacheLock *sync.RWMutex
	cache     cachePerObjectKey
}

type cachePerObjectKey map[string]model.StatsByType

type cachedL2ProviderDeps interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
}

func NewCachedL2Provider(cachedL1 *CachedL1Provider, ttl time.Duration, d cachedL2ProviderDeps) (*CachedL2Provider, error) {
	p := &CachedL2Provider{
		clock:     d.Clock(),
		logger:    d.Logger().AddPrefix("[stats-cache-L2]"),
		cachedL1:  cachedL1,
		cacheLock: &sync.RWMutex{},
		cache:     make(cachePerObjectKey),
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func() {
		p.logger.Info("received shutdown request")
		cancel()
		wg.Wait()
		p.logger.Info("shutdown done")
	})

	// Periodically invalidates the cache.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := p.clock.Ticker(ttl)
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

func (p *CachedL2Provider) ClearCache() {
	p.cacheLock.Lock()
	p.cache = make(cachePerObjectKey, len(p.cache))
	p.cacheLock.Unlock()
}

func (p *CachedL2Provider) statsFromCache(ctx context.Context, objectKey fmt.Stringer) (out model.StatsByType, err error) {
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
