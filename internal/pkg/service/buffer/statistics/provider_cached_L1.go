package statistics

import (
	"context"
	"fmt"
	"sync"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/prefixtree"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

type CachedL1Provider struct {
	*getters
	logger log.Logger
	client *etcd.Client
	schema schema.Stats
	cache  *etcdop.Mirror[model.Stats, model.Stats]
}

type cachedL1ProviderDeps interface {
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	Schema() *schema.Schema
}

func NewCachedL1Provider(d cachedL1ProviderDeps) (*CachedL1Provider, error) {
	p := &CachedL1Provider{
		logger: d.Logger().AddPrefix("[stats-cache-L1]"),
		client: d.EtcdClient(),
		schema: d.Schema().SliceStats(),
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

	// Start watcher to sync cache
	if err := <-p.setupCache(ctx, wg); err != nil {
		return nil, err
	}

	// Setup common getters
	p.getters = newGetters(p.statsFromCache)

	return p, nil
}

func (p *CachedL1Provider) Revision() int64 {
	return p.cache.Revision()
}

func (p *CachedL1Provider) setupCache(ctx context.Context, wg *sync.WaitGroup) <-chan error {
	stream := p.schema.GetAllAndWatch(ctx, p.client)
	mapKey := func(kv *op.KeyValue, _ model.Stats) string { return string(kv.Key) }
	mapValue := func(_ *op.KeyValue, stats model.Stats) model.Stats { return stats }
	mirror, errCh := etcdop.SetupMirror(p.logger, stream, mapKey, mapValue).StartMirroring(wg)
	p.cache = mirror
	return errCh
}

func (p *CachedL1Provider) statsFromCache(_ context.Context, objectKey fmt.Stringer) (out model.StatsByType, err error) {
	p.cache.Atomic(func(t prefixtree.TreeReadOnly[model.Stats]) {
		for _, state := range allStates {
			t.WalkPrefix(
				p.schema.InState(state).InObject(objectKey).Prefix(),
				func(_ string, v model.Stats) bool {
					aggregate(state, v, &out)
					return false
				},
			)
		}
	})
	return out, nil
}
