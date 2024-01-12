package statistics

import (
	"context"
	"fmt"
	"sync"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/prefixtree"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

type L1CacheProvider struct {
	*getters
	logger log.Logger
	client *etcd.Client
	schema schemaRoot
	cache  *etcdop.Mirror[Value, Value]
}

type l1CachedProviderDeps interface {
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
}

func NewL1CacheProvider(d l1CachedProviderDeps) (*L1CacheProvider, error) {
	p := &L1CacheProvider{
		logger: d.Logger().WithComponent("stats-cache-L1"),
		client: d.EtcdClient(),
		schema: newSchema(d.EtcdSerde()),
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

	// Start watcher to sync cache
	if err := <-p.setupCache(ctx, wg); err != nil {
		return nil, err
	}

	// Setup common getters
	p.getters = newGetters(p.statsFromCache)

	return p, nil
}

func (p *L1CacheProvider) Revision() int64 {
	return p.cache.Revision()
}

func (p *L1CacheProvider) setupCache(ctx context.Context, wg *sync.WaitGroup) <-chan error {
	stream := p.schema.GetAllAndWatch(ctx, p.client)
	mapKey := func(kv *op.KeyValue, _ Value) string { return string(kv.Key) }
	mapValue := func(_ *op.KeyValue, stats Value) Value { return stats }
	mirror, errCh := etcdop.SetupMirror(p.logger, stream, mapKey, mapValue).StartMirroring(ctx, wg)
	p.cache = mirror
	return errCh
}

func (p *L1CacheProvider) statsFromCache(_ context.Context, objectKey fmt.Stringer) (out Aggregated, err error) {
	p.cache.Atomic(func(t prefixtree.TreeReadOnly[Value]) {
		for _, category := range allCategories {
			t.WalkPrefix(
				p.schema.InCategory(category).InObject(objectKey).Prefix(),
				func(_ string, v Value) bool {
					aggregate(category, v, &out)
					return false
				},
			)
		}
	})
	return out, nil
}
