package cache

import (
	"context"
	"fmt"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/prefixtree"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/aggregate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/repository"
)

type provider = repository.Provider

// L1 cache implements the repository.Provider interface.
//
// The L1 cache contains in-memory etcdop.Mirror of all statistics in the database.
//   - Updates are received via the etcd Watch API.
//   - The aggregated values are typically a few milliseconds out of date.
//   - A bit of CPU power is required for the calculation.
//   - The statistics.Value has small footprint in the memory, so 10,000 records will occupy several MB.
//
// The L1 cache method is primarily used to evaluate upload and import conditions every few seconds.
type L1 struct {
	provider
	logger     log.Logger
	repository *repository.Repository

	cancel context.CancelFunc
	wg     *sync.WaitGroup

	cache *etcdop.Mirror[statistics.Value, statistics.Value]
}

func NewL1Cache(logger log.Logger, r *repository.Repository) (*L1, error) {
	c := &L1{
		logger:     logger.WithComponent("stats.cache.L1"),
		repository: r,
		wg:         &sync.WaitGroup{},
	}

	// Setup context for graceful shutdown
	var ctx context.Context
	ctx, c.cancel = context.WithCancel(context.Background())

	// Mirror statistics from the database to the cache via etcd watcher
	stream := c.repository.GetAllAndWatch(ctx)
	mapKey := func(kv *op.KeyValue, _ statistics.Value) string { return string(kv.Key) }
	mapValue := func(_ *op.KeyValue, stats statistics.Value) statistics.Value { return stats }
	mirror, errCh := etcdop.SetupMirror(c.logger, stream, mapKey, mapValue).StartMirroring(ctx, c.wg)
	if err := <-errCh; err == nil {
		c.cache = mirror
	} else {
		return nil, err
	}

	// Setup Provider interface
	c.provider = repository.NewProvider(c.aggregate)

	return c, nil
}

func (c *L1) Stop() {
	c.cancel()
	c.wg.Wait()
}

func (c *L1) Revision() int64 {
	return c.cache.Revision()
}

func (c *L1) aggregate(ctx context.Context, objectKey fmt.Stringer) (statistics.Aggregated, error) {
	out, _ := c.aggregateWithRev(ctx, objectKey)
	return out, nil
}

func (c *L1) aggregateWithRev(_ context.Context, objectKey fmt.Stringer) (out statistics.Aggregated, rev int64) {
	c.cache.Atomic(func(t prefixtree.TreeReadOnly[statistics.Value]) {
		for _, level := range model.AllLevels() {
			t.WalkPrefix(
				c.repository.ObjectPrefix(level, objectKey),
				func(_ string, v statistics.Value) bool {
					aggregate.Aggregate(level, v, &out)
					return false
				},
			)
		}
		rev = c.cache.Revision()
	})
	return out, rev
}
