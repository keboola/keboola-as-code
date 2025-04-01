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
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
	cache      *etcdop.MirrorTree[statistics.Value, statistics.Value]
	cacheMap   *etcdop.MirrorMap[statistics.Value, string, statistics.Value]
}

func NewL1Cache(d dependencies) (*L1, error) {
	c := &L1{
		logger:     d.Logger().WithComponent("stats.cache.L1"),
		repository: d.StatisticsRepository(),
	}

	// Graceful shutdown
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancelCause(context.Background())
	d.Process().OnShutdown(func(ctx context.Context) {
		cancel(errors.New("shutting down: L1 statistics cache"))
		c.logger.Info(ctx, "stopping L1 statistics cache")
		wg.Wait()
		c.logger.Info(ctx, "stopped L1 statistics cache")
	})

	// Mirror statistics from the database to the cache via etcd watcher
	stream := c.repository.GetAllAndWatch(ctx)
	mapKey := func(key string, stats statistics.Value) string { return key }
	mapValue := func(key string, stats statistics.Value, rawValue *op.KeyValue, oldValue *statistics.Value) statistics.Value {
		return stats
	}
	mirror := etcdop.SetupMirrorTree[statistics.Value](stream, mapKey, mapValue).BuildMirror()
	if err := <-mirror.StartMirroring(ctx, wg, c.logger, d.Telemetry(), d.WatchTelemetryInterval()); err == nil {
		c.cache = mirror
	} else {
		return nil, err
	}

	streamMap := c.repository.GetAllAndWatch(ctx)
	cacheMap := etcdop.SetupMirrorMap[statistics.Value, string, statistics.Value](streamMap, mapKey, mapValue).BuildMirror()
	if err := <-cacheMap.StartMirroring(ctx, wg, c.logger, d.Telemetry(), d.WatchTelemetryInterval()); err == nil {
		c.cacheMap = cacheMap
	} else {
		return nil, err
	}

	// Setup Provider interface
	c.provider = repository.NewProvider(c.aggregate)

	return c, nil
}

func (c *L1) Revision() int64 {
	return c.cache.Revision()
}

func (c *L1) WaitForRevision(ctx context.Context, expected int64) error {
	return c.cache.WaitForRevision(ctx, expected)
}

func (c *L1) WaitForRevisionMap(ctx context.Context, expected int64) error {
	return c.cacheMap.WaitForRevision(ctx, expected)
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
