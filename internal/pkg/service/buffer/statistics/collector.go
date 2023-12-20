package statistics

import (
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const (
	collectorMaxStatsPerTxn = 50
)

// Collector collects node statistics in memory and periodically synchronizes them to the database.
type Collector struct {
	nodeID     string
	clock      clock.Clock
	logger     log.Logger
	telemetry  telemetry.Telemetry
	client     *etcd.Client
	schema     schemaRoot
	repository *Repository

	statsLock     *sync.Mutex
	statsPerSlice map[key.SliceKey]*sliceStats
}

type sliceStats struct {
	Value
	changed bool
}

func (s *sliceStats) Add(value Value) {
	s.Value = s.Value.Add(value)
	s.changed = true
}

type collectorDeps interface {
	Clock() clock.Clock
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	APIConfig() config.APIConfig
	StatisticsRepository() *Repository
}

func NewCollector(d collectorDeps) *Collector {
	c := &Collector{
		nodeID:        d.Process().UniqueID(),
		clock:         d.Clock(),
		logger:        d.Logger().AddPrefix("[stats-collector]"),
		telemetry:     d.Telemetry(),
		client:        d.EtcdClient(),
		schema:        newSchema(d.EtcdSerde()),
		repository:    d.StatisticsRepository(),
		statsLock:     &sync.Mutex{},
		statsPerSlice: make(map[key.SliceKey]*sliceStats),
	}

	// Graceful shutdown
	// The context is cancelled on shutdown, after the HTTP server.
	// OnShutdown applies LIFO order, the HTTP server is started last and terminated first.
	ctx, cancel := context.WithCancel(context.Background()) // nolint: contextcheck
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func(ctx context.Context) {
		c.logger.InfoCtx(ctx, "received shutdown request")
		cancel()
		wg.Wait()
		c.logger.InfoCtx(ctx, "shutdown done")
	})

	// Receive notifications and periodically trigger sync
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := c.clock.Ticker(d.APIConfig().StatisticsSyncInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				<-c.Sync(context.Background()) // nolint: contextcheck
				return
			case <-ticker.C:
				c.Sync(ctx)
			}
		}
	}()

	return c
}

func (c *Collector) Notify(receivedAt time.Time, sliceKey key.SliceKey, recordSize, bodySize datasize.ByteSize) {
	c.statsLock.Lock()
	defer c.statsLock.Unlock()

	// Init stats
	value, exists := c.statsPerSlice[sliceKey]
	if !exists {
		value = &sliceStats{}
		c.statsPerSlice[sliceKey] = value
	}

	// Update stats
	receivedAtUTC := utctime.UTCTime(receivedAt)
	value.Add(Value{
		RecordsCount:  1,
		RecordsSize:   recordSize,
		BodySize:      bodySize,
		FirstRecordAt: receivedAtUTC,
		LastRecordAt:  receivedAtUTC,
	})
}

func (c *Collector) Sync(ctx context.Context) <-chan error {
	stats := c.statsForSync()
	errCh := make(chan error, 1)
	if len(stats) > 0 {
		go func() {
			defer close(errCh)
			c.logger.DebugfCtx(ctx, "syncing %d records", len(stats))
			if err := c.repository.Insert(ctx, c.nodeID, stats); err == nil {
				c.logger.DebugCtx(ctx, "sync done")
			} else {
				c.logger.ErrorfCtx(ctx, "cannot update stats in etcd: %s", err.Error())
				errCh <- err
			}
		}()
	} else {
		close(errCh)
	}

	return errCh
}

func (c *Collector) statsForSync() (out []PerAPINode) {
	c.statsLock.Lock()
	defer c.statsLock.Unlock()
	for k, v := range c.statsPerSlice {
		if v.changed {
			out = append(out, PerAPINode{SliceKey: k, Value: v.Value})
			v.changed = false
		}
	}
	return out
}
