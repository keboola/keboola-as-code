package statistics

import (
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

// Collector collects node statistics in memory and periodically synchronizes them to the database.
type Collector struct {
	nodeID string
	config config.APIConfig
	logger log.Logger
	clock  clock.Clock
	store  *store.Store

	statsLock     *sync.Mutex
	statsPerSlice map[key.SliceKey]*sliceStats
}

type sliceStats struct {
	lastRecordAt utctime.UTCTime
	recordsCount uint64
	recordsSize  datasize.ByteSize
	bodySize     datasize.ByteSize
	changed      bool
}

type collectorDeps interface {
	APIConfig() config.APIConfig
	Logger() log.Logger
	Clock() clock.Clock
	Process() *servicectx.Process
	Store() *store.Store
}

func NewCollector(d collectorDeps) *Collector {
	c := &Collector{
		nodeID:        d.Process().UniqueID(),
		config:        d.APIConfig(),
		logger:        d.Logger().AddPrefix("[stats-collector]"),
		clock:         d.Clock(),
		store:         d.Store(),
		statsLock:     &sync.Mutex{},
		statsPerSlice: make(map[key.SliceKey]*sliceStats),
	}

	// The context is cancelled on shutdown, after the HTTP server.
	// OnShutdown applies LIFO order, the HTTP server is started last and terminated first.
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func() {
		c.logger.Info("received shutdown request")
		cancel()
		wg.Wait()
		c.logger.Info("shutdown done")
	})

	// Receive notifications and periodically trigger sync
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := c.clock.Ticker(c.config.StatisticsSyncInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				<-c.Sync(context.Background())
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
	if _, exists := c.statsPerSlice[sliceKey]; !exists {
		c.statsPerSlice[sliceKey] = &sliceStats{}
	}

	// Update stats
	stats := c.statsPerSlice[sliceKey]
	stats.recordsCount += 1
	stats.recordsSize += recordSize
	stats.bodySize += bodySize
	if receivedAtUTC := utctime.UTCTime(receivedAt); receivedAtUTC.After(stats.lastRecordAt) {
		stats.lastRecordAt = receivedAtUTC
	}
	stats.changed = true
}

func (c *Collector) Sync(ctx context.Context) <-chan struct{} {
	stats := c.statsForSync()
	done := make(chan struct{})
	if len(stats) > 0 {
		go func() {
			defer close(done)
			c.logger.Debugf("syncing %d records", len(stats))
			if err := c.store.UpdateSliceReceivedStats(ctx, c.nodeID, stats); err != nil {
				c.logger.Errorf("cannot update stats in etcd: %s", err.Error())
			}
			c.logger.Debug("sync done")
		}()
	} else {
		close(done)
	}

	return done
}

func (c *Collector) statsForSync() (out []model.SliceAPINodeStats) {
	c.statsLock.Lock()
	defer c.statsLock.Unlock()
	for k, v := range c.statsPerSlice {
		if v.changed {
			out = append(out, model.SliceAPINodeStats{
				NodeID:   c.nodeID,
				SliceKey: k,
				Stats: model.Stats{
					LastRecordAt: v.lastRecordAt,
					RecordsCount: v.recordsCount,
					RecordsSize:  v.recordsSize,
					BodySize:     v.bodySize,
				},
			})
			v.changed = false
		}
	}
	return out
}
