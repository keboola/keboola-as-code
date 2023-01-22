// Package apinode provides lock free collection of statistics from the API import endpoint.
package apinode

import (
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

const SyncInterval = time.Second

// Node collects node statistics in memory and periodically synchronizes them to the database.
type Node struct {
	nodeID string
	logger log.Logger
	clock  clock.Clock
	store  *store.Store

	statsLock     *sync.Mutex
	statsPerSlice map[key.SliceKey]*sliceStats
}

type sliceStats struct {
	lastRecordAt model.UTCTime
	recordsCount uint64
	recordsSize  datasize.ByteSize
	bodySize     datasize.ByteSize
	changed      bool
}

type Dependencies interface {
	Logger() log.Logger
	Clock() clock.Clock
	Process() *servicectx.Process
	Store() *store.Store
}

func New(d Dependencies) *Node {
	m := &Node{
		nodeID:        d.Process().UniqueID(),
		logger:        d.Logger().AddPrefix("[stats]"),
		clock:         d.Clock(),
		store:         d.Store(),
		statsLock:     &sync.Mutex{},
		statsPerSlice: make(map[key.SliceKey]*sliceStats),
	}

	// Receive notifications and periodically trigger sync
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		ticker := m.clock.Ticker(SyncInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				<-m.Sync(context.Background())
				close(done)
				return
			case <-ticker.C:
				m.Sync(ctx)
			}
		}
	}()

	// The context is cancelled on shutdown, after the HTTP server.
	// OnShutdown applies LIFO order, the HTTP server is started last and terminated first.
	d.Process().OnShutdown(func() {
		m.logger.Info("received shutdown request")
		cancel()
		<-done
		m.logger.Info("shutdown done")
	})

	return m
}

func (m *Node) Notify(sliceKey key.SliceKey, recordSize, bodySize datasize.ByteSize) {
	m.statsLock.Lock()
	defer m.statsLock.Unlock()

	// Init stats
	if _, exists := m.statsPerSlice[sliceKey]; !exists {
		m.statsPerSlice[sliceKey] = &sliceStats{}
	}

	// Update stats
	receivedAt := model.UTCTime(m.clock.Now())
	stats := m.statsPerSlice[sliceKey]
	stats.recordsCount += 1
	stats.recordsSize += recordSize
	stats.bodySize += bodySize
	if receivedAt.After(stats.lastRecordAt) {
		stats.lastRecordAt = receivedAt
	}
	stats.changed = true
}

func (m *Node) Sync(ctx context.Context) <-chan struct{} {
	stats := m.statsForSync()
	done := make(chan struct{})
	if len(stats) > 0 {
		go func() {
			m.logger.Debugf("syncing %d records", len(stats))
			if err := m.store.UpdateSliceReceivedStats(ctx, m.nodeID, stats); err != nil {
				m.logger.Errorf("cannot update stats in etcd: %s", err.Error())
			}
			m.logger.Debug("sync done")
			close(done)
		}()
	} else {
		close(done)
	}

	return done
}

func (m *Node) statsForSync() (out []model.SliceStats) {
	m.statsLock.Lock()
	defer m.statsLock.Unlock()
	for k, v := range m.statsPerSlice {
		if v.changed {
			out = append(out, model.SliceStats{
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
