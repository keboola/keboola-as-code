package statistics

import (
	"context"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

const SyncInterval = time.Second

// APINode collects node statistics in memory and periodically synchronizes them to the database.
type APINode struct {
	nodeID   string
	logger   log.Logger
	clock    clock.Clock
	store    *store.Store
	ch       chan notifyEvent
	perSlice map[key.SliceKey]*sliceStats
}

type notifyEvent struct {
	sliceKey   key.SliceKey
	recordSize uint64
	bodySize   uint64
	receivedAt key.ReceivedAt
}

type sliceStats struct {
	lastReceivedAt key.ReceivedAt
	recordsCount   uint64
	recordsSize    uint64
	bodySize       uint64
	changed        bool
}

type dependencies interface {
	Logger() log.Logger
	Clock() clock.Clock
	Process() *servicectx.Process
	Store() *store.Store
}

func NewAPINode(d dependencies) *APINode {
	m := &APINode{
		nodeID: d.Process().UniqueID(),
		logger: d.Logger().AddPrefix("[stats]"),
		clock:  d.Clock(),
		store:  d.Store(),
		// channel needs to be large enough to not block under average load
		ch:       make(chan notifyEvent, 2048),
		perSlice: make(map[key.SliceKey]*sliceStats),
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
			case event := <-m.ch:
				m.handleNotify(event)
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

func (m *APINode) Notify(sliceKey key.SliceKey, recordSize, bodySize uint64) {
	m.ch <- notifyEvent{
		sliceKey:   sliceKey,
		recordSize: recordSize,
		bodySize:   bodySize,
		receivedAt: key.ReceivedAt(m.clock.Now()),
	}
}

func (m *APINode) Sync(ctx context.Context) <-chan struct{} {
	stats := make([]model.SliceStats, 0, len(m.perSlice))
	for k, v := range m.perSlice {
		if v.changed {
			stats = append(stats, model.SliceStats{
				SliceKey: k,
				Stats: model.Stats{
					LastRecordAt: model.UTCTime(v.lastReceivedAt),
					RecordsCount: v.recordsCount,
					RecordsSize:  v.recordsSize,
					BodySize:     v.bodySize,
				},
			})
			v.changed = false
		}
	}

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

func (m *APINode) handleNotify(event notifyEvent) {
	// Init stats
	if _, exists := m.perSlice[event.sliceKey]; !exists {
		m.perSlice[event.sliceKey] = &sliceStats{}
	}

	// Update stats
	stats := m.perSlice[event.sliceKey]
	stats.recordsCount += 1
	stats.recordsSize += event.recordSize
	stats.bodySize += event.bodySize
	if event.receivedAt.After(stats.lastReceivedAt) {
		stats.lastReceivedAt = event.receivedAt
	}
	stats.changed = true
}
