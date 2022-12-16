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
	key        key.SliceKey
	size       uint64
	receivedAt time.Time
}

type sliceStats struct {
	count          uint64
	size           uint64
	lastReceivedAt time.Time
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
				m.logger.Info("the server is shutting down, starting sync")
				<-m.handleSync(context.Background())
				m.logger.Info("all done")
				close(done)
				return
			case event := <-m.ch:
				m.handleNotify(event)
			case <-ticker.C:
				m.handleSync(ctx)
			}
		}
	}()

	// The context is cancelled on shutdown, after the HTTP server.
	// OnShutdown applies LIFO order, the HTTP server is started last and terminated first.
	d.Process().OnShutdown(func() {
		cancel()
		<-done
	})

	return m
}

func (m *APINode) Notify(key key.SliceKey, size uint64) {
	m.ch <- notifyEvent{
		key:        key,
		size:       size,
		receivedAt: m.clock.Now(),
	}
}

func (m *APINode) handleNotify(event notifyEvent) {
	// Init stats
	if _, exists := m.perSlice[event.key]; !exists {
		m.perSlice[event.key] = &sliceStats{}
	}

	// Update stats
	stats := m.perSlice[event.key]
	stats.count += 1
	stats.size += event.size
	if event.receivedAt.After(stats.lastReceivedAt) {
		stats.lastReceivedAt = event.receivedAt
	}
	stats.changed = true
}

func (m *APINode) handleSync(ctx context.Context) <-chan struct{} {
	stats := make([]model.SliceStats, 0, len(m.perSlice))
	for k, v := range m.perSlice {
		if v.changed {
			stats = append(stats, model.NewSliceStats(k, v.count, v.size, v.lastReceivedAt))
			v.changed = false
		}
	}

	done := make(chan struct{})
	if len(stats) > 0 {
		go func() {
			m.logger.Debugf("syncing %d records", len(stats))
			if err := m.store.UpdateSliceStats(ctx, m.nodeID, stats); err != nil {
				m.logger.Error("cannot update stats in etcd: %s", err.Error())
			}
			m.logger.Debug("sync done")
			close(done)
		}()
	} else {
		close(done)
	}

	return done
}
