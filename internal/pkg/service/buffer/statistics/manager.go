package statistics

import (
	"context"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

type Manager struct {
	logger   log.Logger
	clock    clock.Clock
	ch       chan notifyEvent
	syncFn   syncFn
	perSlice map[key.SliceStatsKey]*stats
}

type syncFn func(context.Context, []model.SliceStats)

type notifyEvent struct {
	key            key.SliceStatsKey
	size           uint64
	lastReceivedAt time.Time
}

type stats struct {
	count          uint64
	size           uint64
	lastReceivedAt time.Time
	changed        bool
}

type dependencies interface {
	Logger() log.Logger
	Clock() clock.Clock
	Process() *servicectx.Process
}

func New(d dependencies, fn syncFn) Manager {
	m := Manager{
		logger: d.Logger().AddPrefix("[stats]"),
		clock:  d.Clock(),
		// channel needs to be large enough to not block under average load
		ch:       make(chan notifyEvent, 2048),
		syncFn:   fn,
		perSlice: make(map[key.SliceStatsKey]*stats),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		ticker := m.clock.Ticker(time.Second)
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

func (m *Manager) Notify(key key.SliceStatsKey, size uint64) {
	m.ch <- notifyEvent{
		key:            key,
		size:           size,
		lastReceivedAt: m.clock.Now(),
	}
}

func (m *Manager) handleNotify(update notifyEvent) {
	// Init stats
	if _, exists := m.perSlice[update.key]; !exists {
		m.perSlice[update.key] = &stats{}
	}

	// Update stats
	stats := m.perSlice[update.key]
	stats.count += 1
	stats.size += update.size
	if update.lastReceivedAt.After(stats.lastReceivedAt) {
		stats.lastReceivedAt = update.lastReceivedAt
	}
	stats.changed = true
}

func (m *Manager) handleSync(ctx context.Context) <-chan struct{} {
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
			m.syncFn(ctx, stats)
			m.logger.Debug("sync done")
			close(done)
		}()
	} else {
		close(done)
	}

	return done
}
