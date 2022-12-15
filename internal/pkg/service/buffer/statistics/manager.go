package statistics

import (
	"context"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
)

type Manager struct {
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
	Clock() clock.Clock
}

func New(ctx context.Context, d dependencies, fn syncFn) Manager {
	m := Manager{
		clock: d.Clock(),
		// channel needs to be large enough to not block under average load
		ch:       make(chan notifyEvent, 2048),
		syncFn:   fn,
		perSlice: make(map[key.SliceStatsKey]*stats),
	}

	go func() {
		ticker := m.clock.Ticker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case update := <-m.ch:
				m.handleUpdate(update)
			case <-ticker.C:
				m.handleSync(ctx)
			}
		}
	}()

	return m
}

func (m *Manager) Notify(key key.SliceStatsKey, size uint64) {
	m.ch <- notifyEvent{
		key:            key,
		size:           size,
		lastReceivedAt: m.clock.Now(),
	}
}

func (m *Manager) handleUpdate(update notifyEvent) {
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

func (m *Manager) handleSync(ctx context.Context) {
	stats := make([]model.SliceStats, 0, len(m.perSlice))
	for k, v := range m.perSlice {
		if v.changed {
			stats = append(stats, model.NewSliceStats(k, v.count, v.size, v.lastReceivedAt))
			v.changed = false
		}
	}

	go m.syncFn(ctx, stats)
}
