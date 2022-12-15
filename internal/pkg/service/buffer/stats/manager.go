package stats

import (
	"context"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
)

type Manager struct {
	ch       chan update
	store    *store.Store
	clock    clock.Clock
	perSlice map[key.SliceStatsKey]*stats
}

type update struct {
	key            key.SliceStatsKey
	count          uint64
	size           uint64
	lastReceivedAt time.Time
}

type stats struct {
	count          uint64
	size           uint64
	lastReceivedAt time.Time
	changed        bool
}

func New(store *store.Store, clock clock.Clock) Manager {
	return Manager{
		// channel needs to be large enough to not block under average load
		ch:       make(chan update, 2048),
		store:    store,
		clock:    clock,
		perSlice: make(map[key.SliceStatsKey]*stats),
	}
}

func (m *Manager) Notify(key key.SliceStatsKey, count uint64, size uint64) {
	m.ch <- update{
		key:            key,
		count:          count,
		size:           size,
		lastReceivedAt: time.Now(),
	}
}

func (m *Manager) Watch(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case update := <-m.ch:
				m.handleUpdate(update)
			case <-m.clock.After(time.Second):
				m.syncStats(ctx)
			}
		}
	}()
}

func (m *Manager) handleUpdate(update update) {
	// Init stats
	if _, exists := m.perSlice[update.key]; !exists {
		m.perSlice[update.key] = &stats{}
	}

	// Update stats
	stats := m.perSlice[update.key]
	stats.count += update.count
	stats.size += update.size
	if update.lastReceivedAt.After(stats.lastReceivedAt) {
		stats.lastReceivedAt = update.lastReceivedAt
	}
	stats.changed = true
}

func (m *Manager) syncStats(ctx context.Context) {
	stats := make([]model.SliceStats, 0, len(m.perSlice))
	for k, v := range m.perSlice {
		if v.changed {
			stats = append(stats, model.NewSliceStats(k, v.count, v.size, v.lastReceivedAt))
		}
	}
	_ = m.store.UpdateSliceStats(ctx, stats)
}
