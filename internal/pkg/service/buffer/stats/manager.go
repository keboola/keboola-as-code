package stats

import (
	"context"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
)

type Manager struct {
	logger   log.Logger
	ch       chan update
	syncFn   syncFn
	clock    clock.Clock
	ticker   *clock.Ticker
	perSlice map[key.SliceStatsKey]*stats
}

type syncFn func(context.Context, []model.SliceStats)

type update struct {
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

func New(store *store.Store, logger log.Logger) Manager {
	logger = logger.AddPrefix("[stats]")
	clock := clock.New()
	return Manager{
		logger: logger,
		// channel needs to be large enough to not block under average load
		ch:       make(chan update, 2048),
		syncFn:   syncToStore(store, logger),
		clock:    clock,
		ticker:   clock.Ticker(time.Second),
		perSlice: make(map[key.SliceStatsKey]*stats),
	}
}

func (m *Manager) Notify(key key.SliceStatsKey, size uint64) {
	m.ch <- update{
		key:            key,
		size:           size,
		lastReceivedAt: m.clock.Now(),
	}
}

func (m *Manager) Watch(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.ticker.Stop()
				return
			case update := <-m.ch:
				m.handleUpdate(update)
			case <-m.ticker.C:
				m.handleSync(ctx)
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

func syncToStore(store *store.Store, logger log.Logger) syncFn {
	return func(ctx context.Context, stats []model.SliceStats) {
		if err := store.UpdateSliceStats(ctx, stats); err != nil {
			logger.Error("cannot update slice stats in etcd: %s", err.Error())
		}
	}
}
