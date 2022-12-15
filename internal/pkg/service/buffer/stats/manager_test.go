package stats

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/stretchr/testify/assert"
)

func TestStatsManager(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clock := clock.NewMock()
	ticker := clock.Ticker(time.Second)

	store := newMockStatsStore()
	stats := Manager{
		ch: make(chan update),
		syncFn: func(_ context.Context, s []model.SliceStats) {
			store.append(s...)
		},
		clock:    clock,
		ticker:   ticker,
		perSlice: make(map[key.SliceStatsKey]*stats),
	}

	stats.Watch(ctx)

	clock.Add(time.Second)
	assert.Empty(t, store.read())

	receivedAt := clock.Now()
	k := key.NewSliceStatsKey(123, "my-receiver", "my-export", receivedAt, receivedAt, "my-node")
	stats.Notify(k, 1000)
	clock.Add(time.Second)
	assert.Equal(t,
		model.SliceStats{
			SliceStatsKey:  k,
			Count:          1,
			Size:           1000,
			LastReceivedAt: receivedAt,
		},
		store.read()[0],
	)

	receivedAt = clock.Now()
	stats.Notify(k, 2000)
	clock.Add(time.Second)
	assert.Equal(t,
		model.SliceStats{
			SliceStatsKey:  k,
			Count:          2,
			Size:           3000,
			LastReceivedAt: receivedAt,
		},
		store.read()[1],
	)
}

type mockStatsStore struct {
	v []model.SliceStats
	m *sync.Mutex
}

func newMockStatsStore() mockStatsStore {
	return mockStatsStore{
		v: make([]model.SliceStats, 0),
		m: &sync.Mutex{},
	}
}

func (m *mockStatsStore) read() []model.SliceStats {
	m.m.Lock()
	defer m.m.Unlock()

	out := make([]model.SliceStats, len(m.v))
	copy(out, m.v)
	return out
}

func (m *mockStatsStore) append(v ...model.SliceStats) {
	m.m.Lock()
	defer m.m.Unlock()

	m.v = append(m.v, v...)
}
