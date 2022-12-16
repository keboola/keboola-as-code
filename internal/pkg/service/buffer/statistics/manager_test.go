package statistics_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestStatsManager(t *testing.T) {
	t.Parallel()

	clk := clock.NewMock()
	d := dependencies.NewMockedDeps(t, dependencies.WithClock(clk))

	// mock store which contains every version of `SliceStats`
	store := newMockStatsStore()
	stats := New(d, func(_ context.Context, s []model.SliceStats) {
		store.append(s...)
	})

	// no notify -> wait 1 second -> no sync
	clk.Add(time.Second)
	assert.Empty(t, store.read())

	// notify -> wait 1 second -> sync
	receivedAt0 := clk.Now()
	k := key.NewSliceStatsKey(123, "my-receiver", "my-export", receivedAt0, receivedAt0, "my-node")
	stats.Notify(k, 1000)
	clk.Add(time.Second)
	assert.Equal(t,
		[]model.SliceStats{
			{
				SliceStatsKey:  k,
				Count:          1,
				Size:           1000,
				LastReceivedAt: receivedAt0,
			},
		},
		store.read(),
	)

	// no notify -> wait 1 second -> no sync
	clk.Add(time.Second)
	assert.Equal(t,
		[]model.SliceStats{
			{
				SliceStatsKey:  k,
				Count:          1,
				Size:           1000,
				LastReceivedAt: receivedAt0,
			},
		},
		store.read(),
	)

	// notify -> wait 1 second -> sync
	receivedAt1 := clk.Now()
	stats.Notify(k, 2000)
	clk.Add(time.Second)
	assert.Equal(t,
		[]model.SliceStats{
			{
				SliceStatsKey:  k,
				Count:          1,
				Size:           1000,
				LastReceivedAt: receivedAt0,
			},
			{
				SliceStatsKey:  k,
				Count:          2,
				Size:           3000,
				LastReceivedAt: receivedAt1,
			},
		},
		store.read(),
	)

	// no notify -> wait 1 second -> no sync
	clk.Add(time.Second)
	assert.Equal(t,
		[]model.SliceStats{
			{
				SliceStatsKey:  k,
				Count:          1,
				Size:           1000,
				LastReceivedAt: receivedAt0,
			},
			{
				SliceStatsKey:  k,
				Count:          2,
				Size:           3000,
				LastReceivedAt: receivedAt1,
			},
		},
		store.read(),
	)

	// notify before shutdown
	clk.Add(100 * time.Millisecond)
	receivedAt2 := clk.Now()
	stats.Notify(k, 3000)

	d.Process().Shutdown(errors.New("test shutdown"))
	d.Process().WaitForShutdown()

	// shutdown triggered sync
	assert.Equal(t,
		[]model.SliceStats{
			{
				SliceStatsKey:  k,
				Count:          1,
				Size:           1000,
				LastReceivedAt: receivedAt0,
			},
			{
				SliceStatsKey:  k,
				Count:          2,
				Size:           3000,
				LastReceivedAt: receivedAt1,
			},
			{
				SliceStatsKey:  k,
				Count:          3,
				Size:           6000,
				LastReceivedAt: receivedAt2,
			},
		},
		store.read(),
	)

	// check logs
	expected := `
INFO  process unique id "%s"
[stats]DEBUG  syncing 1 records
[stats]DEBUG  sync done
[stats]DEBUG  syncing 1 records
[stats]DEBUG  sync done
INFO  exiting (test shutdown)
[stats]INFO  the server is shutting down, starting sync
[stats]DEBUG  syncing 1 records
[stats]DEBUG  sync done
[stats]INFO  all done
INFO  exited
`
	wildcards.Assert(t, strings.TrimSpace(expected), d.DebugLogger().AllMessages())
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
