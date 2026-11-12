package sessionstore

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jonboulle/clockwork"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type metricsHarness struct {
	mr      *miniredis.Miniredis
	client  *redis.Client
	clock   *clockwork.FakeClock
	tel     telemetry.ForTest
	checker *Checker
	metrics *Metrics
}

func newMetricsHarness(t *testing.T) *metricsHarness {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1, DialTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = client.Close() })
	clock := clockwork.NewFakeClock()
	tel := telemetry.NewForTest(t)
	checker := NewChecker(log.NewNopLogger(), clock, client)
	return &metricsHarness{mr: mr, client: client, clock: clock, tel: tel, checker: checker, metrics: NewMetrics(tel.Meter(), client, checker)}
}

func (h *metricsHarness) find(t *testing.T, name string) (metricdata.Metrics, bool) {
	t.Helper()
	for _, m := range h.tel.Metrics(t) {
		if m.Name == name {
			return m, true
		}
	}
	return metricdata.Metrics{}, false
}

func attrs(kv ...string) attribute.Set {
	var list []attribute.KeyValue
	for i := 0; i+1 < len(kv); i += 2 {
		list = append(list, attribute.String(kv[i], kv[i+1]))
	}
	return attribute.NewSet(list...)
}

func (h *metricsHarness) durationCount(t *testing.T, command, result string) uint64 {
	t.Helper()
	m, ok := h.find(t, "keboola.go.appsproxy.sessionstore.duration")
	if !ok {
		return 0
	}
	hist, ok := m.Data.(metricdata.Histogram[float64])
	require.True(t, ok, "%T", m.Data)
	want := attrs("command", command, "result", result)
	for _, dp := range hist.DataPoints {
		if dp.Attributes.Equals(&want) {
			return dp.Count
		}
	}
	return 0
}

func (h *metricsHarness) counter(t *testing.T, name string, kv ...string) int64 {
	t.Helper()
	m, ok := h.find(t, name)
	if !ok {
		return 0
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	require.True(t, ok, "%T", m.Data)
	want := attrs(kv...)
	for _, dp := range sum.DataPoints {
		if dp.Attributes.Equals(&want) {
			return dp.Value
		}
	}
	return 0
}

func (h *metricsHarness) gauge(t *testing.T, name string, kv ...string) (int64, bool) {
	t.Helper()
	m, ok := h.find(t, name)
	if !ok {
		return 0, false
	}
	g, ok := m.Data.(metricdata.Gauge[int64])
	require.True(t, ok, "%T", m.Data)
	want := attrs(kv...)
	for _, dp := range g.DataPoints {
		if dp.Attributes.Equals(&want) {
			return dp.Value, true
		}
	}
	return 0, false
}

func TestMetrics_GetOK(t *testing.T) {
	t.Parallel()
	h := newMetricsHarness(t)
	h.mr.Set("k", "v")
	require.NoError(t, h.client.Get(t.Context(), "k").Err())
	assert.Equal(t, uint64(1), h.durationCount(t, "get", "ok"))
	assert.Zero(t, h.counter(t, "keboola.go.appsproxy.sessionstore.errors", "command", "get"))
}

func TestMetrics_GetMissIsNoError(t *testing.T) {
	t.Parallel()
	h := newMetricsHarness(t)
	require.ErrorIs(t, h.client.Get(t.Context(), "missing").Err(), redis.Nil)
	assert.Equal(t, uint64(1), h.durationCount(t, "get", "miss"))
	assert.Zero(t, h.counter(t, "keboola.go.appsproxy.sessionstore.errors", "command", "get"))
}

func TestMetrics_GetErrorCounted(t *testing.T) {
	t.Parallel()
	h := newMetricsHarness(t)
	h.mr.Close()
	require.Error(t, h.client.Get(t.Context(), "k").Err())
	assert.Equal(t, uint64(1), h.durationCount(t, "get", "error"))
	assert.Equal(t, int64(1), h.counter(t, "keboola.go.appsproxy.sessionstore.errors", "command", "get"))
}

func TestMetrics_CommandNames(t *testing.T) {
	t.Parallel()
	h := newMetricsHarness(t)
	ctx := t.Context()
	require.NoError(t, h.client.Set(ctx, "k", "v", time.Minute).Err())
	require.NoError(t, h.client.Del(ctx, "k").Err())
	require.NoError(t, h.client.Eval(ctx, "return 1", nil).Err())
	require.NoError(t, h.client.Ping(ctx).Err())
	assert.Equal(t, uint64(1), h.durationCount(t, "set", "ok"))
	assert.Equal(t, uint64(1), h.durationCount(t, "del", "ok"))
	assert.Equal(t, uint64(1), h.durationCount(t, "lock", "ok"))
	// Connection setup commands (HELLO, CLIENT SETINFO) are "other" too.
	assert.GreaterOrEqual(t, h.durationCount(t, "other", "ok"), uint64(1))
}

func TestMetrics_Unavailable(t *testing.T) {
	t.Parallel()
	h := newMetricsHarness(t)
	h.metrics.StoreUnavailable(t.Context())
	h.metrics.StoreUnavailable(t.Context())
	assert.Equal(t, int64(2), h.counter(t, "keboola.go.appsproxy.sessionstore.unavailable"))

	var nilMetrics *Metrics
	nilMetrics.StoreUnavailable(t.Context())
}

func TestMetrics_Migrated(t *testing.T) {
	t.Parallel()
	h := newMetricsHarness(t)
	h.metrics.Migrated(t.Context())
	assert.Equal(t, int64(1), h.counter(t, "keboola.go.appsproxy.sessionstore.migrated"))

	var nilMetrics *Metrics
	nilMetrics.Migrated(t.Context())
}

func TestMetrics_Up(t *testing.T) {
	t.Parallel()
	h := newMetricsHarness(t)
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	h.checker.Start(ctx)
	require.Eventually(t, h.checker.Up, 5*time.Second, 10*time.Millisecond)
	v, ok := h.gauge(t, "keboola.go.appsproxy.sessionstore.up")
	require.True(t, ok)
	assert.Equal(t, int64(1), v)

	h.mr.Close()
	require.NoError(t, h.clock.BlockUntilContext(t.Context(), 1))
	h.clock.Advance(checkInterval)
	require.Eventually(t, func() bool { return !h.checker.Up() }, 5*time.Second, 10*time.Millisecond)
	v, _ = h.gauge(t, "keboola.go.appsproxy.sessionstore.up")
	assert.Equal(t, int64(0), v)
}

func TestMetrics_Pool(t *testing.T) {
	t.Parallel()
	h := newMetricsHarness(t)
	require.NoError(t, h.client.Ping(t.Context()).Err())
	total, ok := h.gauge(t, "keboola.go.appsproxy.sessionstore.pool.connections", "state", "total")
	require.True(t, ok)
	assert.Equal(t, int64(1), total)
	idle, ok := h.gauge(t, "keboola.go.appsproxy.sessionstore.pool.connections", "state", "idle")
	require.True(t, ok)
	assert.Equal(t, int64(1), idle)
	_, ok = h.gauge(t, "keboola.go.appsproxy.sessionstore.pool.timeouts")
	assert.True(t, ok)
}
