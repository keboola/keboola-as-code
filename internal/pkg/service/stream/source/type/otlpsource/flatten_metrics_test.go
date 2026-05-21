package otlpsource

import (
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestFlattenMetrics_Empty(t *testing.T) {
	t.Parallel()

	assert.Empty(t, FlattenMetrics(pmetric.NewMetrics()))
}

func TestFlattenMetrics_Gauge_IntAndDouble(t *testing.T) {
	t.Parallel()

	metrics := pmetric.NewMetrics()
	m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("temperature")
	m.SetUnit("C")
	gauge := m.SetEmptyGauge()
	gauge.DataPoints().AppendEmpty().SetIntValue(42)
	gauge.DataPoints().AppendEmpty().SetDoubleValue(98.6)

	records := FlattenMetrics(metrics)
	require.Len(t, records, 2)

	assert.Equal(t, "gauge", mustGet(t, records[0].Body, "metric_type"))
	assert.Equal(t, "temperature", mustGet(t, records[0].Body, "metric_name"))
	assert.Equal(t, "C", mustGet(t, records[0].Body, "metric_unit"))
	assert.Equal(t, int64(42), mustGet(t, records[0].Body, "value"))
	assert.InDelta(t, 98.6, mustGet(t, records[1].Body, "value"), 1e-9)
}

func TestFlattenMetrics_Gauge_EmptyValue(t *testing.T) {
	t.Parallel()

	metrics := pmetric.NewMetrics()
	m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetEmptyGauge().DataPoints().AppendEmpty() // no value set

	records := FlattenMetrics(metrics)
	require.Len(t, records, 1)
	assert.Nil(t, mustGet(t, records[0].Body, "value"), "empty value type yields nil so consumers can distinguish from 0")
}

func TestFlattenMetrics_Sum_Discriminators(t *testing.T) {
	t.Parallel()

	metrics := pmetric.NewMetrics()
	m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	sum := m.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.DataPoints().AppendEmpty().SetIntValue(100)

	records := FlattenMetrics(metrics)
	require.Len(t, records, 1)
	assert.Equal(t, "sum", mustGet(t, records[0].Body, "metric_type"))
	assert.Equal(t, true, mustGet(t, records[0].Body, "is_monotonic"))
	assert.Equal(t, "Cumulative", mustGet(t, records[0].Body, "aggregation_temporality"))
}

func TestFlattenMetrics_Histogram_AllFields(t *testing.T) {
	t.Parallel()

	metrics := pmetric.NewMetrics()
	m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	h := m.SetEmptyHistogram()
	h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dp := h.DataPoints().AppendEmpty()
	dp.SetCount(150)
	dp.SetSum(4523.7)
	dp.SetMin(1.2)
	dp.SetMax(892.1)
	dp.BucketCounts().FromRaw([]uint64{10, 50, 60, 20, 10})
	dp.ExplicitBounds().FromRaw([]float64{5, 25, 50, 100})

	records := FlattenMetrics(metrics)
	require.Len(t, records, 1)
	body := records[0].Body
	assert.Equal(t, "histogram", mustGet(t, body, "metric_type"))
	assert.Equal(t, uint64(150), mustGet(t, body, "count"))
	assert.InDelta(t, 4523.7, mustGet(t, body, "sum"), 1e-9)
	assert.InDelta(t, 1.2, mustGet(t, body, "min"), 1e-9)
	assert.InDelta(t, 892.1, mustGet(t, body, "max"), 1e-9)
	assert.Equal(t, []uint64{10, 50, 60, 20, 10}, mustGet(t, body, "bucket_counts"))
	assert.Equal(t, []float64{5, 25, 50, 100}, mustGet(t, body, "explicit_bounds"))
	assert.Equal(t, "Delta", mustGet(t, body, "aggregation_temporality"))
}

func TestFlattenMetrics_Histogram_OmitsOptionalSumMinMax(t *testing.T) {
	t.Parallel()

	metrics := pmetric.NewMetrics()
	m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	dp := m.SetEmptyHistogram().DataPoints().AppendEmpty()
	dp.SetCount(10) // no sum/min/max
	dp.BucketCounts().FromRaw([]uint64{10})

	records := FlattenMetrics(metrics)
	require.Len(t, records, 1)

	_, hasSum := records[0].Body.Get("sum")
	_, hasMin := records[0].Body.Get("min")
	_, hasMax := records[0].Body.Get("max")
	assert.False(t, hasSum, "sum absent when HasSum is false")
	assert.False(t, hasMin)
	assert.False(t, hasMax)
}

func TestFlattenMetrics_ExponentialHistogram(t *testing.T) {
	t.Parallel()

	metrics := pmetric.NewMetrics()
	m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	eh := m.SetEmptyExponentialHistogram()
	eh.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := eh.DataPoints().AppendEmpty()
	dp.SetCount(200)
	dp.SetSum(123.4)
	dp.SetScale(3)
	dp.SetZeroCount(5)

	records := FlattenMetrics(metrics)
	require.Len(t, records, 1)
	body := records[0].Body
	assert.Equal(t, "exponential_histogram", mustGet(t, body, "metric_type"))
	assert.Equal(t, uint64(200), mustGet(t, body, "count"))
	assert.InDelta(t, 123.4, mustGet(t, body, "sum"), 1e-9)
	assert.Equal(t, int32(3), mustGet(t, body, "scale"))
	assert.Equal(t, uint64(5), mustGet(t, body, "zero_count"))
}

func TestFlattenMetrics_Summary_Quantiles(t *testing.T) {
	t.Parallel()

	metrics := pmetric.NewMetrics()
	m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	dp := m.SetEmptySummary().DataPoints().AppendEmpty()
	dp.SetCount(1000)
	dp.SetSum(50000)
	q1 := dp.QuantileValues().AppendEmpty()
	q1.SetQuantile(0.5)
	q1.SetValue(40)
	q2 := dp.QuantileValues().AppendEmpty()
	q2.SetQuantile(0.99)
	q2.SetValue(120)

	records := FlattenMetrics(metrics)
	require.Len(t, records, 1)
	body := records[0].Body
	assert.Equal(t, "summary", mustGet(t, body, "metric_type"))

	qvs, ok := mustGet(t, body, "quantile_values").([]any)
	require.True(t, ok)
	require.Len(t, qvs, 2)

	first, ok := qvs[0].(*orderedmap.OrderedMap)
	require.True(t, ok)
	assert.InDelta(t, 0.5, mustGet(t, first, "quantile"), 1e-9)
	assert.InDelta(t, 40.0, mustGet(t, first, "value"), 1e-9)
}

func TestFlattenMetrics_TimestampsAndAttributes(t *testing.T) {
	t.Parallel()

	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "api")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("otel-sdk")

	m := sm.Metrics().AppendEmpty()
	m.SetName("latency")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Date(2024, 1, 15, 10, 29, 0, 0, time.UTC)))
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)))
	dp.Attributes().PutStr("http.method", "GET")
	dp.SetDoubleValue(1.5)

	records := FlattenMetrics(metrics)
	require.Len(t, records, 1)
	body := records[0].Body

	assert.Equal(t, "2024-01-15T10:29:00Z", mustGet(t, body, "start_timestamp"))
	assert.Equal(t, "2024-01-15T10:30:00Z", mustGet(t, body, "timestamp"))
	attrs := mustMap(t, body, "attributes")
	assert.Equal(t, "GET", mustGet(t, attrs, "http.method"))
	resource := mustMap(t, body, "resource")
	assert.Equal(t, "api", mustGet(t, resource, "service.name"))
	scope := mustMap(t, body, "scope")
	assert.Equal(t, "otel-sdk", mustGet(t, scope, "name"))
}

func TestFlattenMetrics_MixedTypes_OneRecordPerDataPoint(t *testing.T) {
	t.Parallel()

	metrics := pmetric.NewMetrics()
	sm := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()

	g := sm.Metrics().AppendEmpty()
	g.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)
	g.Gauge().DataPoints().AppendEmpty().SetIntValue(2)

	s := sm.Metrics().AppendEmpty()
	s.SetEmptySum().DataPoints().AppendEmpty().SetIntValue(3)

	h := sm.Metrics().AppendEmpty()
	h.SetEmptyHistogram().DataPoints().AppendEmpty().SetCount(4)

	records := FlattenMetrics(metrics)
	// 2 gauge + 1 sum + 1 histogram = 4 records
	assert.Len(t, records, 4)
}
