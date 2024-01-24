package telemetry

import (
	"context"
	"encoding/binary"
	"math"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	export "go.opentelemetry.io/otel/exporters/prometheus"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/metric/prometheus"
)

const (
	testTraceIDBase = 0xabcd
	testSpanIDBase  = 0x1000
)

type ForTest interface {
	Telemetry
	TraceID(n int) trace.TraceID
	SpanID(n int) trace.SpanID
	Reset()
	SetSpanFilter(f TestSpanFilter) ForTest
	Spans(t *testing.T, opts ...TestSpanOption) tracetest.SpanStubs
	Metrics(t *testing.T, opts ...TestMeterOption) []metricdata.Metrics
	AssertSpans(t *testing.T, expectedSpans tracetest.SpanStubs, opts ...TestSpanOption)
	AssertMetrics(t *testing.T, expectedMetrics []metricdata.Metrics, opts ...TestMeterOption)
}

type TestAttributeMapper func(attr attribute.KeyValue) attribute.KeyValue

type TestSpanOption func(config *assertSpanConfig)

type TestMeterOption func(config *assertMetricConfig)

// TestSpanFilter returns true, if the span should be included in collected spans in a test.
type TestSpanFilter func(ctx context.Context, spanName string, opts ...trace.SpanStartOption) bool

type assertSpanConfig struct {
	attributeMapper TestAttributeMapper
}

type assertMetricConfig struct {
	keepHistogramSum bool
	attributeMapper  TestAttributeMapper
	dataPointSortKey func(attrs attribute.Set) string
}

type forTest struct {
	*telemetry
	idGenerator    *testIDGenerator
	spanExporter   *tracetest.InMemoryExporter
	metricExporter metricsdk.Reader
	traceProvider  *filterTraceProvider
}

// WithSpanAttributeMapper set a mapping function for span attributes.
func WithSpanAttributeMapper(v TestAttributeMapper) TestSpanOption {
	return func(cnf *assertSpanConfig) {
		cnf.attributeMapper = v
	}
}

// WithMeterAttributeMapper set a mapping function for span attributes.
func WithMeterAttributeMapper(v TestAttributeMapper) TestMeterOption {
	return func(cnf *assertMetricConfig) {
		cnf.attributeMapper = v
	}
}

// WithDataPointSortKey set a function to generate sort key for each data point.
// DataPoints are internally represented as a map, so they have random order.
func WithDataPointSortKey(v func(attrs attribute.Set) string) TestMeterOption {
	return func(cnf *assertMetricConfig) {
		cnf.dataPointSortKey = v
	}
}

func WithKeepHistogramSum(v bool) TestMeterOption {
	return func(cnf *assertMetricConfig) {
		cnf.keepHistogramSum = v
	}
}

func newAssertSpanConfig(opts []TestSpanOption) assertSpanConfig {
	cnf := assertSpanConfig{}
	for _, o := range opts {
		o(&cnf)
	}
	return cnf
}

func newAssertMeterConfig(opts []TestMeterOption) assertMetricConfig {
	cnf := assertMetricConfig{}
	for _, o := range opts {
		o(&cnf)
	}
	return cnf
}

func NewForTest(t *testing.T) ForTest {
	t.Helper()
	idGenerator := &testIDGenerator{}
	spanExporter := tracetest.NewInMemoryExporter()
	metricExporter, err := export.New()
	require.NoError(t, err)
	tp := &filterTraceProvider{
		provider: tracesdk.NewTracerProvider(
			tracesdk.WithSyncer(spanExporter),
			tracesdk.WithIDGenerator(idGenerator),
		),
	}
	mp := metricsdk.NewMeterProvider(
		metricsdk.WithReader(metricExporter),
		metricsdk.WithView(prometheus.View()),
	)
	return &forTest{
		traceProvider:  tp,
		telemetry:      newTelemetry(tp, mp),
		idGenerator:    idGenerator,
		spanExporter:   spanExporter,
		metricExporter: metricExporter,
	}
}

func (v *forTest) SetSpanFilter(f TestSpanFilter) ForTest {
	v.traceProvider.filter = f
	v.Reset()
	return v
}

func (v *forTest) TraceID(n int) trace.TraceID {
	return toTraceID(testTraceIDBase + uint16(n))
}

func (v *forTest) SpanID(n int) trace.SpanID {
	return toSpanID(testSpanIDBase + uint16(n))
}

func (v *forTest) Reset() {
	v.spanExporter.Reset()
	_ = v.metricExporter.Collect(context.Background(), &metricdata.ResourceMetrics{})
	v.idGenerator.Reset()
}

func (v *forTest) Spans(t *testing.T, opts ...TestSpanOption) tracetest.SpanStubs {
	t.Helper()
	return getActualSpans(t, v.spanExporter, opts...)
}

func (v *forTest) Metrics(t *testing.T, opts ...TestMeterOption) []metricdata.Metrics {
	t.Helper()
	return getActualMetrics(t, context.Background(), v.metricExporter, opts...)
}

func (v *forTest) AssertSpans(t *testing.T, expectedSpans tracetest.SpanStubs, opts ...TestSpanOption) {
	t.Helper()
	actualSpans := v.Spans(t, opts...)

	// Compare spans one by one, for easier debugging
	assert.Equalf(
		t, len(expectedSpans), len(actualSpans),
		`unexpected number of spans: actual "%d", expected "%d"`, len(actualSpans), len(expectedSpans),
	)
	spansCount := (int)(math.Max((float64)(len(expectedSpans)), (float64)(len(actualSpans))))
	var actualSpan tracetest.SpanStub
	var expectedSpan tracetest.SpanStub
	for i := 0; i < spansCount; i++ {
		if len(actualSpans) > i {
			actualSpan = actualSpans[i]
		} else {
			actualSpan = tracetest.SpanStub{Name: "<missing span>"}
		}
		if len(expectedSpans) > i {
			expectedSpan = expectedSpans[i]
		} else {
			expectedSpan = tracetest.SpanStub{Name: "<missing span>"}
		}
		if assert.Equalf(t, expectedSpan.Name, actualSpan.Name, `span position in list "%d"`, i+1) {
			assert.Equal(t, expectedSpan, actualSpan)
		}
	}
}

func (v *forTest) AssertMetrics(t *testing.T, expectedMetrics []metricdata.Metrics, opts ...TestMeterOption) {
	t.Helper()
	actualMetrics := v.Metrics(t, opts...)

	// Compare metrics one by one, for easier debugging
	assert.Equalf(
		t, len(expectedMetrics), len(actualMetrics),
		`unexpected number of metrics: actual "%d", expected "%d"`, len(expectedMetrics), len(actualMetrics),
	)
	metersCount := (int)(math.Max((float64)(len(expectedMetrics)), (float64)(len(actualMetrics))))
	var actualMeter metricdata.Metrics
	var expectedMeter metricdata.Metrics
	for i := 0; i < metersCount; i++ {
		if len(actualMetrics) > i {
			actualMeter = actualMetrics[i]
		} else {
			actualMeter = metricdata.Metrics{Name: "<missing metric>"}
		}
		if len(expectedMetrics) > i {
			expectedMeter = expectedMetrics[i]
		} else {
			expectedMeter = metricdata.Metrics{Name: "<missing metric>"}
		}
		if assert.Equalf(t, expectedMeter.Name, actualMeter.Name, `meter position in list "%d"`, i+1) {
			assert.Equal(t, expectedMeter, actualMeter)
		}
	}
}

type filterTraceProvider struct {
	embedded.TracerProvider
	filter   TestSpanFilter
	provider trace.TracerProvider
}

type filterTracer struct {
	embedded.Tracer
	tp     *filterTraceProvider
	tracer trace.Tracer
}

func (tp *filterTraceProvider) Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	return &filterTracer{tp: tp, tracer: tp.provider.Tracer(name, opts...)}
}

func (t *filterTracer) Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if t.tp.filter != nil && !t.tp.filter(ctx, spanName, opts...) {
		return nopTracer.Start(ctx, spanName, opts...)
	}
	return t.tracer.Start(ctx, spanName, opts...)
}

type testIDGenerator struct {
	traceID atomic.Int64
	spanID  atomic.Int64
}

func (g *testIDGenerator) Reset() {
	g.traceID.Store(0)
	g.spanID.Store(0)
}

func (g *testIDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	v := g.traceID.Add(1)
	traceID := toTraceID(testTraceIDBase + uint16(v))
	return traceID, g.NewSpanID(ctx, traceID)
}

func (g *testIDGenerator) NewSpanID(_ context.Context, _ trace.TraceID) trace.SpanID {
	v := g.spanID.Add(1)
	return toSpanID(testSpanIDBase + uint16(v))
}

func toTraceID(in uint16) trace.TraceID { //nolint: unparam
	tmp := make([]byte, 16)
	binary.BigEndian.PutUint16(tmp, in)
	return *(*[16]byte)(tmp)
}

func toSpanID(in uint16) trace.SpanID {
	tmp := make([]byte, 8)
	binary.BigEndian.PutUint16(tmp, in)
	return *(*[8]byte)(tmp)
}

func getActualSpans(t *testing.T, exporter *tracetest.InMemoryExporter, opts ...TestSpanOption) tracetest.SpanStubs {
	t.Helper()
	spans := exporter.GetSpans()
	cleanAndSortSpans(spans, opts...)
	return spans
}

func cleanAndSortSpans(spans tracetest.SpanStubs, opts ...TestSpanOption) {
	cnf := newAssertSpanConfig(opts)

	// Sort spans
	sort.SliceStable(spans, func(i, j int) bool {
		return spans[i].SpanContext.SpanID().String() < spans[j].SpanContext.SpanID().String()
	})

	// Clean dynamic values
	for i := range spans {
		s := &spans[i]
		s.StartTime = time.Time{}
		s.EndTime = time.Time{}
		s.Resource = nil
		s.InstrumentationLibrary.Name = ""
		s.InstrumentationLibrary.Version = ""
		for j := range s.Events {
			event := &s.Events[j]
			event.Time = time.Time{}
		}
		for k, attr := range s.Attributes {
			if cnf.attributeMapper != nil {
				s.Attributes[k] = cnf.attributeMapper(attr)
			}
		}
	}
}

func getActualMetrics(t *testing.T, ctx context.Context, reader metricsdk.Reader, opts ...TestMeterOption) (out []metricdata.Metrics) {
	t.Helper()
	all := &metricdata.ResourceMetrics{}
	assert.NoError(t, reader.Collect(ctx, all))
	sort.SliceStable(all.ScopeMetrics, func(i, j int) bool {
		return all.ScopeMetrics[i].Scope.Name < all.ScopeMetrics[j].Scope.Name
	})
	for _, item := range all.ScopeMetrics {
		out = append(out, item.Metrics...)
	}
	cleanAndSortMetrics(out, opts...)
	return out
}

func cleanAndSortMetrics(metrics []metricdata.Metrics, opts ...TestMeterOption) {
	cfg := newAssertMeterConfig(opts)

	// DataPoints have random order, sort them by statusCode and URL.
	dataPointKey := func(attrs attribute.Set) string {
		if cfg.dataPointSortKey != nil {
			return cfg.dataPointSortKey(attrs)
		}
		return ""
	}

	mapAttributes := func(set attribute.Set) attribute.Set {
		if cfg.attributeMapper == nil {
			return set
		}
		var attrs []attribute.KeyValue
		for _, attr := range set.ToSlice() {
			attrs = append(attrs, cfg.attributeMapper(attr))
		}
		return attribute.NewSet(attrs...)
	}

	// Clear dynamic values
	for i := range metrics {
		item := &metrics[i]

		switch record := item.Data.(type) {
		case metricdata.Sum[int64]:
			sort.SliceStable(record.DataPoints, func(i, j int) bool {
				return dataPointKey(record.DataPoints[i].Attributes) < dataPointKey(record.DataPoints[j].Attributes)
			})
			for k := range record.DataPoints {
				point := &record.DataPoints[k]
				point.StartTime = time.Time{}
				point.Time = time.Time{}
				point.Attributes = mapAttributes(point.Attributes)
			}
		case metricdata.Sum[float64]:
			sort.SliceStable(record.DataPoints, func(i, j int) bool {
				return dataPointKey(record.DataPoints[i].Attributes) < dataPointKey(record.DataPoints[j].Attributes)
			})
			for k := range record.DataPoints {
				point := &record.DataPoints[k]
				point.StartTime = time.Time{}
				point.Time = time.Time{}
				point.Attributes = mapAttributes(point.Attributes)
			}
		case metricdata.Histogram[int64]:
			sort.SliceStable(record.DataPoints, func(i, j int) bool {
				return dataPointKey(record.DataPoints[i].Attributes) < dataPointKey(record.DataPoints[j].Attributes)
			})
			for k := range record.DataPoints {
				point := &record.DataPoints[k]
				point.StartTime = time.Time{}
				point.Time = time.Time{}
				point.BucketCounts = nil
				point.Min = metricdata.Extrema[int64]{}
				point.Max = metricdata.Extrema[int64]{}
				if !cfg.keepHistogramSum {
					point.Sum = 0
				}
				point.Attributes = mapAttributes(point.Attributes)
			}
		case metricdata.Histogram[float64]:
			sort.SliceStable(record.DataPoints, func(i, j int) bool {
				return dataPointKey(record.DataPoints[i].Attributes) < dataPointKey(record.DataPoints[j].Attributes)
			})
			for k := range record.DataPoints {
				point := &record.DataPoints[k]
				point.StartTime = time.Time{}
				point.Time = time.Time{}
				point.BucketCounts = nil
				point.Min = metricdata.Extrema[float64]{}
				point.Max = metricdata.Extrema[float64]{}
				if !cfg.keepHistogramSum {
					point.Sum = 0
				}
				point.Attributes = mapAttributes(point.Attributes)
			}
		}
	}
}
