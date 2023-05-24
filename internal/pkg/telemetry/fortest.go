package telemetry

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
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
	Spans(t *testing.T) tracetest.SpanStubs
	Metrics(t *testing.T) []metricdata.Metrics
	AssertSpans(t *testing.T, expectedSpans tracetest.SpanStubs)
	AssertMetrics(t *testing.T, expectedMetrics []metricdata.Metrics)
}

type forTest struct {
	*telemetry
	spanExporter   *tracetest.InMemoryExporter
	metricExporter metricsdk.Reader
}

func NewForTest(t *testing.T) ForTest {
	t.Helper()
	spanExporter := tracetest.NewInMemoryExporter()
	metricExporter, err := export.New()
	require.NoError(t, err)
	return &forTest{
		telemetry: newTelemetry(
			tracesdk.NewTracerProvider(
				tracesdk.WithSyncer(spanExporter),
				tracesdk.WithIDGenerator(&testIDGenerator{}),
			),
			metricsdk.NewMeterProvider(
				metricsdk.WithReader(metricExporter),
				metricsdk.WithView(prometheus.View()),
			),
		),
		spanExporter:   spanExporter,
		metricExporter: metricExporter,
	}
}

func (v *forTest) TraceID(n int) trace.TraceID {
	return toTraceID(testTraceIDBase + uint16(n))
}

func (v *forTest) SpanID(n int) trace.SpanID {
	return toSpanID(testSpanIDBase + uint16(n))
}

func (v *forTest) Spans(t *testing.T) tracetest.SpanStubs {
	t.Helper()
	return getActualSpans(t, v.spanExporter)
}

func (v *forTest) Metrics(t *testing.T) []metricdata.Metrics {
	t.Helper()
	return getActualMetrics(t, context.Background(), v.metricExporter)
}

func (v *forTest) AssertSpans(t *testing.T, expectedSpans tracetest.SpanStubs) {
	t.Helper()
	actualSpans := v.Spans(t)

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

func (v *forTest) AssertMetrics(t *testing.T, expectedMetrics []metricdata.Metrics) {
	t.Helper()
	actualMetrics := v.Metrics(t)

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

type testIDGenerator struct {
	traceID uint16
	spanID  uint16
}

func (g *testIDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	g.traceID++
	traceID := toTraceID(testTraceIDBase + g.traceID)
	return traceID, g.NewSpanID(ctx, traceID)
}

func (g *testIDGenerator) NewSpanID(_ context.Context, _ trace.TraceID) trace.SpanID {
	g.spanID++
	return toSpanID(testSpanIDBase + g.spanID)
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

func getActualSpans(t *testing.T, exporter *tracetest.InMemoryExporter) tracetest.SpanStubs {
	t.Helper()
	spans := exporter.GetSpans()
	cleanAndSortSpans(spans)
	return spans
}

func cleanAndSortSpans(spans tracetest.SpanStubs) {
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
			if attr.Key == "http.request_id" && len(attr.Value.AsString()) > 0 {
				s.Attributes[k] = attribute.String(string(attr.Key), "<dynamic>")
			}
			if attr.Key == "http.response.header.x-request-id" && len(attr.Value.AsString()) > 0 {
				s.Attributes[k] = attribute.String(string(attr.Key), "<dynamic>")
			}
		}
	}
}

func getActualMetrics(t *testing.T, ctx context.Context, reader metricsdk.Reader) []metricdata.Metrics {
	t.Helper()
	all := &metricdata.ResourceMetrics{}
	assert.NoError(t, reader.Collect(ctx, all))
	assert.Len(t, all.ScopeMetrics, 1)
	metrics := all.ScopeMetrics[0].Metrics
	cleanAndSortMetrics(metrics)
	return metrics
}

func cleanAndSortMetrics(metrics []metricdata.Metrics) {
	// DataPoints have random order, sort them by statusCode and URL.
	dataPointKey := func(attrs attribute.Set) string {
		status, _ := attrs.Value("http.status_code")
		url, _ := attrs.Value("http.url")
		return fmt.Sprintf("%d:%s", status.AsInt64(), url.AsString())
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
			}
		case metricdata.Sum[float64]:
			sort.SliceStable(record.DataPoints, func(i, j int) bool {
				return dataPointKey(record.DataPoints[i].Attributes) < dataPointKey(record.DataPoints[j].Attributes)
			})
			for k := range record.DataPoints {
				point := &record.DataPoints[k]
				point.StartTime = time.Time{}
				point.Time = time.Time{}
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
				point.Sum = 0
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
				point.Sum = 0
			}
		}
	}
}
