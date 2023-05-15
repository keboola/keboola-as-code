package middleware_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/dimfeld/httptreemux/v5"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	export "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
)

const (
	testTraceID    = 0xabcd
	testSpanIDBase = 0x1000
)

type testIDGenerator struct {
	spanID uint16
}

func (g *testIDGenerator) NewIDs(ctx context.Context) (otelTrace.TraceID, otelTrace.SpanID) {
	traceID := toTraceID(testTraceID)
	return traceID, g.NewSpanID(ctx, traceID)
}

func (g *testIDGenerator) NewSpanID(_ context.Context, _ otelTrace.TraceID) otelTrace.SpanID {
	g.spanID++
	return toSpanID(testSpanIDBase + g.spanID)
}

func toTraceID(in uint16) otelTrace.TraceID { //nolint: unparam
	tmp := make([]byte, 16)
	binary.BigEndian.PutUint16(tmp, in)
	return *(*[16]byte)(tmp)
}

func toSpanID(in uint16) otelTrace.SpanID {
	tmp := make([]byte, 8)
	binary.BigEndian.PutUint16(tmp, in)
	return *(*[8]byte)(tmp)
}

func TestOpenTelemetryMiddleware(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup tracing
	res, err := resource.New(ctx)
	assert.NoError(t, err)
	traceExporter := tracetest.NewInMemoryExporter()
	tracerProvider := trace.NewTracerProvider(
		trace.WithSyncer(traceExporter),
		trace.WithResource(res),
		trace.WithIDGenerator(&testIDGenerator{}),
	)

	// Setup metrics
	metricExporter, err := export.New()
	assert.NoError(t, err)
	meterProvider := metric.NewMeterProvider(
		metric.WithReader(metricExporter),
		metric.WithResource(res),
	)

	// Create muxer
	mux := httptreemux.NewContextMux()
	mux.UseHandler(middleware.OpenTelemetryExtractRoute())
	handler := middleware.Wrap(
		mux,
		middleware.RequestInfo(),
		middleware.OpenTelemetry(
			tracerProvider, meterProvider,
			middleware.WithRedactedRouteParam("secret1"),
			middleware.WithRedactedQueryParam("secret2"),
			middleware.WithRedactedHeader("X-StorageAPI-Token"),
		),
	)

	// Register endpoint
	mux.NewGroup("/api").POST("/item/:id/:secret1", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("some error"))
	})

	// Send request
	rec := httptest.NewRecorder()
	body := io.NopCloser(strings.NewReader("some body"))
	req := httptest.NewRequest("POST", "/api/item/123/my-secret-1?foo=bar&secret2=my-secret-2", body)
	req.Header.Set("User-Agent", "my-user-agent")
	req.Header.Set("X-StorageAPI-Token", "my-token")
	handler.ServeHTTP(rec, req)

	// Assert
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Equal(t, "some error", rec.Body.String())
	assert.Equal(t, expectedSpans(), actualSpans(t, traceExporter))
	assert.Equal(t, expectedMetrics(), actualMetrics(t, ctx, metricExporter))
}

func actualSpans(t *testing.T, exporter *tracetest.InMemoryExporter) tracetest.SpanStubs {
	t.Helper()

	// Get spans
	spans := exporter.GetSpans()

	// Sort spans
	sort.SliceStable(spans, func(i, j int) bool {
		return spans[i].SpanContext.SpanID().String() < spans[j].SpanContext.SpanID().String()
	})

	// Clear dynamic values
	for i := range spans {
		span := &spans[i]
		span.StartTime = time.Time{}
		span.EndTime = time.Time{}
		span.Resource = nil
		span.InstrumentationLibrary.Name = ""
		span.InstrumentationLibrary.Version = ""
		for j := range span.Events {
			event := &span.Events[j]
			event.Time = time.Time{}
		}
		for j, attr := range span.Attributes {
			if attr.Key == "request.id" && len(attr.Value.AsString()) > 0 {
				span.Attributes[j] = attribute.String(string(attr.Key), "<dynamic>")
			}
			if attr.Key == "http.response.header.x-request-id" && len(attr.Value.AsString()) > 0 {
				span.Attributes[j] = attribute.String(string(attr.Key), "<dynamic>")
			}
		}
	}

	return spans
}

func actualMetrics(t *testing.T, ctx context.Context, reader metric.Reader) []metricdata.Metrics {
	t.Helper()

	// Get metrics
	all := &metricdata.ResourceMetrics{}
	assert.NoError(t, reader.Collect(ctx, all))
	assert.Len(t, all.ScopeMetrics, 1)
	metrics := all.ScopeMetrics[0].Metrics

	// DataPoints have random order, sort them by statusCode and URL.
	dataPointKey := func(attrs attribute.Set) string {
		status, _ := attrs.Value("http.status_code")
		url, _ := attrs.Value("http.url")
		return fmt.Sprintf("%d:%s", status.AsInt64(), url.AsString())
	}

	// Clear dynamic values
	s := spew.NewDefaultConfig()
	s.DisableCapacities = true
	s.DisablePointerAddresses = true
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
	return metrics
}

func expectedSpans() tracetest.SpanStubs {
	rootSpanContext := otelTrace.NewSpanContext(otelTrace.SpanContextConfig{
		TraceID:    toTraceID(testTraceID),
		SpanID:     toSpanID(testSpanIDBase + 1),
		TraceFlags: otelTrace.FlagsSampled,
	})
	return tracetest.SpanStubs{
		{
			Name:        "http.server.request",
			SpanKind:    otelTrace.SpanKindServer,
			SpanContext: rootSpanContext,
			Status: trace.Status{
				Code:        codes.Error,
				Description: "",
			},
			Attributes: []attribute.KeyValue{
				attribute.String("http.method", "POST"),
				attribute.String("http.scheme", "http"),
				attribute.String("http.flavor", "1.1"),
				attribute.String("net.host.name", "example.com"),
				attribute.String("net.sock.peer.addr", "192.0.2.1"),
				attribute.Int("net.sock.peer.port", 1234),
				attribute.String("http.user_agent", "my-user-agent"),
				attribute.String("request.id", "<dynamic>"),
				attribute.String("http.query.foo", "bar"),
				attribute.String("http.query.secret2", "****"),
				attribute.String("http.header.x-storageapi-token", "****"),
				attribute.String("resource.name", "/api/item/:id/:secret1"),
				attribute.String("http.route", "/api/item/:id/:secret1"),
				attribute.String("http.route_param.id", "123"),
				attribute.String("http.route_param.secret1", "****"),
				attribute.String("http.response.header.x-request-id", "<dynamic>"),
				attribute.Int("http.wrote_bytes", 10),
				attribute.Int("http.status_code", http.StatusInternalServerError),
			},
		},
	}
}

func expectedMetrics() []metricdata.Metrics {
	attrs := attribute.NewSet(
		attribute.String("http.method", "POST"),
		attribute.String("http.scheme", "http"),
		attribute.String("http.flavor", "1.1"),
		attribute.String("net.host.name", "example.com"),
		attribute.String("net.sock.peer.addr", "192.0.2.1"),
		attribute.Int("net.sock.peer.port", 1234),
		attribute.String("http.user_agent", "my-user-agent"),
		attribute.String("http.route", "/api/item/:id/:secret1"),
		attribute.Int("http.status_code", http.StatusInternalServerError),
	)
	return []metricdata.Metrics{
		{
			Name:        "http.server.request_content_length",
			Description: "",
			Data: metricdata.Sum[int64]{
				Temporality: 1,
				IsMonotonic: true, // counter
				DataPoints: []metricdata.DataPoint[int64]{
					{Value: 0, Attributes: attrs},
				},
			},
		},
		{
			Name:        "http.server.response_content_length",
			Description: "",
			Data: metricdata.Sum[int64]{
				Temporality: 1,
				IsMonotonic: true, // counter
				DataPoints: []metricdata.DataPoint[int64]{
					{Value: 10, Attributes: attrs},
				},
			},
		},
		{
			Name:        "http.server.duration",
			Description: "",
			Unit:        "",
			Data: metricdata.Histogram[float64]{
				Temporality: 1,
				DataPoints: []metricdata.HistogramDataPoint[float64]{
					{
						Count:      1,
						Bounds:     []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000},
						Attributes: attrs,
					},
				},
			},
		},
	}
}
