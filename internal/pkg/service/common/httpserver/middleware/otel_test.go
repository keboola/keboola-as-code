package middleware_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/dimfeld/httptreemux/v5"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	otelTrace "go.opentelemetry.io/otel/trace"
	goa "goa.design/goa/v3/pkg"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const (
	responseContent = "some response"
)

type MiddlewareTest struct {
	middlewareWrapper func(handler http.Handler, tel telemetry.Telemetry, cfg middleware.Config) http.Handler
	expectedMetrics   []metricdata.Metrics
}

func middlewareTests() []MiddlewareTest {
	req1Attrs := attribute.NewSet(
		attribute.String("http.method", http.MethodGet),
		attribute.String("http.scheme", "http"),
		attribute.String("net.host.name", "example.com"),
		attribute.String("http.route", "/api/ignored-tracing"),
		attribute.Int("http.status_code", http.StatusOK),
		attribute.String("endpoint.name", "/api/ignored-tracing"),
	)
	req2Attrs := attribute.NewSet(
		attribute.String("http.method", "POST"),
		attribute.String("http.scheme", "http"),
		attribute.String("net.host.name", "example.com"),
		attribute.String("http.route", "/api/item/:id/:secret1"),
		attribute.Int("http.status_code", http.StatusInternalServerError),
		attribute.String("endpoint.name", "my-endpoint"),
	)
	basicMetrics := []metricdata.Metrics{
		{
			Name:        "keboola.go.http.server.request.size",
			Description: "Measures the size of HTTP request messages.",
			Unit:        "By",
			Data: metricdata.Sum[int64]{
				Temporality: 1,
				IsMonotonic: true, // counter
				DataPoints: []metricdata.DataPoint[int64]{
					{Value: 0, Attributes: req1Attrs},
					{Value: 0, Attributes: req2Attrs},
				},
			},
		},
		{
			Name:        "keboola.go.http.server.response.size",
			Description: "Measures the size of HTTP response messages.",
			Unit:        "By",
			Data: metricdata.Sum[int64]{
				Temporality: 1,
				IsMonotonic: true, // counter
				DataPoints: []metricdata.DataPoint[int64]{
					{Value: int64(len(responseContent)), Attributes: req1Attrs},
					{Value: int64(len(responseContent)), Attributes: req2Attrs},
				},
			},
		},
		{
			Name:        "keboola.go.http.server.duration",
			Description: "Measures the duration of inbound HTTP requests.",
			Unit:        "ms",
			Data: metricdata.Histogram[float64]{
				Temporality: 1,
				DataPoints: []metricdata.HistogramDataPoint[float64]{
					{
						Count:      1,
						Bounds:     []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000},
						Attributes: req1Attrs,
					},
					{
						Count:      1,
						Bounds:     []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000},
						Attributes: req2Attrs,
					},
				},
			},
		},
	}

	apdexReq1Attrs := attribute.NewSet(
		attribute.String("http.route", "/api/ignored-tracing"),
		attribute.String("endpoint.name", "/api/ignored-tracing"),
	)
	apdexReq2Attrs := attribute.NewSet(
		attribute.String("http.route", "/api/item/:id/:secret1"),
		attribute.String("endpoint.name", "my-endpoint"),
	)
	apdexMetrics := []metricdata.Metrics{
		{
			Name:        "keboola_go_http_server_apdex_count",
			Description: "",
			Data: metricdata.Sum[int64]{
				Temporality: 1,
				IsMonotonic: true,
				DataPoints: []metricdata.DataPoint[int64]{
					{Value: 1, Attributes: apdexReq1Attrs},
					{Value: 1, Attributes: apdexReq2Attrs},
				},
			},
		},
		{
			Name:        "keboola_go_http_server_apdex_500_sum",
			Description: "",
			Data: metricdata.Sum[float64]{
				Temporality: 1,
				IsMonotonic: true,
				DataPoints: []metricdata.DataPoint[float64]{
					// status code = 200, duration OK, apdex=1
					{Value: 1, Attributes: apdexReq1Attrs},
					// status code = 500, apdex=0
					{Value: 0, Attributes: apdexReq2Attrs},
				},
			},
		},
		{
			Name:        "keboola_go_http_server_apdex_1000_sum",
			Description: "",
			Data: metricdata.Sum[float64]{
				Temporality: 1,
				IsMonotonic: true,
				DataPoints: []metricdata.DataPoint[float64]{
					// status code = 200, duration OK, apdex=1
					{Value: 1, Attributes: apdexReq1Attrs},
					// status code = 500, apdex=0
					{Value: 0, Attributes: apdexReq2Attrs},
				},
			},
		},
		{
			Name:        "keboola_go_http_server_apdex_2000_sum",
			Description: "",
			Data: metricdata.Sum[float64]{
				Temporality: 1,
				IsMonotonic: true,
				DataPoints: []metricdata.DataPoint[float64]{
					// status code = 200, duration OK, apdex=1
					{Value: 1, Attributes: apdexReq1Attrs},
					// status code = 500, apdex=0
					{Value: 0, Attributes: apdexReq2Attrs},
				},
			},
		},
	}

	allMetrics := slices.Concat(basicMetrics, apdexMetrics)

	return []MiddlewareTest{
		{
			middlewareWrapper: func(handler http.Handler, tel telemetry.Telemetry, cfg middleware.Config) http.Handler {
				return middleware.Wrap(
					handler,
					middleware.RequestInfo(),
					middleware.Filter(cfg),
					middleware.OpenTelemetry(tel.TracerProvider(), tel.MeterProvider(), cfg),
				)
			},
			expectedMetrics: basicMetrics,
		},
		{
			middlewareWrapper: func(handler http.Handler, tel telemetry.Telemetry, cfg middleware.Config) http.Handler {
				return middleware.Wrap(
					handler,
					middleware.RequestInfo(),
					middleware.Filter(cfg),
					middleware.OpenTelemetry(tel.TracerProvider(), tel.MeterProvider(), cfg),
					middleware.OpenTelemetryApdex(tel.MeterProvider()),
				)
			},
			expectedMetrics: allMetrics,
		},
	}
}

func TestOpenTelemetryMiddleware(t *testing.T) {
	t.Parallel()

	for _, tt := range middlewareTests() {
		// Setup tracing
		tel := telemetry.NewForTest(t)

		// Create muxer
		mux := httptreemux.NewContextMux()
		mux.UseHandler(middleware.OpenTelemetryExtractRoute())
		cfg := middleware.NewConfig(
			middleware.WithRedactedRouteParam("secret1"),
			middleware.WithRedactedQueryParam("secret2"),
			middleware.WithRedactedHeader("X-StorageAPI-Token"),
			middleware.WithFilter(func(req *http.Request) bool {
				return req.URL.Path != "/api/ignored-all"
			}),
			middleware.WithFilterTracing(func(req *http.Request) bool {
				return req.URL.Path != "/api/ignored-tracing"
			}),
		)

		// Create group
		grp := mux.NewGroup("/api")

		// Register ignored routes
		grp.GET("/ignored-all", func(w http.ResponseWriter, req *http.Request) {
			_, span := tel.Tracer().Start(req.Context(), "my-ignored-span-1")
			span.End(nil)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(responseContent))
		})
		grp.GET("/ignored-tracing", func(w http.ResponseWriter, req *http.Request) {
			_, span := tel.Tracer().Start(req.Context(), "my-ignored-span-2")
			span.End(nil)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(responseContent))
		})

		// Simulate Goa framework
		grp.UseHandler(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				// Add fake Goa metadata
				ctx := req.Context()
				ctx = context.WithValue(ctx, goa.ServiceKey, "MyService")
				ctx = context.WithValue(ctx, goa.MethodKey, "MyEndpoint")

				// Apply middleware to nop Goa endpoint
				goaEndpoint := func(ctx context.Context, request any) (any, error) { return nil, nil }
				goaEndpoint = middleware.OpenTelemetryExtractEndpoint()(goaEndpoint)

				// Invoke nop endpoint
				_, err := goaEndpoint(ctx, nil)
				assert.NoError(t, err)

				next.ServeHTTP(w, req.WithContext(ctx))
			})
		})

		// Register endpoint
		grp.POST("/item/:id/:secret1", func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(responseContent))
		})

		// Create request
		rec := httptest.NewRecorder()
		body := io.NopCloser(strings.NewReader("some body"))
		req := httptest.NewRequest(http.MethodPost, "/api/item/123/my-secret-1?foo=bar&secret2=my-secret-2", body)
		req.Header.Set("User-Agent", "my-user-agent")
		req.Header.Set("X-StorageAPI-Token", "my-token")

		// Request handler
		handler := tt.middlewareWrapper(mux, tel, cfg)

		// Send request
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.Equal(t, responseContent, rec.Body.String())

		// Send ignored requests
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/ignored-all", nil))
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, responseContent, rec.Body.String())
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/ignored-tracing", nil))
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, responseContent, rec.Body.String())

		// Assert
		tel.AssertSpans(t, expectedSpans(tel), telemetry.WithSpanAttributeMapper(func(attr attribute.KeyValue) attribute.KeyValue {
			if attr.Key == "http.request_id" && len(attr.Value.AsString()) > 0 {
				return attribute.String(string(attr.Key), "<dynamic>")
			}
			if attr.Key == "http.response.header.x-request-id" && len(attr.Value.AsString()) > 0 {
				return attribute.String(string(attr.Key), "<dynamic>")
			}
			return attr
		}))
		tel.AssertMetrics(t, tt.expectedMetrics, telemetry.WithDataPointSortKey(func(attrs attribute.Set) string {
			status, _ := attrs.Value("http.status_code")
			url, _ := attrs.Value("http.route")
			return fmt.Sprintf("%d:%s", status.AsInt64(), url.AsString())
		}))
	}
}

func expectedSpans(tel telemetry.ForTest) tracetest.SpanStubs {
	req1Context := otelTrace.NewSpanContext(otelTrace.SpanContextConfig{
		TraceID:    tel.TraceID(1),
		SpanID:     tel.SpanID(1),
		TraceFlags: otelTrace.FlagsSampled,
	})
	return tracetest.SpanStubs{
		{
			Name:        "http.server.request",
			SpanKind:    otelTrace.SpanKindServer,
			SpanContext: req1Context,
			Status: trace.Status{
				Code:        codes.Error,
				Description: "",
			},
			Attributes: []attribute.KeyValue{
				attribute.String("http.method", "POST"),
				attribute.String("http.scheme", "http"),
				attribute.String("net.host.name", "example.com"),
				attribute.String("net.sock.peer.addr", "192.0.2.1"),
				attribute.Int("net.sock.peer.port", 1234),
				attribute.String("user_agent.original", "my-user-agent"),
				attribute.String("http.target", "/api/item/123/****"),
				attribute.String("net.protocol.version", "1.1"),
				attribute.String("http.request_id", "<dynamic>"),
				attribute.String("span.kind", "server"),
				attribute.String("span.type", "web"),
				attribute.String("http.query.foo", "bar"),
				attribute.String("http.query.secret2", "****"),
				attribute.String("http.header.x-storageapi-token", "****"),
				attribute.String("resource.name", "/api/item/:id/:secret1 MyEndpoint"),
				attribute.String("http.route", "/api/item/:id/:secret1"),
				attribute.String("http.route_param.id", "123"),
				attribute.String("http.route_param.secret1", "****"),
				attribute.String("endpoint.service", "MyService"),
				attribute.String("endpoint.name", "MyEndpoint"),
				attribute.String("endpoint.name_full", "MyService.MyEndpoint"),
				attribute.String("http.response.header.x-request-id", "<dynamic>"),
				attribute.Int("http.response_content_length", len(responseContent)),
				attribute.Int("http.status_code", http.StatusInternalServerError),
			},
		},
	}
}
