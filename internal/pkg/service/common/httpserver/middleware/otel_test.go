package middleware_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dimfeld/httptreemux/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
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

func TestOpenTelemetryMiddleware(t *testing.T) {
	t.Parallel()

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
			require.NoError(t, err)

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
	handler := middleware.Wrap(
		mux,
		middleware.RequestInfo(),
		middleware.Filter(cfg),
		middleware.OpenTelemetry(tel.TracerProvider(), tel.MeterProvider(), cfg),
	)

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

	// Assert spans (HTTP server metrics are disabled via noop MeterProvider)
	tel.AssertSpans(t, expectedSpans(tel), telemetry.WithSpanAttributeMapper(func(attr attribute.KeyValue) attribute.KeyValue {
		if attr.Key == "http.request_id" && len(attr.Value.AsString()) > 0 {
			return attribute.String(string(attr.Key), "<dynamic>")
		}
		if attr.Key == "http.response.header.x-request-id" && len(attr.Value.AsString()) > 0 {
			return attribute.String(string(attr.Key), "<dynamic>")
		}
		return attr
	}))
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
				attribute.String("server.address", "example.com"),
				attribute.String("http.request.method", "POST"),
				attribute.String("url.scheme", "http"),
				attribute.String("network.peer.address", "192.0.2.1"),
				attribute.Int("network.peer.port", 1234),
				attribute.String("user_agent.original", "my-user-agent"),
				attribute.String("client.address", "192.0.2.1"),
				attribute.String("url.path", "/api/item/123/my-secret-1"),
				attribute.String("network.protocol.version", "1.1"),
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
				attribute.String("http.target", "/api/item/123/****"),
				attribute.String("endpoint.service", "MyService"),
				attribute.String("endpoint.name", "MyEndpoint"),
				attribute.String("endpoint.name_full", "MyService.MyEndpoint"),
				attribute.String("http.response.header.x-request-id", "<dynamic>"),
				attribute.Int("http.response.body.size", len(responseContent)),
				attribute.Int("http.response.status_code", http.StatusInternalServerError),
			},
		},
	}
}
