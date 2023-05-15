package middleware_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dimfeld/httptreemux/v5"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	goa "goa.design/goa/v3/pkg"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
)

func TestEndpointMiddleware(t *testing.T) {
	t.Parallel()

	// Create dummy handler
	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	// Add fake request span, route metadata and Goa metadata
	traceExporter := tracetest.NewInMemoryExporter()
	tracerProvider := trace.NewTracerProvider(trace.WithSyncer(traceExporter))
	goaMetadataProvider := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx, span := tracerProvider.Tracer("my-tracer").Start(req.Context(), "my-span")
			defer span.End()
			ctx = context.WithValue(ctx, middleware.RequestSpanCtxKey, span)
			ctx = httptreemux.AddRouteToContext(ctx, "/my/route")
			ctx = context.WithValue(ctx, goa.ServiceKey, "MyService")
			ctx = context.WithValue(ctx, goa.MethodKey, "MyEndpoint")
			next.ServeHTTP(w, req.WithContext(ctx))
		})
	}

	// Register middlewares
	handler = middleware.Wrap(handler, goaMetadataProvider, middleware.TraceEndpoints())

	// Send request
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)

	// Assert
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK", rec.Body.String())
	spans := traceExporter.GetSpans()
	assert.Len(t, spans, 1)
	assert.Equal(t, []attribute.KeyValue{
		attribute.String("resource.name", "/my/route MyEndpoint"),
		attribute.String("endpoint.service", "MyService"),
		attribute.String("endpoint.name", "MyEndpoint"),
		attribute.String("endpoint.name_full", "MyService.MyEndpoint"),
	}, spans[0].Attributes)
}
