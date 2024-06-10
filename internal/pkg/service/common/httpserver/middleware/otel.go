package middleware

import (
	"context"
	"net/http"
	"sort"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	SpanName           = "http.server.request"
	RequestCtxKey      = ctxKey("request")
	RequestSpanCtxKey  = ctxKey("request-span")
	attrRequestID      = "http.request_id"
	attrQuery          = "http.query."
	attrRequestHeader  = "http.header."
	attrResponseHeader = "http.response.header."
	// Extra attributes for DataDog.
	attrSpanKind            = "span.kind"
	attrSpanKindValueServer = "server"
	attrSpanType            = "span.type"
	attrSpanTypeValueServer = "web"
)

func OpenTelemetry(tp trace.TracerProvider, mp metric.MeterProvider, cfg Config) Middleware {
	return func(next http.Handler) http.Handler {
		h := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if isTelemetryDisabled(req) {
				next.ServeHTTP(w, req)
				return
			}

			ctx := req.Context()
			span := trace.SpanFromContext(ctx)

			// Set additional request attributes
			span.SetAttributes(spanRequestAttrs(&cfg, req)...)
			ctx = context.WithValue(ctx, RequestCtxKey, req)
			ctx = context.WithValue(ctx, RequestSpanCtxKey, span)

			// Route and route params must be obtained by the OpenTelemetryRoute middleware registered to httptreemux.Muxer.
			// At this point, we set the list of redacted parameters to the context.
			ctx = context.WithValue(ctx, redactedRouteParamsCtxKey, cfg.redactedRouteParams)

			// Process request
			next.ServeHTTP(w, req.WithContext(ctx))

			// Set addition response attributes
			span.SetAttributes(spanResponseAttrs(&cfg, w.Header())...)
		})
		return otelhttp.NewHandler(h, SpanName, otelOptions(cfg, tp, mp)...)
	}
}

func Request(ctx context.Context) (*http.Request, bool) {
	v, ok := ctx.Value(RequestCtxKey).(*http.Request)
	return v, ok
}

func RequestSpan(ctx context.Context) (trace.Span, bool) {
	v, ok := ctx.Value(RequestSpanCtxKey).(trace.Span)
	return v, ok
}

func otelOptions(cfg Config, tp trace.TracerProvider, mp metric.MeterProvider) []otelhttp.Option {
	out := []otelhttp.Option{
		otelhttp.WithTracerProvider(tp),
		otelhttp.WithMeterProvider(mp),
		otelhttp.WithFilter(func(req *http.Request) bool {
			return !isTelemetryDisabled(req)
		}),
	}
	if cfg.propagators != nil {
		out = append(out, otelhttp.WithPropagators(cfg.propagators))
	}
	return out
}

func spanRequestAttrs(cfg *Config, req *http.Request) (out []attribute.KeyValue) {
	// Request ID
	requestID, _ := req.Context().Value(RequestIDCtxKey).(string)
	out = append(out, attribute.String(attrRequestID, requestID))

	// Mark the span as HTTP server request
	out = append(
		out,
		attribute.String(attrSpanKind, attrSpanKindValueServer),
		attribute.String(attrSpanType, attrSpanTypeValueServer),
	)

	// Query params
	{
		var attrs []attribute.KeyValue
		for key, values := range req.URL.Query() {
			value := strings.Join(values, ";")
			if _, found := cfg.redactedQueryParams[strings.ToLower(key)]; found {
				value = maskedValue
			}
			attrs = append(attrs, attribute.String(attrQuery+key, value))
		}
		sort.SliceStable(attrs, func(i, j int) bool {
			return attrs[i].Key < attrs[j].Key
		})
		out = append(out, attrs...)
	}

	// Headers
	{
		var attrs []attribute.KeyValue
		for key, values := range req.Header {
			key = strings.ToLower(key)
			value := strings.Join(values, ";")
			if key == "user-agent" {
				// Skip, it is already present from otelhttp
				continue
			}
			if _, found := cfg.redactedHeaders[key]; found {
				value = maskedValue
			}
			attrs = append(attrs, attribute.String(attrRequestHeader+key, value))
		}
		sort.SliceStable(attrs, func(i, j int) bool {
			return attrs[i].Key < attrs[j].Key
		})
		out = append(out, attrs...)
	}

	return out
}

func spanResponseAttrs(cfg *Config, header http.Header) (out []attribute.KeyValue) {
	// Headers
	{
		var attrs []attribute.KeyValue
		for key, values := range header {
			key = strings.ToLower(key)
			value := strings.Join(values, ";")
			if _, found := cfg.redactedHeaders[key]; found {
				value = maskedValue
			}
			attrs = append(attrs, attribute.String(attrResponseHeader+key, value))
		}
		sort.SliceStable(attrs, func(i, j int) bool {
			return attrs[i].Key < attrs[j].Key
		})
		out = append(out, attrs...)
	}
	return out
}
