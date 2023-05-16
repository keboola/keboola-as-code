package middleware

import (
	"context"
	"go.opentelemetry.io/otel/propagation"
	"net/http"
	"sort"
	"strings"

	"github.com/dimfeld/httptreemux/v5"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.18.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	SpanName           = "http.server.request"
	requestSpanCtxKey  = ctxKey("request-span")
	attrRequestID      = "http.request_id"
	attrQuery          = "http.query."
	attrRequestHeader  = "http.header."
	attrResponseHeader = "http.response.header."
	// extra attributes for DataDog
	attrManualDrop          = "manual.drop"
	attrSpanKind            = "span.kind"
	attrSpanKindValueServer = "server"
	attrSpanType            = "span.type"
	attrSpanTypeValueServer = "web"
)

func OpenTelemetry(tp trace.TracerProvider, mp metric.MeterProvider, opts ...OTELOption) Middleware {
	cfg := newOTELConfig(opts)
	tracer := tp.Tracer("otel-middleware")
	return func(next http.Handler) http.Handler {
		h := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := req.Context()
			span := trace.SpanFromContext(ctx)

			// Create dropped span for filtered request, so child spans won't appear in the telemetry too.
			if !span.IsRecording() {
				ctx, span = tracer.Start(ctx, SpanName, trace.WithAttributes(attribute.Bool(attrManualDrop, true)))
				next.ServeHTTP(w, req.WithContext(ctx))
				span.End()
				return
			}

			// Set additional metrics attributes
			labeler, _ := otelhttp.LabelerFromContext(ctx)
			labeler.Add(metricAttrs(req)...)

			// Set additional request attributes
			span.SetAttributes(spanRequestAttrs(&cfg, req)...)
			ctx = context.WithValue(ctx, requestSpanCtxKey, span)

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

func RequestSpan(ctx context.Context) (trace.Span, bool) {
	v, ok := ctx.Value(requestSpanCtxKey).(trace.Span)
	return v, ok
}

func otelOptions(cfg otelConfig, tp trace.TracerProvider, mp metric.MeterProvider) []otelhttp.Option {
	out := []otelhttp.Option{
		otelhttp.WithTracerProvider(tp),
		otelhttp.WithMeterProvider(mp),
		otelhttp.WithPropagators(propagation.TraceContext{}),
	}
	for _, f := range cfg.filters {
		out = append(out, otelhttp.WithFilter(f))
	}
	return out
}

func metricAttrs(req *http.Request) (out []attribute.KeyValue) {
	// Route
	if routerData := httptreemux.ContextData(req.Context()); routerData != nil {
		out = append(out, semconv.HTTPRoute(routerData.Route()))
	}
	return out
}

func spanRequestAttrs(cfg *otelConfig, req *http.Request) (out []attribute.KeyValue) {
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

func spanResponseAttrs(cfg *otelConfig, header http.Header) (out []attribute.KeyValue) {
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
