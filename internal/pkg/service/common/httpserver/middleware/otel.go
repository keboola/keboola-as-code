package middleware

import (
	"context"
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
	attrRequestID      = "http.request_id"
	attrQuery          = "http.query."
	attrRequestHeader  = "http.header."
	attrResponseHeader = "http.response.header."
)

func OpenTelemetry(tp trace.TracerProvider, mp metric.MeterProvider, opts ...OTELOption) Middleware {
	cfg := newOTELConfig(opts)
	return func(next http.Handler) http.Handler {
		h := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := req.Context()

			// Set additional metrics attributes
			labeler, _ := otelhttp.LabelerFromContext(ctx)
			labeler.Add(metricAttrs(req)...)

			// Set additional request attributes
			span := trace.SpanFromContext(ctx)
			span.SetAttributes(spanRequestAttrs(&cfg, req)...)

			// Route and route params must be obtained by the OpenTelemetryRoute middleware registered to httptreemux.Muxer.
			// At this point, we set the list of redacted parameters to the context.
			req = req.WithContext(context.WithValue(req.Context(), redactedRouteParamsCtxKey, cfg.redactedRouteParams))

			// Process request
			next.ServeHTTP(w, req)

			// Set addition response attributes
			span.SetAttributes(spanResponseAttrs(&cfg, w.Header())...)
		})
		return otelhttp.NewHandler(h, SpanName, otelOptions(cfg, tp, mp)...)
	}
}

func otelOptions(cfg otelConfig, tp trace.TracerProvider, mp metric.MeterProvider) []otelhttp.Option {
	out := []otelhttp.Option{otelhttp.WithTracerProvider(tp), otelhttp.WithMeterProvider(mp)}
	for _, f := range cfg.filters {
		otelhttp.WithFilter(f)
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
