package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

const disabledTracingCtxKey = ctxKey("disabled-tracing")

//nolint:gochecknoglobals
var nopTracer = trace.NewNoopTracerProvider().Tracer("")

// contextTracer wraps trace.Tracer and adds the option to turn off tracing based on the context, see IsTracingDisabled function.
// In the OpenTelemetry it is not implemented for Go yet: https://github.com/open-telemetry/opentelemetry-specification/issues/530
type contextTracer struct {
	wrapped trace.Tracer
}

// contextTracerProvider - see contextTracer.
type contextTracerProvider struct {
	wrapped trace.TracerProvider
}

// newContextTracerProvider - see contextTracer.
func newContextTracerProvider(p trace.TracerProvider) trace.TracerProvider {
	return &contextTracerProvider{wrapped: p}
}

func (p *contextTracerProvider) Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	return &contextTracer{wrapped: p.wrapped.Tracer(name, opts...)}
}

func (t *contextTracer) Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if IsTracingDisabled(ctx) {
		return nopTracer.Start(ctx, spanName, opts...)
	}
	return t.wrapped.Start(ctx, spanName, opts...)
}

func ContextWithDisabledTracing(ctx context.Context) context.Context {
	return context.WithValue(ctx, disabledTracingCtxKey, true)
}

func IsTracingDisabled(ctx context.Context) bool {
	v, _ := ctx.Value(disabledTracingCtxKey).(bool)
	return v
}
