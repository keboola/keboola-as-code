package datadog

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

type wrappedDDTracer struct {
	trace.Tracer
	tp *wrappedDDTracerProvider
}

// Start method wraps underlying DD span to wrappedDDSpan, see details there.
func (t *wrappedDDTracer) Start(parentCtx context.Context, spanName string, opts ...trace.SpanStartOption) (ctx context.Context, span trace.Span) {
	ctx, span = t.Tracer.Start(parentCtx, spanName, opts...)
	if span != nil {
		span = &wrappedDDSpan{Span: span, tp: t.tp}
		ctx = trace.ContextWithSpan(ctx, span)
	}
	return ctx, span
}
