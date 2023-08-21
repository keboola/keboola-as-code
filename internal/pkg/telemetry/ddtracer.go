package telemetry

import (
	"context"
	"go.opentelemetry.io/otel/trace"
)

type wrappedDDTracer struct {
	tracer         trace.Tracer
	tracerProvider trace.TracerProvider
}

func (t *wrappedDDTracer) Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	ctx, span := t.tracer.Start(ctx, spanName, opts...)
	if span != nil {
		span = &wrappedDDSpan{Span: span, tracerProvider: t.tracerProvider}
	}
	return ctx, span
}
