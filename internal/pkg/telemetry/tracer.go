package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

type Tracer interface {
	Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, Span)
}

type tracer struct {
	tracer trace.Tracer
}

func (t *tracer) Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, Span) {
	ctx, s := t.tracer.Start(ctx, spanName, opts...)
	return ctx, &span{span: s}
}
