package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

//nolint:gochecknoglobals
var nopTracer = trace.NewNoopTracerProvider().Tracer("")

type Tracer interface {
	Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, Span)
}

type tracer struct {
	tracer trace.Tracer
}

func (t *tracer) Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, Span) {
	// If the parent span is a nop span, tracer will still create a new root span.
	// So we have to ask the context if tracing is disabled.
	if IsTracingDisabled(ctx) {
		ctx, s := nopTracer.Start(ctx, spanName)
		return ctx, &span{span: s}
	}

	ctx, s := t.tracer.Start(ctx, spanName, opts...)
	return ctx, &span{span: s}
}
