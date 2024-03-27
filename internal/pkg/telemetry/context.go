package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

// ContextWithSpan returns a copy of parent with span set as the current Span.
func ContextWithSpan(ctx context.Context, s Span) context.Context {
	if s, ok := s.(*span); ok {
		ctx = trace.ContextWithSpan(ctx, s.span)
	}
	return ctx
}

// SpanFromContext returns the current Span from ctx.
func SpanFromContext(ctx context.Context) Span {
	return &span{span: trace.SpanFromContext(ctx)}
}
