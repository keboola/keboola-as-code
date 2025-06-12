package datadog

import (
	"context"
	"strconv"

	ddtracer "github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
)

type wrappedDDTracer struct {
	trace.Tracer
	tp *wrappedDDTracerProvider
}

// Start method wraps underlying DD span to wrappedDDSpan, see details there.
func (t *wrappedDDTracer) Start(parentCtx context.Context, spanName string, opts ...trace.SpanStartOption) (ctx context.Context, span trace.Span) {
	// Create span
	ctx, span = t.Tracer.Start(parentCtx, spanName, opts...)
	if span == nil {
		return ctx, span
	}

	// Add TraceID and SpanID attributes for logs
	if ddspan, ok := ddtracer.SpanFromContext(ctx); ok {
		ddCtx := ddspan.Context()
		ctx = ctxattr.ContextWith(
			ctx,
			attribute.String("dd.trace_id", strconv.FormatUint(ddCtx.TraceIDLower(), 10)),
			attribute.String("dd.span_id", strconv.FormatUint(ddCtx.SpanID(), 10)),
		)

		span = &wrappedDDSpan{Span: span, tp: t.tp}
		ctx = trace.ContextWithSpan(ctx, span)
	}

	// Append attributes from the context
	if attrs := ctxattr.Attributes(ctx).ToSlice(); len(attrs) > 0 {
		span.SetAttributes(attrs...)
	}

	return ctx, span
}
