package telemetry

import (
	"context"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
)

type wrappedDDTracer struct {
	trace.Tracer
	tp *wrappedDDTracerProvider
}

// Start method wraps underlying DD span to wrappedDDSpan, see details there.
func (t *wrappedDDTracer) Start(parentCtx context.Context, spanName string, opts ...trace.SpanStartOption) (ctx context.Context, span trace.Span) {
	ctx, span = t.Tracer.Start(parentCtx, spanName, opts...)
	if span != nil {
		if ddspan, ok := span.(ddtrace.Span); ok {
			spanCtx := ddspan.Context()
			ctx = ctxattr.ContextWith(
				ctx,
				attribute.String("dd.trace_id", strconv.FormatUint(spanCtx.TraceID(), 10)),
				attribute.String("dd.span_id", strconv.FormatUint(spanCtx.SpanID(), 10)),
			)

			span = &wrappedDDSpan{Span: span, tp: t.tp}
			ctx = trace.ContextWithSpan(ctx, span)
		}
	}
	return ctx, span
}
