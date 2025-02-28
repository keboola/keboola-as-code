package telemetry_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/codes"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

func TestContextWithSpan(t *testing.T) {
	t.Parallel()
	tel := telemetry.NewForTest(t)

	// Start span
	_, span1 := tel.Tracer().Start(t.Context(), "my.span")

	// Test ContextWithSpan
	ctx2 := t.Context()
	ctx2 = telemetry.ContextWithSpan(ctx2, span1)
	ctx2, span2 := tel.Tracer().Start(ctx2, "my.sub.span")

	// Test SpanFromContext
	span2Copy := telemetry.SpanFromContext(ctx2)
	ctx3 := t.Context()
	ctx3 = telemetry.ContextWithSpan(ctx3, span2Copy)
	_, span3 := tel.Tracer().Start(ctx3, "my.sub.sub.span")

	// Close spans
	span3.End(nil)
	span2.End(nil)
	span1.End(nil)

	// Span2 is a child of the span 1
	tel.AssertSpans(t, tracetest.SpanStubs{
		{
			Name:     "my.span",
			SpanKind: trace.SpanKindInternal,
			SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    tel.TraceID(1),
				SpanID:     tel.SpanID(1),
				TraceFlags: trace.FlagsSampled,
			}),
			Status:         tracesdk.Status{Code: codes.Unset},
			Parent:         trace.SpanContext{},
			ChildSpanCount: 1,
		},
		{
			Name:     "my.sub.span",
			SpanKind: trace.SpanKindInternal,
			SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    tel.TraceID(1),
				SpanID:     tel.SpanID(2),
				TraceFlags: trace.FlagsSampled,
			}),
			Status: tracesdk.Status{Code: codes.Unset},
			Parent: trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    tel.TraceID(1),
				SpanID:     tel.SpanID(1),
				TraceFlags: trace.FlagsSampled,
			}),
			ChildSpanCount: 1,
		},
		{
			Name:     "my.sub.sub.span",
			SpanKind: trace.SpanKindInternal,
			SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    tel.TraceID(1),
				SpanID:     tel.SpanID(3),
				TraceFlags: trace.FlagsSampled,
			}),
			Status: tracesdk.Status{Code: codes.Unset},
			Parent: trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    tel.TraceID(1),
				SpanID:     tel.SpanID(2),
				TraceFlags: trace.FlagsSampled,
			}),
			ChildSpanCount: 0,
		},
	})
}
