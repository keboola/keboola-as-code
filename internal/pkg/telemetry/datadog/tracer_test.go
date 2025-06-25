package datadog_test

import (
	"fmt"
	"testing"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/datadog"
)

func TestContextAttributes(t *testing.T) {
	t.Parallel()

	// Setup telemetry
	logger := log.NewDebugLogger()
	tel, err := telemetry.New(
		func() (trace.TracerProvider, error) {
			return datadog.NewTracerProvider(logger, servicectx.NewForTest(t)), nil
		},
		nil,
	)
	require.NoError(t, err)

	// Start mock tracer
	mockTracer := mocktracer.Start()
	defer mockTracer.Stop()

	// Add some common context attribute, it should appear in span and log record
	ctx := t.Context()
	ctx = ctxattr.ContextWith(ctx, attribute.String("foo", "bar"))

	// Create span
	ctx, span := tel.Tracer().Start(ctx, "keboola.go.test")
	span.End(nil)

	// Tracer added TraceID and SpanID attributes to the context
	traceID, ok := ctxattr.Attributes(ctx).Value("dd.trace_id")
	assert.True(t, ok)
	spanID, ok := ctxattr.Attributes(ctx).Value("dd.span_id")
	assert.True(t, ok)

	// Context attributes match the span details
	spans := mockTracer.FinishedSpans()
	assert.Len(t, spans, 1)
	assert.Equal(t, fmt.Sprint(spans[0].TraceID()), traceID.Emit())
	assert.Equal(t, fmt.Sprint(spans[0].SpanID()), spanID.Emit())

	// Span contains common attribute from the context
	assert.Equal(t, "bar", fmt.Sprint(spans[0].Tag("foo")))

	// Log record contains common attribute, TraceID and SpanID
	logger.Info(ctx, "test")
	logger.AssertJSONMessages(t, fmt.Sprintf(
		`{"level":"info","message":"test","foo":"bar","dd.trace_id":"%s","dd.span_id":"%s"}`,
		traceID.Emit(),
		spanID.Emit(),
	))
}
