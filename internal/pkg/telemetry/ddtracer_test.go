package telemetry

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/mocktracer"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

func TestContextAttributes(t *testing.T) {
	t.Parallel()

	mockTracer := mocktracer.Start()
	defer mockTracer.Stop()

	logger := log.NewDebugLogger()

	proc, err := servicectx.New(servicectx.WithLogger(logger), servicectx.WithUniqueID("mockid"))
	require.NoError(t, err)

	// Setup telemetry
	tel, err := New(
		func() (trace.TracerProvider, error) {
			return NewDDTracerProvider(logger, proc), nil
		},
		nil,
	)
	require.NoError(t, err)

	ctx := context.Background()

	ctx, span := tel.Tracer().Start(ctx, "keboola.go.test")
	defer span.End(nil)

	traceID, ok := ctxattr.Attributes(ctx).Value("dd.trace_id")
	assert.True(t, ok)

	spanID, ok := ctxattr.Attributes(ctx).Value("dd.span_id")
	assert.True(t, ok)

	spans := mockTracer.OpenSpans()
	assert.Len(t, spans, 1)

	assert.Equal(t, fmt.Sprint(spans[0].TraceID()), traceID.Emit())
	assert.Equal(t, fmt.Sprint(spans[0].SpanID()), spanID.Emit())

	logger.Info(ctx, "test")

	expected := `{"level":"info","message":"test","dd.trace_id":"%s","dd.span_id":"%s"}`
	log.AssertJSONMessages(t, expected, logger.AllMessages())
}
