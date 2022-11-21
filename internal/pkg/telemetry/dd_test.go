package telemetry

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/mocktracer"

	jsonLib "github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
)

type testError struct {
	msg string
}

func (v testError) Error() string {
	return v.msg
}

// MarshalJSON implementation to show error in test output.
func (v testError) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.msg)
}

func TestOpenTelemetryToDataDogIntegration(t *testing.T) {
	t.Parallel()

	// Start the mock tracer, it set global variable (DD has no other way)
	mockedTracer := mocktracer.Start()
	defer mockedTracer.Stop()

	ctx := context.Background()
	otelTracker := NewDataDogTracer()

	// Start span1
	startTime1, err := time.Parse(time.RFC3339, "2000-01-01T10:10:10Z")
	assert.NoError(t, err)
	ctxSpan1, span1 := otelTracker.Start(
		ctx,
		"my.operation1",
		trace.WithTimestamp(startTime1),
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("resource.name", "my/resource"),
			attribute.String("foo", "bar"),
			attribute.Bool("bool", true),
		),
	)
	assert.Len(t, mockedTracer.OpenSpans(), 1)
	assert.Len(t, mockedTracer.FinishedSpans(), 0)

	// Start span2 - child of span1
	startTime2, err := time.Parse(time.RFC3339, "2000-01-01T10:10:20Z")
	assert.NoError(t, err)
	_, span2 := otelTracker.Start(ctxSpan1, "my.operation1.sub", trace.WithSpanKind(trace.SpanKindClient), trace.WithTimestamp(startTime2))
	assert.Len(t, mockedTracer.OpenSpans(), 2)
	assert.Len(t, mockedTracer.FinishedSpans(), 0)

	// Start span3
	startTime3, err := time.Parse(time.RFC3339, "2000-01-01T10:10:30Z")
	assert.NoError(t, err)
	_, span3 := otelTracker.Start(ctx, "my.operation2", trace.WithTimestamp(startTime3))
	assert.Len(t, mockedTracer.OpenSpans(), 3)
	assert.Len(t, mockedTracer.FinishedSpans(), 0)

	// Event in span3
	eventTime, err := time.Parse(time.RFC3339, "2000-01-01T10:10:35Z")
	assert.NoError(t, err)
	span3.AddEvent("my.event", trace.WithTimestamp(eventTime))
	assert.Len(t, mockedTracer.OpenSpans(), 3)
	assert.Len(t, mockedTracer.FinishedSpans(), 1)

	// End span3 with error, without stacktrace
	endTime3, err := time.Parse(time.RFC3339, "2000-01-01T10:10:40Z")
	assert.NoError(t, err)
	span3.RecordError(&testError{msg: "some span 3 error"})
	span3.End(trace.WithStackTrace(false), trace.WithTimestamp(endTime3))
	assert.Len(t, mockedTracer.OpenSpans(), 2)
	assert.Len(t, mockedTracer.FinishedSpans(), 2)

	// End span2 without error
	endTime2, err := time.Parse(time.RFC3339, "2000-01-01T10:10:50Z")
	assert.NoError(t, err)
	span2.End(trace.WithTimestamp(endTime2))
	assert.Len(t, mockedTracer.OpenSpans(), 1)
	assert.Len(t, mockedTracer.FinishedSpans(), 3)

	// End span1 without error
	endTime1, err := time.Parse(time.RFC3339, "2000-01-01T10:10:55Z")
	assert.NoError(t, err)
	span1.RecordError(&testError{msg: "some span 1 error"})
	span1.End(trace.WithTimestamp(endTime1))
	assert.Len(t, mockedTracer.OpenSpans(), 0)
	assert.Len(t, mockedTracer.FinishedSpans(), 4)

	// Check conversion to OpenTelemetry IDs (counter in mocked tracer starts at 123)
	span1Id := span1.SpanContext().SpanID()
	span1TraceId := span1.SpanContext().TraceID()
	assert.Equal(t, uint64(124), binary.LittleEndian.Uint64(span1Id[:]))
	assert.Equal(t, uint64(124), binary.LittleEndian.Uint64(span1TraceId[:]))
	span2Id := span2.SpanContext().SpanID()
	span2TraceId := span2.SpanContext().TraceID()
	assert.Equal(t, uint64(125), binary.LittleEndian.Uint64(span2Id[:]))
	assert.Equal(t, uint64(124), binary.LittleEndian.Uint64(span2TraceId[:]))
	span3Id := span3.SpanContext().SpanID()
	span3TraceId := span3.SpanContext().TraceID()
	assert.Equal(t, uint64(126), binary.LittleEndian.Uint64(span3Id[:]))
	assert.Equal(t, uint64(126), binary.LittleEndian.Uint64(span3TraceId[:]))

	// Convert spans to string
	var out strings.Builder
	for _, s := range mockedTracer.FinishedSpans() {
		out.WriteString(fmt.Sprintf("name: %v\n", s.OperationName()))
		out.WriteString(fmt.Sprintf("tags: %s\n", jsonLib.MustEncode(s.Tags(), false)))
		out.WriteString(fmt.Sprintf("start: %s\n", s.StartTime()))
		out.WriteString(fmt.Sprintf("finish: %s\n", s.FinishTime()))
		out.WriteString(fmt.Sprintf("spanId: %v\n", s.Context().SpanID()))
		out.WriteString(fmt.Sprintf("parent: %v\n", s.ParentID()))
		out.WriteString(fmt.Sprintf("trace: %v\n", s.Context().TraceID()))
		out.WriteString("-----\n")
	}

	// Assert all spans
	expected := `
name: event.my.event
tags: {"resource.name":"event.my.event","service.name":null}
start: 2000-01-01 10:10:35 +0000 UTC
finish: 2000-01-01 10:10:35 +0000 UTC
spanId: 127
parent: 126
trace: 126
-----
name: my.operation2
tags: {"error":"some span 3 error","error.stack":"\u003cdebug stack disabled\u003e","resource.name":"my.operation2"}
start: 2000-01-01 10:10:30 +0000 UTC
finish: 2000-01-01 10:10:40 +0000 UTC
spanId: 126
parent: 0
trace: 126
-----
name: my.operation1.sub
tags: {"resource.name":"my.operation1.sub","service.name":null,"span.type":"http"}
start: 2000-01-01 10:10:20 +0000 UTC
finish: 2000-01-01 10:10:50 +0000 UTC
spanId: 125
parent: 124
trace: 124
-----
name: my.operation1
tags: {"bool":true,"error":"some span 1 error","foo":"bar","resource.name":"my/resource","span.type":"web"}
start: 2000-01-01 10:10:10 +0000 UTC
finish: 2000-01-01 10:10:55 +0000 UTC
spanId: 124
parent: 0
trace: 124
-----
`
	assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(out.String()))
}
