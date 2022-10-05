package telemetry

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	ddext "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
)

const (
	HTTPMethod = ddext.HTTPMethod
	HTTPURL    = ddext.HTTPURL
	SampleRate = ddext.EventSampleRate
)

// KeepSpan by tracer retention policy.
func KeepSpan() attribute.KeyValue {
	return attribute.Float64(SampleRate, 1.0)
}

func EndSpan(span trace.Span, errPtr *error) {
	if errPtr != nil {
		err := *errPtr
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "OK")
		}
	}
	span.End()
}
