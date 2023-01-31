package telemetry

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	ddext "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
)

func SampleRate(v float64) attribute.KeyValue {
	return attribute.Float64(ddext.EventSampleRate, v)
}

func KeepSpan() attribute.KeyValue {
	return attribute.Bool(ddext.ManualKeep, true)
}

func DropSpan() attribute.KeyValue {
	return attribute.Bool(ddext.ManualDrop, true)
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
