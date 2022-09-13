package telemetry

import (
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
)

const HTTPMethod = ext.HTTPMethod
const HTTPURL = ext.HTTPURL

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
