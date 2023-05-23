package telemetry

import (
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

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
