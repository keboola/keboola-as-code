package telemetry

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Span interface {
	End(errPtr *error, opts ...trace.SpanEndOption)
	SetAttributes(kv ...attribute.KeyValue)
}

type span struct {
	span trace.Span
}

func (s *span) SetAttributes(kv ...attribute.KeyValue) {
	s.span.SetAttributes(kv...)
}

func (s *span) End(errPtr *error, opts ...trace.SpanEndOption) {
	if errPtr != nil {
		err := *errPtr
		if err != nil {
			s.span.RecordError(err)
			s.span.SetStatus(codes.Error, err.Error())
		} else {
			s.span.SetStatus(codes.Ok, "")
		}
	}
	s.span.End(opts...)
}
