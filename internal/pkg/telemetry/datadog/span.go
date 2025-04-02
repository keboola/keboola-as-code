package datadog

import (
	"go.opentelemetry.io/otel/trace"
)

// wrappedDDSpan to override RecordError.
type wrappedDDSpan struct {
	trace.Span
	tp *wrappedDDTracerProvider
}

func (s *wrappedDDSpan) TracerProvider() trace.TracerProvider {
	return s.tp
}

func (s *wrappedDDSpan) RecordError(err error, _ ...trace.EventOption) {
	s.SetAttributes(ErrorAttrs(err)...)
}
