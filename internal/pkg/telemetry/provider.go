package telemetry

import (
	"go.opentelemetry.io/otel/trace"
)

type TracerProvider struct {
	tracer trace.Tracer
}

func NewTracerProvider(tracer trace.Tracer) trace.TracerProvider {
	return &TracerProvider{tracer: tracer}
}

func (p *TracerProvider) Tracer(_ string, _ ...trace.TracerOption) trace.Tracer {
	return p.tracer
}
