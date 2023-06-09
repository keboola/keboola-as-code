package telemetry

import (
	"go.opentelemetry.io/otel/trace"
)

type singleTracerProvider struct {
	tracer trace.Tracer
}

// WrapDD is a workaround for DataDog OpenTelemetry tracer.
// DataDog restarts a global tracer on each TracerProvider.Tracer() call, which is not what we want.
// In the DataDog library there is no concept (internally yes, but not publicly) of tracer instance,
// everything is handled globally.
func WrapDD(tracer trace.Tracer) trace.TracerProvider {
	return &singleTracerProvider{tracer: &wrappedDDTracer{tracer: tracer}}
}

func (p *singleTracerProvider) Tracer(_ string, _ ...trace.TracerOption) trace.Tracer {
	return p.tracer
}
