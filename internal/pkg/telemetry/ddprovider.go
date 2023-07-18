package telemetry

import (
	octrace "go.opencensus.io/trace"
	"go.opentelemetry.io/otel/bridge/opencensus"
	"go.opentelemetry.io/otel/trace"
	ddotel "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/opentelemetry"
	ddTracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

type singleTracerProvider struct {
	tracer trace.Tracer
}

// NewDDTracerProvider - see wrapDDTracerProvider.
func NewDDTracerProvider(logger log.Logger, proc *servicectx.Process, opts ...ddTracer.StartOption) trace.TracerProvider {
	opts = append(opts, ddTracer.WithLogger(NewDDLogger(logger)))
	wrappedTp := ddotel.NewTracerProvider(opts...)
	proc.OnShutdown(func() {
		if err := wrappedTp.Shutdown(); err != nil {
			logger.Error(err)
		}
	})

	// Wrap a tracer instance that will be used everywhere
	tp := wrapDDTracerProvider(wrappedTp.Tracer(""))

	// Register legacy OpenCensus tracing for go-cloud (https://github.com/google/go-cloud/issues/2877).
	octrace.DefaultTracer = opencensus.NewTracer(tp.Tracer("opencensus-bridge"))

	return tp
}

// wrapDDTracerProvider is a workaround for DataDog OpenTelemetry tracer.
// DataDog restarts a global tracer on each TracerProvider.Tracer() call, which is not what we want.
// In the DataDog library there is no concept (internally yes, but not publicly) of tracer instance,
// everything is handled globally.
func wrapDDTracerProvider(tracer trace.Tracer) trace.TracerProvider {
	tp := &singleTracerProvider{}
	tc := &wrappedDDTracer{tracer: tracer, tracerProvider: tp}
	tp.tracer = tc
	return tp
}

func (p *singleTracerProvider) Tracer(_ string, _ ...trace.TracerOption) trace.Tracer {
	return p.tracer
}
