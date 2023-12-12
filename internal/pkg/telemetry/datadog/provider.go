package datadog

import (
	"sync"

	octrace "go.opencensus.io/trace"
	"go.opentelemetry.io/otel/bridge/opencensus"
	"go.opentelemetry.io/otel/trace"
	ddotel "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/opentelemetry"
	ddTracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

type wrappedDDTracerProvider struct {
	*ddotel.TracerProvider
	lock    *sync.Mutex
	tracers map[string]trace.Tracer
}

// NewTracerProvider - see wrapDDTracerProvider.
func NewTracerProvider(logger log.Logger, proc *servicectx.Process, opts ...ddTracer.StartOption) trace.TracerProvider {
	opts = append(opts, ddTracer.WithLogger(NewDDLogger(logger)))
	tp := &wrappedDDTracerProvider{
		TracerProvider: ddotel.NewTracerProvider(opts...),
		lock:           &sync.Mutex{},
		tracers:        make(map[string]trace.Tracer),
	}
	proc.OnShutdown(func() {
		if err := tp.Shutdown(); err != nil {
			logger.Error(err)
		}
	})

	// Register legacy OpenCensus tracing for go-cloud (https://github.com/google/go-cloud/issues/2877).
	octrace.DefaultTracer = opencensus.NewTracer(tp.Tracer("opencensus-bridge"))

	return tp
}

func (p *wrappedDDTracerProvider) Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	p.lock.Lock()
	defer p.lock.Unlock()

	v, ok := p.tracers[name]
	if !ok {
		v = &wrappedDDTracer{Tracer: p.TracerProvider.Tracer(name, opts...), tp: p}
		p.tracers[name] = v
	}

	return v
}
