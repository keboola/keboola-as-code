package datadog

import (
	"context"
	"sync"

	ddotel "github.com/DataDog/dd-trace-go/v2/ddtrace/opentelemetry"
	ddTracer "github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"go.opentelemetry.io/otel/bridge/opencensus"
	"go.opentelemetry.io/otel/trace"

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
	proc.OnShutdown(func(ctx context.Context) {
		if err := tp.Shutdown(); err != nil {
			logger.Error(ctx, err.Error())
		}
	})

	// Register legacy OpenCensus tracing for go-cloud (https://github.com/google/go-cloud/issues/2877).
	opencensus.InstallTraceBridge(opencensus.WithTracerProvider(tp))

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
