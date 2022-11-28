// Package dependencies provides dependencies for Buffer Worker.
//
// # Dependency Container
//
// This package extends:
//   - common dependencies from  [pkg/github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies].
//   - service dependencies from [pkg/github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies].
package dependencies

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	serviceDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// ForWorker interface provides dependencies for Buffer Worker.
// The container exists during the entire run of the Worker.
type ForWorker interface {
	serviceDependencies.ForService
	WorkerCtx() context.Context
	WorkerWaitGroup() *sync.WaitGroup
}

// forWorker implements ForWorker interface.
type forWorker struct {
	serviceDependencies.ForService
	workerCtx context.Context
	workerWg  *sync.WaitGroup
}

func NewWorkerDeps(workerCtx context.Context, envs env.Provider, logger log.PrefixLogger, debug, dumpHTTP bool) (v ForWorker, err error) {
	// Create tracer
	ctx := workerCtx
	var tracer trace.Tracer = nil
	if telemetry.IsDataDogEnabled(envs) {
		var span trace.Span
		tracer = telemetry.NewDataDogTracer()
		ctx, span = tracer.Start(ctx, "keboola.go.buffer.worker.dependencies.NewWorkerDeps")
		defer telemetry.EndSpan(span, &err)
	} else {
		tracer = telemetry.NewNopTracer()
	}

	// Create wait group - for graceful shutdown
	workerWg := &sync.WaitGroup{}

	// Create service dependencies
	userAgent := "keboola-buffer-worker"
	serviceDeps, err := serviceDependencies.NewServiceDeps(workerCtx, ctx, workerWg, tracer, envs, logger, debug, dumpHTTP, userAgent)
	if err != nil {
		return nil, err
	}

	// Create server dependencies
	d := &forWorker{
		ForService: serviceDeps,
		workerCtx:  workerCtx,
		workerWg:   workerWg,
	}

	return d, nil
}

func (v *forWorker) WorkerCtx() context.Context {
	return v.workerCtx
}

func (v *forWorker) WorkerWaitGroup() *sync.WaitGroup {
	return v.workerWg
}
