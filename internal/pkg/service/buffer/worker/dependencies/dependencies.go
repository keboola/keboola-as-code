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

	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	serviceDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// ForWorker interface provides dependencies for Buffer Worker.
// The container exists during the entire run of the Worker.
type ForWorker interface {
	serviceDependencies.ForService
}

// forWorker implements ForWorker interface.
type forWorker struct {
	serviceDependencies.ForService
}

func NewWorkerDeps(ctx context.Context, proc *servicectx.Process, envs env.Provider, logger log.Logger, debug, dumpHTTP bool) (v ForWorker, err error) {
	// Create tracer
	var tracer trace.Tracer = nil
	if telemetry.IsDataDogEnabled(envs) {
		var span trace.Span
		tracer = telemetry.NewDataDogTracer()
		ctx, span = tracer.Start(ctx, "keboola.go.buffer.worker.dependencies.NewWorkerDeps")
		defer telemetry.EndSpan(span, &err)
	} else {
		tracer = telemetry.NewNopTracer()
	}

	// Create service dependencies
	userAgent := "keboola-buffer-worker"
	serviceDeps, err := serviceDependencies.NewServiceDeps(ctx, proc, tracer, envs, logger, debug, dumpHTTP, userAgent)
	if err != nil {
		return nil, err
	}

	// Create server dependencies
	d := &forWorker{
		ForService: serviceDeps,
	}

	return d, nil
}
