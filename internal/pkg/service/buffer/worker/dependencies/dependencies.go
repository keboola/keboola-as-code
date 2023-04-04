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
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/event"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/watcher"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// ForWorker interface provides dependencies for Buffer Worker.
// The container exists during the entire run of the Worker.
type ForWorker interface {
	serviceDependencies.ForService
	WorkerConfig() config.Config
	DistributionWorkerNode() *distribution.Node
	WatcherWorkerNode() *watcher.WorkerNode
	TaskNode() *task.Node
	EventSender() *event.Sender
}

// forWorker implements ForWorker interface.
type forWorker struct {
	serviceDependencies.ForService
	config      config.Config
	distNode    *distribution.Node
	watcherNode *watcher.WorkerNode
	taskNode    *task.Node
	eventSender *event.Sender
}

func NewWorkerDeps(ctx context.Context, proc *servicectx.Process, cfg config.Config, envs env.Provider, logger log.Logger) (v ForWorker, err error) {
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
	serviceDeps, err := serviceDependencies.NewServiceDeps(ctx, proc, tracer, cfg.ServiceConfig, envs, logger, userAgent)
	if err != nil {
		return nil, err
	}

	// Create worker dependencies
	d := &forWorker{
		ForService: serviceDeps,
		config:     cfg,
	}

	d.distNode, err = distribution.NewNode(d)
	if err != nil {
		return nil, err
	}

	d.watcherNode, err = watcher.NewWorkerNode(d)

	d.taskNode, err = task.NewNode(d)
	if err != nil {
		return nil, err
	}

	d.eventSender = event.NewSender(logger)

	return d, nil
}

func (v *forWorker) WorkerConfig() config.Config {
	return v.config
}

func (v *forWorker) DistributionWorkerNode() *distribution.Node {
	return v.distNode
}

func (v *forWorker) WatcherWorkerNode() *watcher.WorkerNode {
	return v.watcherNode
}

func (v *forWorker) TaskNode() *task.Node {
	return v.taskNode
}

func (v *forWorker) EventSender() *event.Sender {
	return v.eventSender
}
