package dependencies

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/event"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/watcher"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const (
	distributionWorkerGroupName = "buffer-worker"
	workerUserAgent             = "keboola-buffer-worker"
)

// workerScope implements WorkerScope interface.
type workerScope struct {
	ServiceScope
	dependencies.DistributionScope
	dependencies.OrchestratorScope
	config      config.WorkerConfig
	watcherNode *watcher.WorkerNode
	eventSender *event.Sender
}

func NewWorkerScope(ctx context.Context, proc *servicectx.Process, cfg config.WorkerConfig, logger log.Logger, tel telemetry.Telemetry) (v WorkerScope, err error) {
	ctx, span := tel.Tracer().Start(ctx, "keboola.go.buffer.api.dependencies.NewWorkerScope")
	defer span.End(&err)
	serviceScp, err := NewServiceScope(ctx, cfg.ServiceConfig, proc, logger, tel, workerUserAgent)
	return newWorkerScope(ctx, cfg, serviceScp)
}

func newWorkerScope(ctx context.Context, cfg config.WorkerConfig, serviceScp ServiceScope) (v WorkerScope, err error) {
	d := &workerScope{}

	d.config = cfg

	d.ServiceScope = serviceScp

	d.DistributionScope, err = dependencies.NewDistributionScope(ctx, cfg.NodeID, distributionWorkerGroupName, d)
	if err != nil {
		return nil, err
	}

	d.OrchestratorScope = dependencies.NewOrchestratorScope(ctx, d)

	d.watcherNode, err = watcher.NewWorkerNode(d)
	if err != nil {
		return nil, err
	}

	d.eventSender = event.NewSender(d)

	return d, nil
}

func (v *workerScope) WorkerConfig() config.WorkerConfig {
	return v.config
}

func (v *workerScope) WatcherWorkerNode() *watcher.WorkerNode {
	return v.watcherNode
}

func (v *workerScope) EventSender() *event.Sender {
	return v.eventSender
}
