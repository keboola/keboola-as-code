package orchestrator

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const (
	spanName = "keboola.go.task.orchestrator"
)

type orchestrator[T any] struct {
	config Config[T]
	node   *Node
	dist   *distribution.GroupNode
	logger log.Logger
}

func (o orchestrator[T]) start() <-chan error {
	initErrCh := make(chan error, 1)
	o.node.wg.Add(1)
	go func() {
		defer o.node.wg.Done()
		ctx, span := o.node.tracer.Start(o.node.ctx, spanName, trace.WithAttributes(attribute.String("resource.name", o.config.Name)))
		stream := o.config.Source.WatchPrefix.GetAllAndWatch(ctx, o.node.client, o.config.Source.WatchEtcdOps...)
		err := <-stream.SetupConsumer(o.logger.WithComponent("watch.consumer")).
			WithOnClose(func(err error) {
				span.End(&err)
			}).
			WithForEach(func(events []etcdop.WatchEventT[T], header *etcdop.Header, _ bool) {
				for _, event := range events {
					o.startTask(ctx, event)
				}
			}).
			StartConsumer(ctx, o.node.wg)

		// Handle init error
		if err == nil {
			close(initErrCh)
		} else {
			span.End(&err)
			initErrCh <- err
			close(initErrCh)
		}

		// Restart on distribution change and periodically
		distChangeListener := o.dist.OnChangeListener()
		defer distChangeListener.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case events := <-distChangeListener.C:
				stream.Restart(RestartedByDistribution{error: errors.Errorf(`restarted by distribution change: %s`, strhelper.Truncate(events.Messages(), 100, "…"))})
			case <-o.node.clock.After(o.config.Source.RestartInterval):
				stream.Restart(RestartedByTimer{error: errors.New("restarted by timer")})
			}
		}
	}()
	return initErrCh
}

// startTask for the event received from the watched prefix.
func (o orchestrator[T]) startTask(ctx context.Context, event etcdop.WatchEventT[T]) {
	// Check event type
	if event.Type != etcdop.CreateEvent {
		return
	}

	// Generate keys
	taskKey := o.config.TaskKey(event)
	distributionKey := o.config.DistributionKey(event)

	// Error is not expected, there is present always at least one node - self.
	if !o.dist.MustCheckIsOwner(distributionKey) {
		// Another node handles the resource.
		o.logger.Debugf(ctx, `not assigned "%s", distribution key "%s"`, taskKey.String(), distributionKey)
		return
	}

	// Should be the task started?
	if o.config.StartTaskIf != nil {
		if skipCause, start := o.config.StartTaskIf(event); !start {
			o.logger.Debugf(ctx, `skipped "%s", %s`, taskKey.String(), skipCause)
			return
		}
	}

	// Create task handler
	taskFn := o.config.TaskFactory(event)
	if taskFn == nil {
		o.logger.Infof(ctx, `skipped "%s"`, taskKey)
		return
	}

	// Generate lock, if empty, then the lock will be generated from TaskKey in the StartTask method
	var lock string
	if o.config.Lock != nil {
		lock = o.config.Lock(event)
	}

	// Run task in the background
	o.logger.Infof(ctx, `assigned "%s"`, taskKey)
	taskCfg := task.Config{
		Type:      o.config.Name,
		Key:       taskKey,
		Lock:      lock,
		Context:   o.config.TaskCtx,
		Operation: taskFn,
	}
	if _, err := o.node.tasks.StartTask(ctx, taskCfg); err != nil {
		o.logger.Error(ctx, err.Error())
	}
}
