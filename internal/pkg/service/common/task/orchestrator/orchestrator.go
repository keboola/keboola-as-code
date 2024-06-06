package orchestrator

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
	initDone := make(chan error, 1)
	o.node.wg.Add(1)
	go func() {
		defer o.node.wg.Done()
		defer o.logger.Info(o.node.ctx, "stopped")
		b := newRetryBackoff()

		for {
			select {
			case <-o.node.ctx.Done():
				return
			default:
				ctx, span := o.node.tracer.Start(o.node.ctx, spanName, trace.WithAttributes(attribute.String("resource.name", o.config.Name)))

				// The watcher is periodically restarted to rescan existing keys.
				if initDone == nil {
					o.logger.Debug(ctx, "restart")
				}

				// Run the watch operation for the RestartInterval.
				err := o.watch(ctx, span, o.config.Source.RestartInterval, func() {
					if initDone != nil {
						// Initialization was successful
						o.logger.Info(ctx, "ready")
						close(initDone)
						initDone = nil
					}
				})

				// Handle initialization error for the watcher.
				if err == nil {
					// No error, reset backoff.
					b.Reset()
				} else {
					if initDone != nil {
						// Initialization error in the first iteration is fatal, e.g., connection failed, stop.
						initDone <- err
						close(initDone)
						return
					} else if errors.Is(err, context.Canceled) {
						return
					}

					// An error occurred, wait before reset.
					delay := b.NextBackOff()
					o.logger.Warnf(ctx, "re-creating watcher, backoff delay %s, reason: %s", delay, err.Error())
					<-time.After(delay)
				}
			}
		}
	}()
	return initDone
}

func (o orchestrator[T]) watch(ctx context.Context, span telemetry.Span, timeout time.Duration, onReady func()) error {
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	err := <-o.config.Source.WatchPrefix.
		GetAllAndWatch(ctx, o.node.client, o.config.Source.WatchEtcdOps...).
		SetupConsumer(o.logger).
		WithOnClose(func(err error) {
			span.End(&err)
			close(done)
		}).
		WithForEach(func(events []etcdop.WatchEventT[T], header *etcdop.Header, _ bool) {
			for _, event := range events {
				o.startTask(ctx, event)
			}
		}).
		StartConsumer(ctx, o.node.wg)
	if err != nil {
		return err
	}

	// Wait for the consumer to finish.
	onReady()
	select {
	case <-done:
		return nil
	case <-o.node.clock.After(timeout):
		cancel()
		<-done
		return nil
	}
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
		if skipReason, start := o.config.StartTaskIf(event); !start {
			o.logger.Debugf(ctx, `skipped "%s", %s`, taskKey.String(), skipReason)
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
