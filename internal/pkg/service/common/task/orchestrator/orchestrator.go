package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

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

// Orchestrator creates a task for each watch event, but only on one worker node in the cluster.
// Decision is made by the distribution.Assigner.
// See documentation of: distribution.Node, task.Node, Config[R].
type orchestrator[T any] struct {
	clock  clock.Clock
	logger log.Logger
	tracer telemetry.Tracer
	client *etcd.Client
	dist   *distribution.Node
	tasks  *task.Node
	config Config[T]
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	EtcdClient() *etcd.Client
	DistributionWorkerNode() *distribution.Node
	TaskNode() *task.Node
}

func Start[T any](ctx context.Context, wg *sync.WaitGroup, d dependencies, config Config[T]) <-chan error {
	if err := config.Validate(); err != nil {
		panic(err)
	}

	o := &orchestrator[T]{
		clock:  d.Clock(),
		logger: d.Logger().AddPrefix(fmt.Sprintf("[orchestrator][%s]", config.Name)),
		tracer: d.Telemetry().Tracer(),
		client: d.EtcdClient(),
		dist:   d.DistributionWorkerNode(),
		tasks:  d.TaskNode(),
		config: config,
	}

	// Delete events are not needed/ignored
	o.config.Source.WatchEtcdOps = append(o.config.Source.WatchEtcdOps, etcd.WithFilterDelete())

	return o.start(ctx, wg)
}

func (o orchestrator[R]) start(ctx context.Context, wg *sync.WaitGroup) <-chan error {
	initDone := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer o.logger.Info("stopped")

		initDone := initDone
		b := newRetryBackoff()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				// The watcher is periodically restarted to rescan existing keys.
				if initDone == nil {
					o.logger.Debug("restart")
				}

				// Run the watch operation for the RestartInterval.
				err := o.watch(ctx, wg, o.config.Source.RestartInterval, func() {
					if initDone != nil {
						// Initialization was successful
						o.logger.Info("ready")
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
					o.logger.Warnf("re-creating watcher, backoff delay %s, reason: %s", delay, err.Error())
					<-time.After(delay)
				}
			}
		}
	}()
	return initDone
}

func (o orchestrator[R]) watch(ctx context.Context, wg *sync.WaitGroup, timeout time.Duration, onReady func()) error {
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ctx, span := o.node.tracer.Start(ctx, spanName, trace.WithAttributes(attribute.String("resource.name", o.config.Name)))
	err := <-o.config.Source.WatchPrefix.
		GetAllAndWatch(ctx, o.client, o.config.Source.WatchEtcdOps...).
		SetupConsumer(o.logger).
		WithOnClose(func(err error) {
			span.End(&err)
			close(done)
		}).
		WithForEach(func(events []etcdop.WatchEventT[R], header *etcdop.Header, _ bool) {
			for _, event := range events {
				o.startTask(event)
			}
		}).
		StartConsumer(wg)
	if err != nil {
		return err
	}

	// Wait for the consumer to finish.
	onReady()
	select {
	case <-done:
		return nil
	case <-o.clock.After(timeout):
		cancel()
		<-done
		return nil
	}
}

// startTask for the event received from the watched prefix.
func (o orchestrator[R]) startTask(event etcdop.WatchEventT[R]) {
	// Check event type
	if event.Type != etcdop.CreateEvent {
		return
	}

	// Generate keys
	taskKey := o.config.TaskKey(event)
	distributionKey := o.config.DistributionKey(event)

	// Error is not expected, there is present always at least one node - self.
	if !o.dist.MustCheckIsOwner(distributionKey) {
		// Another worker node handles the resource.
		o.logger.Debugf(`not assigned "%s", distribution key "%s"`, taskKey.String(), distributionKey)
		return
	}

	// Should be the task started?
	if o.config.StartTaskIf != nil {
		if skipReason, start := o.config.StartTaskIf(event); !start {
			o.logger.Debugf(`skipped "%s", %s`, taskKey.String(), skipReason)
			return
		}
	}

	// Create task handler
	taskFn := o.config.TaskFactory(event)
	if taskFn == nil {
		o.logger.Infof(`skipped "%s"`, taskKey)
		return
	}

	// Generate lock, if empty, then the lock will be generated from TaskKey in the StartTask method
	var lock string
	if o.config.Lock != nil {
		lock = o.config.Lock(event)
	}

	// Run task in the background
	o.logger.Infof(`assigned "%s"`, taskKey)
	taskCfg := task.Config{
		Type:      o.config.Name,
		Key:       taskKey,
		Lock:      lock,
		Context:   o.config.TaskCtx,
		Operation: taskFn,
	}
	if _, err := o.tasks.StartTask(taskCfg); err != nil {
		o.logger.Error(err)
	}
}
