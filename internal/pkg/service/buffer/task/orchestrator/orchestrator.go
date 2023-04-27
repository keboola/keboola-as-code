// Package orchestrator combines the distribution.Node and the task.Node,
// to run a task only on one node in the cluster, as a reaction to a watch event.
package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	SpanNamePrefix = "keboola.go.buffer.orchestrator"
)

// Config configures the orchestrator.
//
// The orchestrator watches for all Source.WatchEvents in the etcd Source.WatchPrefix.
// For each event, the TaskFactory is invoked.
// If you want to handle only an event type, use StartTaskIf, see below.
//
// The TaskFactory is invoked only if the generated DistributionKey is assigned to the orchestrator.Node.
// All events are received by all nodes, but each node decides
// whether the DistributionKey is assigned to it and task should be started or not.
//
// Each orchestrator.Node in the cluster contains the distribution.Node instance
// Nodes are synchronized, so they make the same decisions.
// Decision is made by the distribution.Assigner.
// Any short-term difference can lead to task duplication, but it can be prevented by the task locks.
//
// The watched Source.WatchPrefix is rescanned at Source.ReSyncInterval and when there is any change in workers distribution.
// On the rescan, the existing tasks are NOT cancelled.
// Task duplication is prevented by the task locks.
//
// The TaskFactory may return nil, then the event will be ignored.
type Config[T any] struct {
	// Name is orchestrator name used by logger.
	Name string
	// Source triggers new tasks.
	Source Source[T]
	// DistributionKey determines which worker processes the task. See distribution package.
	DistributionKey func(event etcdop.WatchEventT[T]) string
	// Lock, only one task with the lock can run at a time.
	// If it is not set, the TaskKey is used as the lock name.
	Lock func(event etcdop.WatchEventT[T]) string
	// TaskKey defines etcd prefix where the task will be stored in etcd.
	// CreatedAt datetime and a random suffix are always appended to the TaskID.
	TaskKey func(event etcdop.WatchEventT[T]) task.Key
	// StartTaskIf, if set, it determines whether the task is started or not
	StartTaskIf func(event etcdop.WatchEventT[T]) (skipReason string, start bool)
	// TaskCtx must return a task context with a deadline.
	TaskCtx task.ContextFactory
	// TaskFactory is a function that converts an etcd watch event to a task.
	TaskFactory TaskFactory[T]
}

func (c Config[T]) Validate() error {
	errs := errors.NewMultiError()
	if c.Name == "" {
		errs.Append(errors.New("orchestrator name must be configured"))
	}
	if c.Source.WatchPrefix.Prefix() == "" {
		errs.Append(errors.New("source watch prefix definition must be configured"))
	}
	if c.Source.RestartInterval <= 0 {
		errs.Append(errors.New("restart interval must be configured"))
	}
	if c.DistributionKey == nil {
		errs.Append(errors.New("task distribution factory key factory must be configured"))
	}
	if c.TaskKey == nil {
		errs.Append(errors.New("task key must be configured"))
	}
	if c.TaskCtx == nil {
		errs.Append(errors.New("task ctx factory must be configured"))
	}
	if c.TaskFactory == nil {
		errs.Append(errors.New("task factory must be configured"))
	}
	return errs.ErrorOrNil()
}

type Source[T any] struct {
	// WatchPrefix defines an etcd prefix that is watched by GetAllAndWatch.
	// Each event triggers new task.
	WatchPrefix etcdop.PrefixT[T]
	// WatchEtcdOps contains additional options for the watch operation
	WatchEtcdOps []etcd.OpOption
	// ReSyncInterval defines the interval after all keys in the prefix are processed again.
	RestartInterval time.Duration
}

// Orchestrator creates a task for each watch event, but only on one worker node in the cluster.
// Decision is made by the distribution.Assigner.
// See documentation of: distribution.Node, task.Node, Config[R].
type orchestrator[T any] struct {
	clock  clock.Clock
	logger log.Logger
	tracer trace.Tracer
	client *etcd.Client
	dist   *distribution.Node
	tasks  *task.Node
	config Config[T]
}

type TaskFactory[T any] func(event etcdop.WatchEventT[T]) task.Fn

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

	w := &orchestrator[T]{
		clock:  d.Clock(),
		logger: d.Logger().AddPrefix(fmt.Sprintf("[orchestrator][%s]", config.Name)),
		tracer: d.Telemetry().Tracer(),
		client: d.EtcdClient(),
		dist:   d.DistributionWorkerNode(),
		tasks:  d.TaskNode(),
		config: config,
	}

	// Delete events are not needed/ignored
	w.config.Source.WatchEtcdOps = append(w.config.Source.WatchEtcdOps, etcd.WithFilterDelete())

	return w.start(ctx, wg)
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

	ctx, span := o.tracer.Start(ctx, SpanNamePrefix+"."+o.config.Name)
	err := <-o.config.Source.WatchPrefix.
		GetAllAndWatch(ctx, o.client, o.config.Source.WatchEtcdOps...).
		SetupConsumer(o.logger).
		WithOnClose(func(err error) {
			telemetry.EndSpan(span, &err)
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
