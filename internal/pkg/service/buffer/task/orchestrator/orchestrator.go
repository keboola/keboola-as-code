// Package orchestrator combines the distribution.Node and the task.Node,
// to run a task only on one node in the cluster, as a reaction to a watch event.
package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
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
	TaskKey func(event etcdop.WatchEventT[T]) key.TaskKey
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
	logger log.Logger
	tracer trace.Tracer
	client *etcd.Client
	dist   *distribution.Node
	tasks  *task.Node
	config Config[T]
}

type TaskFactory[T any] func(event etcdop.WatchEventT[T]) task.Task

type dependencies interface {
	Logger() log.Logger
	Tracer() trace.Tracer
	EtcdClient() *etcd.Client
	DistributionWorkerNode() *distribution.Node
	TaskNode() *task.Node
}

func Start[T any](ctx context.Context, wg *sync.WaitGroup, d dependencies, config Config[T]) <-chan error {
	if err := config.Validate(); err != nil {
		panic(err)
	}

	w := &orchestrator[T]{
		logger: d.Logger().AddPrefix(fmt.Sprintf("[orchestrator][%s]", config.Name)),
		tracer: d.Tracer(),
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
	work := func(ctx context.Context, assigner *distribution.Assigner) <-chan error {
		ctx, span := o.tracer.Start(ctx, SpanNamePrefix+"."+o.config.Name)
		return o.config.Source.WatchPrefix.
			GetAllAndWatch(ctx, o.client, o.config.Source.WatchEtcdOps...).
			SetupConsumer(o.logger).
			WithOnClose(func(err error) {
				telemetry.EndSpan(span, &err)
			}).
			WithForEach(func(events []etcdop.WatchEventT[R], header *etcdop.Header, _ bool) {
				for _, event := range events {
					o.startTask(assigner, event)
				}
			}).
			StartConsumer(wg)
	}
	return o.dist.StartWork(ctx, wg, o.logger, work, distribution.WithResetInterval(o.config.Source.RestartInterval))
}

// startTask for the event received from the watched prefix.
func (o orchestrator[R]) startTask(assigner *distribution.Assigner, event etcdop.WatchEventT[R]) {
	// Check event type
	if event.Type != etcdop.CreateEvent {
		return
	}

	// Generate keys
	taskKey := o.config.TaskKey(event)
	distributionKey := o.config.DistributionKey(event)

	// Error is not expected, there is present always at least one node - self.
	if !assigner.MustCheckIsOwner(distributionKey) {
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
