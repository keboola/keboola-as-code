// Package orchestrator combines the distribution.Node and the task.Node,
// to run a task only on one node in the cluster, as a reaction to a watch event.
package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
	TaskCtx func(ctx context.Context) (context.Context, context.CancelFunc)
	// TaskFactory is a function that converts an etcd watch event to a task.
	TaskFactory TaskFactory[T]
}

type Source[T any] struct {
	// WatchPrefix defines an etcd prefix that is watched by GetAllAndWatch.
	// Each event triggers new task.
	WatchPrefix etcdop.PrefixT[T]
	// WatchEvents must contain at least one of etcdop.CreateEvent, etcdop.UpdateEvent, etcdop.DeleteEvent.
	WatchEvents []etcdop.EventType
	// WatchEtcdOps contains additional options for the watch operation
	WatchEtcdOps []etcd.OpOption
	// ReSyncInterval defines the interval after all keys in the prefix are processed again.
	ReSyncInterval time.Duration
}

// Orchestrator creates a task for each watch event, but only on one worker node in the cluster.
// Decision is made by the distribution.Assigner.
// See documentation of: distribution.Node, task.Node, Config[R].
type orchestrator[T any] struct {
	logger       log.Logger
	client       *etcd.Client
	dist         *distribution.Node
	tasks        *task.Node
	config       Config[T]
	allowedTypes map[etcdop.EventType]bool
}

type TaskFactory[T any] func(event etcdop.WatchEventT[T]) task.Task

type dependencies interface {
	Logger() log.Logger
	EtcdClient() *etcd.Client
	DistributionWorkerNode() *distribution.Node
	TaskNode() *task.Node
}

func Start[T any](ctx context.Context, wg *sync.WaitGroup, d dependencies, config Config[T]) <-chan error {
	// Validate the config
	if config.Name == "" {
		panic(errors.New("orchestrator name must be configured"))
	}
	if config.Source.WatchPrefix.Prefix() == "" {
		panic(errors.New("source watch prefix definition must be configured"))
	}
	if len(config.Source.WatchEvents) == 0 {
		panic(errors.New("source watch events definition must be configured"))
	}
	if config.Source.ReSyncInterval <= 0 {
		panic(errors.New("re-sync interval must be configured"))
	}
	if config.DistributionKey == nil {
		panic(errors.New("task distribution factory key factory must be configured"))
	}
	if config.TaskKey == nil {
		panic(errors.New("task key must be configured"))
	}
	if config.TaskCtx == nil {
		panic(errors.New("task ctx factory must be configured"))
	}
	if config.TaskFactory == nil {
		panic(errors.New("task factory must be configured"))
	}

	w := &orchestrator[T]{
		logger:       d.Logger().AddPrefix(fmt.Sprintf("[orchestrator][%s]", config.Name)),
		client:       d.EtcdClient(),
		dist:         d.DistributionWorkerNode(),
		tasks:        d.TaskNode(),
		config:       config,
		allowedTypes: make(map[etcdop.EventType]bool),
	}
	for _, eventType := range config.Source.WatchEvents {
		w.allowedTypes[eventType] = true
	}

	return w.start(ctx, wg)
}

func (o orchestrator[R]) start(ctx context.Context, wg *sync.WaitGroup) <-chan error {
	work := func(distCtx context.Context, assigner *distribution.Assigner) <-chan error {
		return o.config.Source.WatchPrefix.
			GetAllAndWatch(distCtx, o.client, o.config.Source.WatchEtcdOps...).
			SetupConsumer(o.logger).
			WithForEach(func(events []etcdop.WatchEventT[R], header *etcdop.Header, _ bool) {
				for _, event := range events {
					o.startTask(ctx, assigner, event)
				}
			}).
			StartConsumer(wg)
	}
	return o.dist.StartWork(ctx, wg, o.logger, work, distribution.WithResetInterval(o.config.Source.ReSyncInterval))
}

// startTask for the event received from the watched prefix.
func (o orchestrator[R]) startTask(ctx context.Context, assigner *distribution.Assigner, event etcdop.WatchEventT[R]) {
	var ops []task.Option

	// Check event type
	if !o.allowedTypes[event.Type] {
		return
	}

	// Generate keys
	taskKey := o.config.TaskKey(event)
	distributionKey := o.config.DistributionKey(event)

	// Setup context factory
	ops = append(ops, task.WithContextFactory(func(ctx context.Context) (context.Context, context.CancelFunc) {
		ctx, cancel := o.config.TaskCtx(ctx)
		if _, ok := ctx.Deadline(); !ok {
			panic(errors.Errorf(`orchestrator "%s" task context must have a deadline`, o.config.Name))
		}
		return ctx, cancel
	}))

	// Generate lock, if empty, then the lock will be generated from TaskKey in the StartTask method
	if o.config.Lock != nil {
		ops = append(ops, task.WithLock(o.config.Lock(event)))
	}

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

	// Create task
	taskFn := o.config.TaskFactory(event)
	if taskFn == nil {
		o.logger.Infof(`skipped "%s"`, taskKey)
		return
	}

	// Run task in the background
	o.logger.Infof(`assigned "%s"`, taskKey)

	if _, err := o.tasks.StartTask(ctx, taskKey, o.config.Name, taskFn, ops...); err != nil {
		o.logger.Error(err)
	}
}
