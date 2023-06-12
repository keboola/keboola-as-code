package orchestrator

import (
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
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
	TaskKey func(event etcdop.WatchEventT[T]) task.Key
	// StartTaskIf, if set, it determines whether the task is started or not
	StartTaskIf func(event etcdop.WatchEventT[T]) (skipReason string, start bool)
	// TaskCtx must return a task context with a deadline.
	TaskCtx task.ContextFactory
	// TaskFactory is a function that converts an etcd watch event to a task.
	TaskFactory TaskFactory[T]
}

type TaskFactory[T any] func(event etcdop.WatchEventT[T]) task.Fn

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
