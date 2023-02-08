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
// The orchestrator watches for all PUT events in the etcd Prefix.
// For each event, the TaskFactory is invoked.
//
// The TaskFactory is invoked only if the event ReceiverKey is assigned to the orchestrator.Node.
// All events are received by all nodes, but each node decides
// whether the resource is assigned to it and task should be started or not.
//
// Each orchestrator.Node contains the distribution.Node, the nodes are synchronized, so they make the same decisions.
// Decision is made by the distribution.Assigner. Any short-term difference can lead to task duplication,
// but it is prevented by the task lock.
//
// The watched Prefix is rescanned at ReSyncInterval and when there is any change in workers distribution.
// On the rescan, the existing tasks are NOT cancelled.
// Task duplication is prevented by the task lock.
//
// The TaskFactory may return nil, then the event will be ignored.
type Config[R ReceiverResource] struct {
	Prefix         etcdop.PrefixT[R]
	ReSyncInterval time.Duration
	TaskType       string
	TaskFactory    TaskFactory[R]
	// StartTaskIf can be nil, if set, it determines whether the task is started or not
	StartTaskIf func(event etcdop.WatchEventT[R]) (skipReason string, start bool)
}

// Orchestrator creates a task for each watch event, but only on one worker node in the cluster.
// Decision is made by the distribution.Assigner.
// See documentation of: distribution.Node, task.Node, Config[R].
type orchestrator[R ReceiverResource] struct {
	logger log.Logger
	client *etcd.Client
	dist   *distribution.Node
	tasks  *task.Node
	config Config[R]
}

type ReceiverResource interface {
	String() string
	GetReceiverKey() key.ReceiverKey
}

type TaskFactory[T ReceiverResource] func(event etcdop.WatchEventT[T]) task.Task

type dependencies interface {
	Logger() log.Logger
	EtcdClient() *etcd.Client
	DistributionWorkerNode() *distribution.Node
	TaskNode() *task.Node
}

func Start[R ReceiverResource](ctx context.Context, wg *sync.WaitGroup, d dependencies, config Config[R]) <-chan error {
	// Validate the config
	if config.ReSyncInterval <= 0 {
		panic(errors.New("re-sync interval must be configured"))
	}
	if config.TaskType == "" {
		panic(errors.New("task type must be configured"))
	}
	if config.TaskFactory == nil {
		panic(errors.New("task factory must be configured"))
	}

	w := &orchestrator[R]{
		logger: d.Logger().AddPrefix(fmt.Sprintf("[orchestrator][%s]", config.TaskType)),
		client: d.EtcdClient(),
		dist:   d.DistributionWorkerNode(),
		tasks:  d.TaskNode(),
		config: config,
	}

	return w.start(ctx, wg)
}

func (w orchestrator[R]) start(ctx context.Context, wg *sync.WaitGroup) <-chan error {
	work := func(distCtx context.Context, assigner *distribution.Assigner) <-chan error {
		return w.config.Prefix.
			GetAllAndWatch(distCtx, w.client, etcd.WithFilterDelete()).
			SetupConsumer(w.logger).
			WithForEach(func(events []etcdop.WatchEventT[R], header *etcdop.Header, _ bool) {
				for _, event := range events {
					w.startTask(ctx, assigner, event)
				}
			}).
			StartConsumer(wg)
	}
	return w.dist.StartWork(ctx, wg, w.logger, work, distribution.WithResetInterval(w.config.ReSyncInterval))
}

// startTask for the event received from the watched prefix.
func (w orchestrator[R]) startTask(ctx context.Context, assigner *distribution.Assigner, event etcdop.WatchEventT[R]) {
	// Should be the task started?
	resourceKey := event.Value.String()
	if w.config.StartTaskIf != nil {
		if skipReason, start := w.config.StartTaskIf(event); !start {
			w.logger.Debugf(`skipped "%s", %s`, resourceKey, skipReason)
			return
		}
	}

	// Error is not expected, there is present always at least one node - self.
	if !assigner.MustCheckIsOwner(resourceKey) {
		// Another worker node handles the resource.
		w.logger.Debugf(`not assigned "%s"`, resourceKey)
		return
	}

	// Compose lock name.
	// Task and task lock are bounded to the ReceiverKey, so the ReceiverKey part is stripped.
	value := event.Value
	receiverKey := value.GetReceiverKey()
	lock := w.config.TaskType + "/" + value.String()

	// Create task
	taskFn := w.config.TaskFactory(event)
	if taskFn == nil {
		w.logger.Infof(`skipped "%s"`, resourceKey)
		return
	}

	// Run task in the background
	w.logger.Infof(`assigned "%s"`, resourceKey)
	if _, err := w.tasks.StartTask(ctx, receiverKey, w.config.TaskType, lock, taskFn); err != nil {
		w.logger.Error(err)
	}
}
