// Package orchestrator combines the distribution.Node and the task.Node,
// to run a task only on one node in the cluster, as a reaction to a watch event.
package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Config configures the orchestrator.
//
// The orchestrator watches for all PUT events in the etcd Prefix.
// For each event, the TaskFactory is invoked.
//
// The TaskFactory is invoked only if the event ExportKey is assigned to the orchestrator.Node.
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
type Config[R ExportResource] struct {
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
type orchestrator[R ExportResource] struct {
	logger log.Logger
	client *etcd.Client
	dist   *distribution.Node
	tasks  *task.Node
	config Config[R]
}

type ExportResource interface {
	String() string
	GetExportKey() key.ExportKey
}

type TaskFactory[T ExportResource] func(event etcdop.WatchEventT[T]) task.Task

type dependencies interface {
	Logger() log.Logger
	EtcdClient() *etcd.Client
	DistributionWorkerNode() *distribution.Node
	TaskWorkerNode() *task.Node
}

func Start[R ExportResource](ctx context.Context, wg *sync.WaitGroup, d dependencies, config Config[R]) <-chan error {
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
		tasks:  d.TaskWorkerNode(),
		config: config,
	}

	return w.start(ctx, wg)
}

func (w orchestrator[R]) start(ctx context.Context, wg *sync.WaitGroup) <-chan error {
	return w.dist.StartWork(ctx, wg, w.logger, func(distCtx context.Context, assigner *distribution.Assigner) <-chan error {
		initDone := make(chan error)
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Watch for all PUT events
			ch := w.config.Prefix.GetAllAndWatch(distCtx, w.client, etcd.WithFilterDelete())
			for resp := range ch {
				switch {
				case resp.Created:
					// The watcher has been successfully created.
					// This means transition from GetAll to Watch phase.
					close(initDone)
				case resp.Restarted:
					// A fatal error (etcd ErrCompacted) occurred.
					// It is not possible to continue watching, the operation must be restarted.
					// Duplicate tasks are prevented by the lock, so here is no special handling of the reset.
					// Running tasks are NOT stopped.
					w.logger.Warn(resp.RestartReason)
				case resp.InitErr != nil:
					// Initialization error, stop via initDone channel
					initDone <- resp.InitErr
					close(initDone)
				case resp.Err != nil:
					// An error occurred, it is logged.
					// If it is a fatal error, then it is followed
					// by the "Restarted" event handled bellow,
					// and the operation starts from the beginning.
					w.logger.Error(resp.Err)
				default:
					// Process events
					for _, event := range resp.Events {
						w.startTask(ctx, assigner, event)
					}
				}
			}
		}()
		return initDone
	})
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
	// Task and task lock are bounded to the ExportKey, so the ExportKey part is stripped.
	value := event.Value
	exportKey := value.GetExportKey()
	resourceID := strings.Trim(strings.TrimPrefix(value.String(), exportKey.String()), "/")
	lock := w.config.TaskType + "/" + resourceID

	// Create task
	taskFn := w.config.TaskFactory(event)
	if taskFn == nil {
		w.logger.Infof(`skipped "%s"`, resourceKey)
		return
	}

	// Run task in the background
	w.logger.Infof(`assigned "%s"`, resourceKey)
	if _, err := w.tasks.StartTask(ctx, exportKey, w.config.TaskType, lock, taskFn); err != nil {
		w.logger.Error(err)
	}
}
