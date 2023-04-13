package cleanup

import (
	"context"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/semaphore"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
)

const (
	Timeout              = 5 * time.Minute
	taskTypeTasksCleanup = "tasks.cleanup"
)

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	TaskNode() *task.Node
}

type Node struct {
	deps   dependencies
	client *etcd.Client
	logger log.Logger
	sem    *semaphore.Weighted
	tasks  *task.Node
}

func NewNode(d dependencies, logger log.Logger) *Node {
	return &Node{
		deps:   d,
		client: d.EtcdClient(),
		logger: logger,
		tasks:  d.TaskNode(),
		sem:    semaphore.NewWeighted(1),
	}
}

func (n *Node) Check(ctx context.Context) error {
	// Limit number of parallel cleanup tasks per node
	if err := n.sem.Acquire(ctx, 1); err != nil {
		return err
	}

	_, err := n.tasks.StartTask(task.Config{
		Type: taskTypeTasksCleanup,
		Key: task.Key{
			ProjectID: 1,
			TaskID:    taskTypeTasksCleanup,
		},
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), Timeout)
		},
		Operation: func(ctx context.Context, logger log.Logger) (task.Result, error) {
			defer n.sem.Release(1)
			return newCleanupTask(n.deps, logger).Run(ctx)
		},
	})
	if err != nil {
		return err
	}

	return nil
}
