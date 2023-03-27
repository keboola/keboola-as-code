package cleanup

import (
	"context"
	"strings"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/semaphore"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// FileExpirationDays defines how old files are to be deleted.
	FileExpirationDays = 1
	// MaxTasksPerNode limits number of parallel cleanup tasks per node.
	MaxTasksPerNode        = 20
	ReceiverCleanupTimeout = 5 * time.Minute
)

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Schema() *schema.Schema
	EtcdClient() *etcd.Client
	Store() *store.Store
	DistributionWorkerNode() *distribution.Node
	TaskNode() *task.Node
}

type Node struct {
	deps   dependencies
	client *etcd.Client
	logger log.Logger
	schema *schema.Schema
	dist   *distribution.Node
	tasks  *task.Node
	sem    *semaphore.Weighted
}

func NewNode(d dependencies, logger log.Logger) *Node {
	return &Node{
		deps:   d,
		client: d.EtcdClient(),
		logger: logger,
		schema: d.Schema(),
		dist:   d.DistributionWorkerNode(),
		tasks:  d.TaskNode(),
		sem:    semaphore.NewWeighted(MaxTasksPerNode),
	}
}

func (n *Node) Check(ctx context.Context) error {
	tasksCount := 0
	err := n.schema.
		Configs().
		Receivers().
		GetAll().
		Do(ctx, n.client).
		ForEachValue(func(v model.ReceiverBase, header *iterator.Header) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if n.dist.MustCheckIsOwner(v.ReceiverKey.String()) {
					if _, err := n.startReceiverCleanupTask(ctx, v.ReceiverKey); err == nil {
						tasksCount++
					} else {
						n.logger.Errorf(`cannot start task for receiver "%s": %s`, v.ReceiverKey.String(), err)
					}
				}
				return nil
			}
		})

	n.logger.Infof(`started "%d" receiver cleanup tasks`, tasksCount)
	if err != nil {
		return errors.Errorf(`receivers iterator failed: %w`, err)
	}
	return nil
}

func (n *Node) startReceiverCleanupTask(ctx context.Context, k key.ReceiverKey) (*model.Task, error) {
	// Limit number of parallel cleanup tasks per node
	if err := n.sem.Acquire(ctx, 1); err != nil {
		return nil, err
	}

	taskKey := key.TaskKey{
		ProjectID: k.ProjectID,
		TaskID: key.TaskID(strings.Join([]string{
			k.ReceiverID.String(),
			"receiver.cleanup",
		}, "/")),
	}

	return n.tasks.StartTask(ctx, taskKey, func(_ context.Context, logger log.Logger) (task.Result, error) {
		defer n.sem.Release(1)

		// Don't cancel cleanup on the shutdown, but wait for timeout
		ctx, cancel := context.WithTimeout(context.Background(), ReceiverCleanupTimeout)
		defer cancel()

		return newTask(n.deps, logger, k).Run(ctx)
	})
}
