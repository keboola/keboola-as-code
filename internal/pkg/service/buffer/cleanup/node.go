package cleanup

import (
	"context"
	"strings"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/semaphore"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// FileExpirationDays defines how old files are to be deleted.
	FileExpirationDays = 1
	// MaxTasksPerNode limits number of parallel cleanup tasks per node.
	MaxTasksPerNode         = 20
	ReceiverCleanupTimeout  = 5 * time.Minute
	taskTypeReceiverCleanup = "receiver.cleanup"
)

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Schema() *schema.Schema
	EtcdClient() *etcd.Client
	Store() *store.Store
	DistributionNode() *distribution.Node
	TaskNode() *task.Node
	StatisticsRepository() *statistics.Repository
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
		dist:   d.DistributionNode(),
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
						n.logger.ErrorfCtx(ctx, `cannot start task for receiver "%s": %s`, v.ReceiverKey.String(), err)
					}
				}
				return nil
			}
		})

	n.logger.InfofCtx(ctx, `started "%d" receiver cleanup tasks`, tasksCount)
	if err != nil {
		return errors.Errorf(`receivers iterator failed: %w`, err)
	}
	return nil
}

func (n *Node) startReceiverCleanupTask(ctx context.Context, k key.ReceiverKey) (*task.Task, error) {
	// Limit number of parallel cleanup tasks per node
	if err := n.sem.Acquire(ctx, 1); err != nil {
		return nil, err
	}

	return n.tasks.StartTask(ctx, task.Config{
		Type: taskTypeReceiverCleanup,
		Key: task.Key{
			ProjectID: k.ProjectID,
			TaskID: task.ID(strings.Join([]string{
				k.ReceiverID.String(),
				taskTypeReceiverCleanup,
			}, "/")),
		},
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), ReceiverCleanupTimeout)
		},
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			defer n.sem.Release(1)
			return newCleanupTask(n.deps, logger, k).Run(ctx)
		},
	})
}
