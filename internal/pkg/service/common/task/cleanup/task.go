package cleanup

import (
	"context"
	"fmt"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Task struct {
	clock  clock.Clock
	logger log.Logger
	client *etcd.Client
	prefix etcdop.PrefixT[task.Task]
}

func newCleanupTask(d dependencies, logger log.Logger) *Task {
	prefix := etcdop.NewTypedPrefix[task.Task](etcdop.NewPrefix(task.DefaultTaskEtcdPrefix), d.EtcdSerde())
	return &Task{
		clock:  d.Clock(),
		logger: logger,
		client: d.EtcdClient(),
		prefix: prefix,
	}
}

func (t *Task) Run(ctx context.Context) (task.Result, error) {
	deletedTasksCount := int64(0)
	errs := errors.NewMultiError()
	err := t.prefix.GetAll().Do(ctx, t.client).ForEachKV(func(kv op.KeyValueT[task.Task], header *iterator.Header) error {
		if kv.Value.IsForCleanup() {
			if err := etcdop.Key(kv.Key()).Delete().DoOrErr(ctx, t.client); err == nil {
				t.logger.Debugf(`deleted task "%s"`, kv.Value.Key.String())
				deletedTasksCount++
			} else {
				errs.Append(err)
			}
		}
		return nil
	})
	if err != nil {
		errs.Append(err)
	}

	t.logger.Infof(`deleted "%d" tasks`, deletedTasksCount)
	return fmt.Sprintf("deleted %d tasks", deletedTasksCount), errs.ErrorOrNil()
}
