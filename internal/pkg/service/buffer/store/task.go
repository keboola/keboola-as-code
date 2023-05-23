package store

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
)

func (s *Store) GetTask(ctx context.Context, taskKey task.Key) (r task.Task, err error) {
	ctx, span := s.telemetry.Tracer().Start(ctx, "keboola.go.buffer.store.GetTask")
	defer span.End(&err)

	tsk, err := s.getTaskOp(ctx, taskKey).Do(ctx, s.client)
	if err != nil {
		return task.Task{}, err
	}
	return tsk.Value, nil
}

func (s *Store) getTaskOp(_ context.Context, taskKey task.Key) op.ForType[*op.KeyValueT[task.Task]] {
	return s.schema.
		Tasks().
		ByKey(taskKey).
		Get().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[task.Task], err error) (*op.KeyValueT[task.Task], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("task", taskKey.TaskID.String(), "project")
			}
			return kv, err
		})
}
