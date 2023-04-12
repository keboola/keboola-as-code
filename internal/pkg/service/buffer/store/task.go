package store

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	taskModel "github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	taskKey "github.com/keboola/keboola-as-code/internal/pkg/service/common/task/key"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

func (s *Store) GetTask(ctx context.Context, taskKey taskKey.Key) (r taskModel.Model, err error) {
	ctx, span := s.tracer.Start(ctx, "keboola.go.buffer.store.GetTask")
	defer telemetry.EndSpan(span, &err)

	task, err := s.getTaskOp(ctx, taskKey).Do(ctx, s.client)
	if err != nil {
		return task.TaskModel{}, err
	}
	return task.Value, nil
}

func (s *Store) getTaskOp(_ context.Context, taskKey taskKey.Key) op.ForType[*op.KeyValueT[taskModel.Model]] {
	return s.schema.
		Tasks().
		ByKey(taskKey).
		Get().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[taskModel.Model], err error) (*op.KeyValueT[taskModel.Model], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("task", taskKey.TaskID.String(), "project")
			}
			return kv, err
		})
}
