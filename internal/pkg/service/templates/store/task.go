package store

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

func (s *Store) GetTask(ctx context.Context, taskKey task.Key) (r task.Model, err error) {
	ctx, span := s.tracer.Start(ctx, "keboola.go.templates.store.GetTask")
	defer telemetry.EndSpan(span, &err)

	tsk, err := s.getTaskOp(ctx, taskKey).Do(ctx, s.client)
	if err != nil {
		return task.Model{}, err
	}
	return tsk.Value, nil
}

func (s *Store) getTaskOp(_ context.Context, taskKey task.Key) op.ForType[*op.KeyValueT[task.Model]] {
	return s.schema.
		Tasks().
		ByKey(taskKey).
		Get().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[task.Model], err error) (*op.KeyValueT[task.Model], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("task", taskKey.TaskID.String(), "project")
			}
			return kv, err
		})
}
