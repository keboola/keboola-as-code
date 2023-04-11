package store

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/store/model"
	taskKey "github.com/keboola/keboola-as-code/internal/pkg/service/common/task/key"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

func (s *Store) GetTask(ctx context.Context, taskKey taskKey.Key) (r model.Task, err error) {
	ctx, span := s.tracer.Start(ctx, "keboola.go.templates.store.GetTask")
	defer telemetry.EndSpan(span, &err)

	task, err := s.getTaskOp(ctx, taskKey).Do(ctx, s.client)
	if err != nil {
		return model.Task{}, err
	}
	return task.Value, nil
}

func (s *Store) getTaskOp(_ context.Context, taskKey taskKey.Key) op.ForType[*op.KeyValueT[model.Task]] {
	return s.schema.
		Tasks().
		ByKey(taskKey).
		Get().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.Task], err error) (*op.KeyValueT[model.Task], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("task", taskKey.TaskID.String(), "project")
			}
			return kv, err
		})
}
