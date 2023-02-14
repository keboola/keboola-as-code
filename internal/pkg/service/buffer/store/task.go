package store

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

func (s *Store) GetTask(ctx context.Context, taskKey key.TaskKey) (r model.Task, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.GetTask")
	defer telemetry.EndSpan(span, &err)

	task, err := s.getTaskOp(ctx, taskKey).Do(ctx, s.client)
	if err != nil {
		return model.Task{}, err
	}
	return task.Value, nil
}

func (s *Store) getTaskOp(_ context.Context, taskKey key.TaskKey) op.ForType[*op.KeyValueT[model.Task]] {
	return s.schema.
		Tasks().
		ByKey(taskKey).
		Get().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.Task], err error) (*op.KeyValueT[model.Task], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("task", taskKey.TaskID.String(), "receiver")
			}
			return kv, err
		})
}
