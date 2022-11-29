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

// CreateExport puts an export into the store.
// Logic errors:
// - CountLimitReachedError
// - ResourceAlreadyExistsError.
func (s *Store) CreateExport(ctx context.Context, export model.Export) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.CreateExport")
	defer telemetry.EndSpan(span, &err)

	count, err := s.schema.Configs().Exports().InReceiver(export.ReceiverKey).Count().Do(ctx, s.client)
	if err != nil {
		return err
	} else if count >= MaxExportsPerReceiver {
		return serviceError.NewCountLimitReachedError("export", MaxExportsPerReceiver, "receiver")
	}

	_, err = s.createExportOp(ctx, export).Do(ctx, s.client)
	return err
}

func (s *Store) createExportOp(_ context.Context, export model.Export) op.BoolOp {
	return s.schema.
		Configs().
		Exports().
		ByKey(export.ExportKey).
		PutIfNotExists(export).
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, ok bool, err error) (bool, error) {
			if !ok && err == nil {
				return false, serviceError.NewResourceAlreadyExistsError("export", export.ExportKey.String(), "receiver")
			}
			return ok, err
		})
}

// GetExport fetches an export from the store.
// Logic errors:
// - ResourceNotFoundError.
func (s *Store) GetExport(ctx context.Context, exportKey key.ExportKey) (r model.Export, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.GetExport")
	defer telemetry.EndSpan(span, &err)

	kv, err := s.getExportOp(ctx, exportKey).Do(ctx, s.client)
	if err != nil {
		return model.Export{}, err
	}
	return kv.Value, nil
}

func (s *Store) getExportOp(_ context.Context, exportKey key.ExportKey) op.ForType[*op.KeyValueT[model.Export]] {
	return s.schema.
		Configs().
		Exports().
		ByKey(exportKey).
		Get().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.Export], err error) (*op.KeyValueT[model.Export], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("export", exportKey.String())
			}
			return kv, err
		})
}

// ListExports from the store.
func (s *Store) ListExports(ctx context.Context, receiverKey key.ReceiverKey) (out []model.Export, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.ListExports")
	defer telemetry.EndSpan(span, &err)

	kvs, err := s.listExportsOp(ctx, receiverKey).Do(ctx, s.client)
	if err != nil {
		return nil, err
	}

	return kvs.Values(), nil
}

func (s *Store) listExportsOp(_ context.Context, receiverKey key.ReceiverKey) op.ForType[op.KeyValuesT[model.Export]] {
	return s.schema.Configs().Exports().InReceiver(receiverKey).GetAll(etcd.WithSort(etcd.SortByKey, etcd.SortAscend))
}

// DeleteExport deletes an export from the store.
// Logic errors:
// - ResourceNotFoundError.
func (s *Store) DeleteExport(ctx context.Context, exportKey key.ExportKey) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.DeleteReceiver")
	defer telemetry.EndSpan(span, &err)

	_, err = s.deleteExportOp(ctx, exportKey).Do(ctx, s.client)
	return err
}

func (s *Store) deleteExportOp(_ context.Context, exportKey key.ExportKey) op.BoolOp {
	return s.schema.
		Configs().
		Exports().
		ByKey(exportKey).
		Delete().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, ok bool, err error) (bool, error) {
			if !ok && err == nil {
				return false, serviceError.NewResourceNotFoundError("export", exportKey.String())
			}
			return ok, err
		})
}
