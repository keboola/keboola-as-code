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

	_, err = op.MergeToTxn(
		s.createExportBaseOp(ctx, export.ExportBase),
		s.createMappingOp(ctx, export.Mapping),
		s.createTokenOp(ctx, export.ExportKey, export.Token),
	).Do(ctx, s.client)
	return err
}

func (s *Store) createExportBaseOp(_ context.Context, export model.ExportBase) op.BoolOp {
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

	export, err := s.getExportBaseOp(ctx, exportKey).Do(ctx, s.client)
	if err != nil {
		return model.Export{}, err
	}
	mapping, err := s.getLatestMappingOp(ctx, exportKey).Do(ctx, s.client)
	if err != nil {
		return model.Export{}, err
	}
	token, err := s.getTokenOp(ctx, exportKey).Do(ctx, s.client)
	if err != nil {
		return model.Export{}, err
	}
	return model.Export{
		ExportBase: export.Value,
		Mapping:    mapping.Value,
		Token:      token.Value,
	}, nil
}

func (s *Store) getExportBaseOp(_ context.Context, exportKey key.ExportKey) op.ForType[*op.KeyValueT[model.ExportBase]] {
	return s.schema.
		Configs().
		Exports().
		ByKey(exportKey).
		Get().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.ExportBase], err error) (*op.KeyValueT[model.ExportBase], error) {
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

	exportKvs, err := s.listExportsBaseOp(ctx, receiverKey).Do(ctx, s.client)
	if err != nil {
		return nil, err
	}
	exports := make([]model.Export, 0, len(exportKvs))
	for _, export := range exportKvs {
		mapping, err := s.getLatestMappingOp(ctx, export.Value.ExportKey).Do(ctx, s.client)
		if err != nil {
			return nil, err
		}
		token, err := s.getTokenOp(ctx, export.Value.ExportKey).Do(ctx, s.client)
		if err != nil {
			return nil, err
		}
		exports = append(exports, model.Export{
			ExportBase: export.Value,
			Mapping:    mapping.Value,
			Token:      token.Value,
		})
	}

	return exports, nil
}

func (s *Store) listExportsBaseOp(_ context.Context, receiverKey key.ReceiverKey) op.ForType[op.KeyValuesT[model.ExportBase]] {
	return s.schema.
		Configs().
		Exports().
		InReceiver(receiverKey).
		GetAll(etcd.WithSort(etcd.SortByKey, etcd.SortAscend))
}

// DeleteExport deletes an export from the store.
// Logic errors:
// - ResourceNotFoundError.
func (s *Store) DeleteExport(ctx context.Context, exportKey key.ExportKey) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.DeleteReceiver")
	defer telemetry.EndSpan(span, &err)

	_, err = op.MergeToTxn(
		s.deleteExportBaseOp(ctx, exportKey),
		s.deleteAllMappingsOp(ctx, exportKey),
		s.deleteTokenOp(ctx, exportKey),
	).Do(ctx, s.client)
	return err
}

func (s *Store) deleteExportBaseOp(_ context.Context, exportKey key.ExportKey) op.BoolOp {
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

func (s *Store) deleteExportBaseListOp(_ context.Context, receiverKey key.ReceiverKey) op.CountOp {
	return s.schema.
		Configs().
		Exports().
		InReceiver(receiverKey).
		DeleteAll().
		WithProcessor(func(ctx context.Context, response etcd.OpResponse, result int64, err error) (int64, error) {
			if result == 0 && err == nil {
				return 0, serviceError.NewResourceNotFoundError("export", receiverKey.String())
			}
			return result, err
		})
}
