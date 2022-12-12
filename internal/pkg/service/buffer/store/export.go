package store

import (
	"context"

	"github.com/keboola/go-client/pkg/storageapi"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// CreateExport puts an export into the store.
// Logic errors:
// - CountLimitReachedError
// - ResourceAlreadyExistsError.
func (s *Store) CreateExport(ctx context.Context, export model.Export, fileRes *storageapi.File) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.CreateExport")
	defer telemetry.EndSpan(span, &err)

	count, err := s.schema.Configs().Exports().InReceiver(export.ReceiverKey).Count().Do(ctx, s.client)
	if err != nil {
		return err
	} else if count >= MaxExportsPerReceiver {
		return serviceError.NewCountLimitReachedError("export", MaxExportsPerReceiver, "receiver")
	}

	now := s.clock.Now()
	fileKey := key.FileKey{FileID: now, ExportKey: export.ExportKey}
	slice := model.Slice{
		SliceKey:    key.SliceKey{SliceID: now, FileKey: fileKey},
		SliceNumber: 1,
	}
	// We store a copy of the mapping for retrieval optimization.
	// A change in the mapping causes a new file to be created so that here it is immutable.
	file := model.File{
		FileKey:         fileKey,
		Mapping:         export.Mapping,
		StorageResource: fileRes,
	}
	_, err = op.MergeToTxn(
		s.createExportBaseOp(ctx, export.ExportBase),
		s.createMappingOp(ctx, export.Mapping),
		s.createTokenOp(ctx, model.TokenForExport{ExportKey: export.ExportKey, Token: export.Token}),
		s.createFileOp(ctx, file),
		s.createSliceOp(ctx, slice),
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

func (s *Store) UpdateExport(ctx context.Context, export model.Export) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.UpdateExport")
	defer telemetry.EndSpan(span, &err)

	_, err = op.MergeToTxn(
		s.updateExportBaseOp(ctx, export.ExportBase),
		s.updateMappingOp(ctx, export.Mapping),
	).Do(ctx, s.client)

	return err
}

func (s *Store) updateExportBaseOp(_ context.Context, export model.ExportBase) op.NoResultOp {
	return s.schema.
		Configs().
		Exports().
		ByKey(export.ExportKey).
		Put(export)
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
		Token:      token.Value.Token,
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
func (s *Store) ListExports(ctx context.Context, receiverKey key.ReceiverKey, ops ...iterator.Option) (exports []model.Export, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.ListExports")
	defer telemetry.EndSpan(span, &err)

	err = s.
		exportsIterator(ctx, receiverKey).Do(ctx, s.client).
		ForEachValue(func(exportBase model.ExportBase, _ *iterator.Header) error {
			mapping, err := s.getLatestMappingOp(ctx, exportBase.ExportKey).Do(ctx, s.client)
			if err != nil {
				return err
			}
			token, err := s.getTokenOp(ctx, exportBase.ExportKey).Do(ctx, s.client)
			if err != nil {
				return err
			}
			exports = append(exports, model.Export{
				ExportBase: exportBase,
				Mapping:    mapping.Value,
				Token:      token.Value.Token,
			})
			return nil
		})

	if err != nil {
		return nil, err
	}

	return exports, nil
}

func (s *Store) exportsIterator(_ context.Context, receiverKey key.ReceiverKey, ops ...iterator.Option) iterator.DefinitionT[model.ExportBase] {
	return s.schema.
		Configs().
		Exports().
		InReceiver(receiverKey).
		GetAll(ops...)
}

// DeleteExport deletes an export from the store.
// Logic errors:
// - ResourceNotFoundError.
func (s *Store) DeleteExport(ctx context.Context, exportKey key.ExportKey) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.DeleteReceiver")
	defer telemetry.EndSpan(span, &err)

	_, err = op.MergeToTxn(
		s.deleteExportBaseOp(ctx, exportKey),
		s.deleteExportMappingsOp(ctx, exportKey),
		s.deleteExportTokenOp(ctx, exportKey),
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

func (s *Store) deleteReceiverExportsOp(_ context.Context, receiverKey key.ReceiverKey) op.CountOp {
	return s.schema.
		Configs().
		Exports().
		InReceiver(receiverKey).
		DeleteAll()
}
