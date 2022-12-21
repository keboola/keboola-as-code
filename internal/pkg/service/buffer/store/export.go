package store

import (
	"context"
	"reflect"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
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

func (s *Store) createExportOp(ctx context.Context, export model.Export) *op.TxnOp {
	return op.MergeToTxn(
		ctx,
		s.createExportBaseOp(ctx, export.ExportBase),
		s.createMappingOp(ctx, export.Mapping),
		s.createTokenOp(ctx, export.Token),
		s.createFileOp(ctx, export.OpenedFile),
	)
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

func (s *Store) UpdateExport(ctx context.Context, k key.ExportKey, fn func(model.Export) (model.Export, error)) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.UpdateExport")
	defer telemetry.EndSpan(span, &err)

	export, err := s.GetExport(ctx, k)
	if err != nil {
		return err
	}

	oldValue := export
	export, err = fn(export)
	if err != nil {
		return err
	}

	txn, err := s.updateExportOp(ctx, oldValue, export)
	if err != nil {
		return err
	}

	_, err = txn.Do(ctx, s.client)
	return err
}

func (s *Store) updateExportOp(ctx context.Context, oldValue, newValue model.Export) (*op.TxnOp, error) {
	ops := []op.Op{
		s.updateExportBaseOp(ctx, newValue.ExportBase),
	}

	if !reflect.DeepEqual(newValue.Mapping, oldValue.Mapping) {
		ops = append(ops, s.updateMappingOp(ctx, newValue.Mapping))
	}

	if newValue.Token.ID != oldValue.Token.ID {
		ops = append(ops, s.updateTokenOp(ctx, newValue.Token))
	}

	if newValue.OpenedFile.FileID != oldValue.OpenedFile.FileID {
		now := newValue.OpenedFile.OpenedAt()

		// Close opened file and create the new file
		createFileTxn := s.createFileOp(ctx, newValue.OpenedFile)
		closeFileTxn, err := s.setFileStateOp(ctx, now, &oldValue.OpenedFile, filestate.Closing)
		if err != nil {
			return nil, err
		}

		// Close opened slice
		slice, err := s.getLatestSliceOp(ctx, oldValue.OpenedFile.FileKey).Do(ctx, s.client)
		if err != nil {
			return nil, err
		}
		closeSliceTxn, err := s.setSliceStateOp(ctx, now, &slice.Value, slicestate.Closing)
		if err != nil {
			return nil, err
		}

		ops = append(ops, createFileTxn, closeFileTxn, closeSliceTxn)
	}

	return op.MergeToTxn(ctx, ops...), nil
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
	return s.getExportOp(ctx, exportKey).Do(ctx, s.client)
}

func (s *Store) getExportOp(ctx context.Context, exportKey key.ExportKey) op.JoinTo[model.Export] {
	export := model.Export{}
	return op.Join(
		ctx,
		&export,
		s.getExportBaseOp(ctx, exportKey).WithOnResult(func(kv *op.KeyValueT[model.ExportBase]) {
			export.ExportBase = kv.Value
		}),
		s.getLatestMappingOp(ctx, exportKey).WithOnResult(func(kv *op.KeyValueT[model.Mapping]) {
			export.Mapping = kv.Value
		}),
		s.getTokenOp(ctx, exportKey).WithOnResult(func(kv *op.KeyValueT[model.Token]) {
			export.Token = kv.Value
		}),
		s.getOpenedFileOp(ctx, exportKey).WithOnResult(func(kv *op.KeyValueT[model.File]) {
			export.OpenedFile = kv.Value
		}),
	)
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
		exportBaseIterator(ctx, receiverKey).Do(ctx, s.client).
		ForEachValue(func(exportBase model.ExportBase, header *iterator.Header) error {
			mapping, err := s.getLatestMappingOp(ctx, exportBase.ExportKey, etcd.WithRev(header.Revision)).Do(ctx, s.client)
			if err != nil {
				return err
			}

			token, err := s.getTokenOp(ctx, exportBase.ExportKey, etcd.WithRev(header.Revision)).Do(ctx, s.client)
			if err != nil {
				return err
			}

			openedFile, err := s.getOpenedFileOp(ctx, exportBase.ExportKey, etcd.WithRev(header.Revision)).Do(ctx, s.client)
			if err != nil {
				return err
			}

			exports = append(exports, model.Export{
				ExportBase: exportBase,
				Mapping:    mapping.Value,
				Token:      token.Value,
				OpenedFile: openedFile.Value,
			})
			return nil
		})

	if err != nil {
		return nil, err
	}

	return exports, nil
}

func (s *Store) exportBaseIterator(_ context.Context, receiverKey key.ReceiverKey, ops ...iterator.Option) iterator.DefinitionT[model.ExportBase] {
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
		ctx,
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
