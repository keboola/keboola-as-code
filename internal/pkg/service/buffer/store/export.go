package store

import (
	"context"
	"reflect"
	"sort"

	etcd "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/errgroup"

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
func (s *Store) CreateExport(ctx context.Context, export model.Export) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.CreateExport")
	defer telemetry.EndSpan(span, &err)

	exports := s.schema.Configs().Exports().InReceiver(export.ReceiverKey)
	return op.
		Atomic().
		Read(func() op.Op {
			return exports.Count().WithOnResultOrErr(func(count int64) error {
				if count >= MaxExportsPerReceiver {
					return serviceError.NewCountLimitReachedError("export", MaxExportsPerReceiver, "receiver")
				}
				return nil
			})
		}).
		Write(func() op.Op {
			return s.createExportOp(ctx, export)
		}).
		Do(ctx, s.client)
}

func (s *Store) createExportOp(ctx context.Context, export model.Export) *op.TxnOpDef {
	return op.MergeToTxn(
		s.createExportBaseOp(ctx, export.ExportBase),
		s.createMappingOp(ctx, export.Mapping),
		s.createTokenOp(ctx, export.Token),
		s.createFileOp(ctx, export.OpenedFile),
		s.createSliceOp(ctx, export.OpenedSlice),
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
				return false, serviceError.NewResourceAlreadyExistsError("export", export.ExportID.String(), "receiver")
			}
			return ok, err
		})
}

func (s *Store) UpdateExport(ctx context.Context, k key.ExportKey, fn func(model.Export) (model.Export, error)) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.UpdateExport")
	defer telemetry.EndSpan(span, &err)

	var export *model.Export
	return op.Atomic().
		Read(func() op.Op {
			return s.getExportOp(ctx, k).WithOnResult(func(v *model.Export) {
				export = v
			})
		}).
		WriteOrErr(func() (op.Op, error) {
			oldValue := *export
			newValue, err := fn(oldValue)
			if err != nil {
				return nil, err
			}
			txn, err := s.updateExportOp(ctx, oldValue, newValue)
			if err != nil {
				return nil, err
			}
			return txn, nil
		}).
		Do(ctx, s.client)
}

func (s *Store) updateExportOp(ctx context.Context, oldValue, newValue model.Export) (*op.TxnOpDef, error) {
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
		swapFile, err := s.swapFileOp(ctx, now, &oldValue.OpenedFile, &oldValue.OpenedSlice, newValue.OpenedFile, newValue.OpenedSlice)
		if err != nil {
			return nil, err
		}
		ops = append(ops, swapFile)
	}

	return op.MergeToTxn(ops...), nil
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
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.GetExport")
	defer telemetry.EndSpan(span, &err)
	return s.getExportOp(ctx, exportKey).Do(ctx, s.client)
}

func (s *Store) getExportOp(ctx context.Context, exportKey key.ExportKey) *op.JoinTo[model.Export] {
	export := model.Export{}
	return op.Join(
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
		s.getOpenedSliceOp(ctx, exportKey).WithOnResult(func(kv *op.KeyValueT[model.Slice]) {
			export.OpenedSlice = kv.Value
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
				return nil, serviceError.NewResourceNotFoundError("export", exportKey.ExportID.String(), "receiver")
			}
			return kv, err
		})
}

// ListExports from the store.
func (s *Store) ListExports(ctx context.Context, receiverKey key.ReceiverKey, ops ...iterator.Option) (exports []model.Export, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.ListExports")
	defer telemetry.EndSpan(span, &err)

	// Load sub-objects in parallel, stop at the first error
	exportsMap := make(map[key.ExportKey]*model.Export)
	grp, _ := errgroup.WithContext(ctx)

	i := 0
	err = s.
		exportBaseIterator(ctx, receiverKey, ops...).Do(ctx, s.client).
		ForEachValue(func(exportBase model.ExportBase, header *iterator.Header) error {
			export := &model.Export{ExportBase: exportBase}
			exportsMap[exportBase.ExportKey] = export
			i++

			grp.Go(func() error {
				mapping, err := s.getLatestMappingOp(ctx, exportBase.ExportKey, etcd.WithRev(header.Revision)).Do(ctx, s.client)
				if err == nil {
					export.Mapping = mapping.Value
				}
				return err
			})
			grp.Go(func() error {
				token, err := s.getTokenOp(ctx, exportBase.ExportKey, etcd.WithRev(header.Revision)).Do(ctx, s.client)
				if err == nil {
					export.Token = token.Value
				}
				return err
			})
			grp.Go(func() error {
				kv, err := s.getOpenedFileOp(ctx, exportBase.ExportKey, etcd.WithRev(header.Revision)).Do(ctx, s.client)
				if err == nil {
					export.OpenedFile = kv.Value
				}
				return err
			})
			grp.Go(func() error {
				kv, err := s.getOpenedSliceOp(ctx, export.ExportKey, etcd.WithRev(header.Revision)).Do(ctx, s.client)
				if err == nil {
					export.OpenedSlice = kv.Value
				}
				return err
			})
			return nil
		})

	if err != nil {
		return nil, err
	}

	// Wait for sub-objects
	if err := grp.Wait(); err != nil {
		return nil, err
	}

	// Convert map to slice
	exports = make([]model.Export, 0, len(exportsMap))
	for _, v := range exportsMap {
		exports = append(exports, *v)
	}
	sort.SliceStable(exports, func(i, j int) bool {
		return exports[i].ExportID < exports[j].ExportID
	})
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
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.DeleteReceiver")
	defer telemetry.EndSpan(span, &err)

	_, err = op.MergeToTxn(
		s.deleteExportBaseOp(ctx, exportKey),
		s.deleteExportMappingsOp(ctx, exportKey),
		s.deleteExportTokenOp(ctx, exportKey),
		s.schema.ReceivedStats().InExport(exportKey).DeleteAll(),
		s.schema.Files().Opened().InExport(exportKey).DeleteAll(),
		s.schema.Files().Closed().InExport(exportKey).DeleteAll(),
		s.schema.Files().Closing().InExport(exportKey).DeleteAll(),
		s.schema.Files().Importing().InExport(exportKey).DeleteAll(),
		s.schema.Files().Imported().InExport(exportKey).DeleteAll(),
		s.schema.Files().Failed().InExport(exportKey).DeleteAll(),
		s.schema.Slices().Opened().InExport(exportKey).DeleteAll(),
		s.schema.Slices().Closing().InExport(exportKey).DeleteAll(),
		s.schema.Slices().Uploading().InExport(exportKey).DeleteAll(),
		s.schema.Slices().Uploaded().InExport(exportKey).DeleteAll(),
		s.schema.Slices().Failed().InExport(exportKey).DeleteAll(),
		s.schema.Records().InExport(exportKey).DeleteAll(),
		s.schema.Tasks().InExport(exportKey).DeleteAll(),
		s.schema.Runtime().LastRecordID().ByKey(exportKey).Delete(),
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
				return false, serviceError.NewResourceNotFoundError("export", exportKey.ExportID.String(), "receiver")
			}
			return ok, err
		})
}
