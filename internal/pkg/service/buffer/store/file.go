package store

import (
	"context"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (s *Store) createFileOp(_ context.Context, file model.File) op.BoolOp {
	return s.schema.
		Files().
		Opened().
		ByKey(file.FileKey).
		PutIfNotExists(file).
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, ok bool, err error) (bool, error) {
			if !ok && err == nil {
				return false, serviceError.NewResourceAlreadyExistsError("file", file.FileID.String(), "export")
			}
			return ok, err
		})
}

func (s *Store) GetFile(ctx context.Context, fileKey key.FileKey) (out model.File, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.GetFile")
	defer telemetry.EndSpan(span, &err)

	file, err := s.getFileOp(ctx, fileKey).Do(ctx, s.client)
	if err != nil {
		return model.File{}, err
	}
	return file.Value, nil
}

func (s *Store) getOpenedFileOp(_ context.Context, exportKey key.ExportKey, opts ...etcd.OpOption) op.ForType[*op.KeyValueT[model.File]] {
	opts = append(opts, etcd.WithSort(etcd.SortByKey, etcd.SortDescend))
	return s.schema.
		Files().
		Opened().
		InExport(exportKey).
		GetOne(opts...).
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.File], err error) (*op.KeyValueT[model.File], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewNoResourceFoundError("opened file", "export")
			}
			return kv, err
		})
}

func (s *Store) getFileOp(_ context.Context, fileKey key.FileKey) op.ForType[*op.KeyValueT[model.File]] {
	return s.schema.
		Files().
		Opened().
		ByKey(fileKey).
		Get().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.File], err error) (*op.KeyValueT[model.File], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("file", fileKey.FileID.String(), "export")
			}
			return kv, err
		})
}

func (s *Store) ListFilesInState(ctx context.Context, state filestate.State, exportKey key.ExportKey, ops ...iterator.Option) (files []model.File, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.ListFilesInState")
	defer telemetry.EndSpan(span, &err)

	files = make([]model.File, 0)
	err = s.fileInStateIterator(ctx, state, exportKey, ops...).Do(ctx, s.client).
		ForEachValue(func(file model.File, header *iterator.Header) error {
			files = append(files, file)
			return nil
		})

	if err != nil {
		return nil, err
	}
	return files, nil
}

func (s *Store) fileInStateIterator(_ context.Context, state filestate.State, exportKey key.ExportKey, ops ...iterator.Option) iterator.DefinitionT[model.File] {
	return s.schema.
		Files().
		InState(state).
		InExport(exportKey).
		GetAll(ops...)
}

// SetFileState method atomically changes the state of the file.
// False is returned, if the given file is already in the target state.
func (s *Store) SetFileState(ctx context.Context, now time.Time, file *model.File, to filestate.State) (ok bool, err error) { //nolint:dupl
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.SetFileState")
	defer telemetry.EndSpan(span, &err)

	txn, err := s.setFileStateOp(ctx, now, file, to)
	if err != nil {
		return false, err
	}

	resp, err := txn.Do(ctx, s.client)
	if err != nil {
		return false, err
	}

	if err == nil && !resp.Succeeded {
		// File is already in the target state
		return false, nil
	}

	return true, nil
}

// MarkFileImported when the import is finished.
func (s *Store) MarkFileImported(ctx context.Context, file *model.File) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.MarkFileImported")
	defer telemetry.EndSpan(span, &err)
	now := s.clock.Now()
	var slices []model.Slice
	return op.Atomic().
		Read(func() op.Op {
			slices = nil
			return s.schema.Slices().Uploaded().InFile(file.FileKey).
				GetAll().
				ForEachOp(func(value model.Slice, header *op.Header) error {
					slices = append(slices, value)
					return nil
				})
		}).
		WriteOrErr(func() (op.Op, error) {
			fileStateOp, err := s.setFileStateOp(ctx, now, file, filestate.Imported)
			if err != nil {
				return nil, err
			}
			ops := []op.Op{fileStateOp}
			for _, slice := range slices {
				slice := slice
				sliceStateOp, err := s.setSliceStateOp(ctx, now, &slice, slicestate.Imported)
				if err != nil {
					return nil, err
				}
				ops = append(ops, sliceStateOp)
			}

			return op.MergeToTxn(ops...), nil
		}).
		Do(ctx, s.client)
}

// MarkFileImportFailed when the import failed.
func (s *Store) MarkFileImportFailed(ctx context.Context, file *model.File) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.MarkFileImportFailed")
	defer telemetry.EndSpan(span, &err)
	setOp, err := s.setFileStateOp(ctx, s.clock.Now(), file, filestate.Failed)
	if err != nil {
		return err
	}
	return setOp.DoOrErr(ctx, s.client)
}

// ScheduleFileForRetry when it is time for the next import attempt.
func (s *Store) ScheduleFileForRetry(ctx context.Context, file *model.File) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.ScheduleFileRetry")
	defer telemetry.EndSpan(span, &err)
	setOp, err := s.setFileStateOp(ctx, s.clock.Now(), file, filestate.Importing)
	if err != nil {
		return err
	}
	return setOp.DoOrErr(ctx, s.client)
}

func (s *Store) CloseFile(ctx context.Context, file *model.File) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.CloseFile")
	defer telemetry.EndSpan(span, &err)

	var stats model.Stats
	return op.
		Atomic().
		Read(func() op.Op {
			return op.MergeToTxn(
				sumStatsOp(s.schema.Slices().Uploaded().InFile(file.FileKey).GetAll(), &stats),
			)
		}).
		WriteOrErr(func() (op.Op, error) {
			var ops []op.Op

			// Copy slice and do modifications
			modFile := *file
			if stats.RecordsCount > 0 {
				modFile.Statistics = &stats
			} else {
				modFile.IsEmpty = true
			}

			// Set file state from "closing" to "importing"
			// This also saves the changes.
			if v, err := s.setFileStateOp(ctx, s.clock.Now(), &modFile, filestate.Importing); err != nil {
				return nil, err
			} else {
				ops = append(ops, v)
			}

			return op.
				MergeToTxn(ops...).
				WithOnResult(func(result op.TxnResult) {
					*file = modFile
				}), nil
		}).
		Do(ctx, s.client)
}

// SwapFile closes the old slice and creates the new one, in the same file.
func (s *Store) SwapFile(ctx context.Context, oldFile *model.File, oldSlice *model.Slice, newFile model.File, newSlice model.Slice) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.SwapFile")
	defer telemetry.EndSpan(span, &err)
	swapOp, err := s.swapFileOp(ctx, s.clock.Now(), oldFile, oldSlice, newFile, newSlice)
	if err != nil {
		return err
	}
	return swapOp.DoOrErr(ctx, s.client)
}

// swapSliceOp closes the old slice and creates the new one, in the same file.
func (s *Store) swapFileOp(ctx context.Context, now time.Time, oldFile *model.File, oldSlice *model.Slice, newFile model.File, newSlice model.Slice) (op.Op, error) {
	if oldFile.FileKey != oldSlice.FileKey {
		panic(errors.Errorf(`slice "%s" is not from the file "%s"`, oldSlice.SliceKey, oldFile.FileKey))
	}
	if newFile.FileKey != newSlice.FileKey {
		panic(errors.Errorf(`slice "%s" is not from the file "%s"`, newSlice.SliceKey, newFile.FileKey))
	}
	if newFile.ExportKey != oldFile.ExportKey {
		panic(errors.Errorf(`new file "%s" is not from the export "%s"`, newFile.FileKey, oldFile.ExportKey))
	}
	createFileOp := s.createFileOp(ctx, newFile)
	closeFileOp, err := s.setFileStateOp(ctx, now, oldFile, filestate.Closing)
	if err != nil {
		return nil, err
	}
	swapSliceOp, err := s.swapSliceOp(ctx, now, oldSlice, newSlice)
	if err != nil {
		return nil, err
	}
	return op.MergeToTxn(createFileOp, closeFileOp, swapSliceOp), nil
}

func (s *Store) setFileStateOp(ctx context.Context, now time.Time, file *model.File, to filestate.State) (*op.TxnOpDef, error) {
	from := file.State
	clone := *file
	stm := filestate.NewSTM(file.State, func(ctx context.Context, from, to filestate.State) error {
		// Update fields
		nowUTC := model.UTCTime(now)
		clone.State = to
		switch to {
		case filestate.Closing:
			clone.ClosingAt = &nowUTC
		case filestate.Importing:
			clone.ImportingAt = &nowUTC
		case filestate.Imported:
			clone.ImportedAt = &nowUTC
		case filestate.Failed:
			clone.FailedAt = &nowUTC
		default:
			panic(errors.Errorf(`unexpected state "%s"`, to))
		}
		return nil
	})

	if err := stm.To(ctx, to); err != nil {
		return nil, err
	}

	// Atomically swap keys in the transaction
	ops := []op.Op{
		s.schema.Files().InState(from).ByKey(file.FileKey).DeleteIfExists(),
		s.schema.Files().InState(to).ByKey(file.FileKey).PutIfNotExists(clone),
	}

	// Create transaction
	txn := op.
		MergeToTxn(ops...).
		WithProcessor(func(_ context.Context, _ *etcd.TxnResponse, result op.TxnResult, err error) error {
			if err == nil {
				*file = clone
			}
			return err
		})
	return txn, nil
}

func (s *Store) UpdateFile(ctx context.Context, file model.File) error {
	return s.schema.
		Files().
		InState(file.State).
		ByKey(file.FileKey).
		Put(file).Do(ctx, s.client)
}
