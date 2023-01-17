package store

import (
	"context"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
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
		case filestate.Closed:
			clone.ClosedAt = &nowUTC
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
