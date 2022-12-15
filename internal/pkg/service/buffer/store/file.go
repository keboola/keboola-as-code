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

type fileTxnFailedError struct {
	error
}

func (s *Store) CreateFile(ctx context.Context, file model.File) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.CreateFile")
	defer telemetry.EndSpan(span, &err)

	_, err = s.createFileOp(ctx, file).Do(ctx, s.client)
	return err
}

func (s *Store) createFileOp(_ context.Context, file model.File) op.BoolOp {
	return s.schema.
		Files().
		Opened().
		ByKey(file.FileKey).
		PutIfNotExists(file).
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, ok bool, err error) (bool, error) {
			if !ok && err == nil {
				return false, serviceError.NewResourceAlreadyExistsError("file", file.FileKey.String(), "export")
			}
			return ok, err
		})
}

func (s *Store) GetFile(ctx context.Context, fileKey key.FileKey) (out model.File, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.GetFile")
	defer telemetry.EndSpan(span, &err)

	file, err := s.getFileOp(ctx, fileKey).Do(ctx, s.client)
	if err != nil {
		return out, err
	}
	return file.Value, nil
}

func (s *Store) getFileOp(_ context.Context, fileKey key.FileKey) op.ForType[*op.KeyValueT[model.File]] {
	return s.schema.
		Files().
		Opened().
		ByKey(fileKey).
		Get().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.File], err error) (*op.KeyValueT[model.File], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("file", fileKey.String())
			}
			return kv, err
		})
}

// SetFileState method atomically changes the state of the file.
// False is returned, if the given file is already in the target state.
func (s *Store) SetFileState(ctx context.Context, file *model.File, to filestate.State, now time.Time) (bool, error) { //nolint:dupl
	stm := filestate.NewSTM(file.State, func(ctx context.Context, from, to filestate.State) error {
		// Update fields
		clone := *file
		clone.State = to
		switch to {
		case filestate.Closing:
			clone.ClosingAt = &now
		case filestate.Closed:
			clone.ClosedAt = &now
		case filestate.Importing:
			clone.ImportingAt = &now
		case filestate.Imported:
			clone.ImportedAt = &now
		case filestate.Failed:
			clone.FailedAt = &now
		default:
			panic(errors.Errorf(`unexpected state "%s"`, to))
		}

		// Atomically swap keys in the transaction
		files := s.schema.Files()
		resp, err := op.MergeToTxn(
			ctx,
			files.InState(from).ByKey(file.FileKey).DeleteIfExists(),
			files.InState(to).ByKey(file.FileKey).PutIfNotExists(clone),
		).Do(ctx, s.client)

		// Check logical error, transaction failed
		if err == nil && !resp.Succeeded {
			file.State = to
			return fileTxnFailedError{error: errors.Errorf(
				`transaction "%s" -> "%s" failed: the file "%s" is already in the target state`,
				from, to, file.FileKey.String(),
			)}
		}

		if err == nil {
			*file = clone
		}

		return err
	})

	if err := stm.To(ctx, to); err != nil {
		if errors.As(err, &fileTxnFailedError{}) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
