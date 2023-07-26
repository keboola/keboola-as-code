package file

import (
	"context"
	"strings"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/usererror"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	fileSwapTaskType = "file.swap"
)

func (m *AuthorizedManager) SwapFile(fileKey key.FileKey, reason string) (err error) {
	return m.tasks.StartTaskOrErr(task.Config{
		Type: fileSwapTaskType,
		Key: task.Key{
			ProjectID: fileKey.ProjectID,
			TaskID: task.ID(strings.Join([]string{
				fileKey.ReceiverID.String(),
				fileKey.ExportID.String(),
				fileKey.FileID.String(),
				fileSwapTaskType,
			}, "/")),
		},
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), time.Minute)
		},
		Operation: func(ctx context.Context, logger log.Logger) (result task.Result) {
			defer usererror.CheckAndWrap(&result.Error)

			rb := rollback.New(logger)
			defer rb.InvokeIfErr(ctx, &result.Error)

			logger.Infof(`closing file "%s": %s`, fileKey, reason)

			// Get export
			export, err := m.store.GetExport(ctx, fileKey.ExportKey)
			if err != nil {
				return task.ErrResult(errors.Errorf(`cannot close file "%s": %w`, fileKey.String(), err))
			}

			oldFile := export.OpenedFile
			if oldFile.FileKey != fileKey {
				return task.ErrResult(errors.Errorf(`cannot close file "%s": unexpected export opened file "%s"`, fileKey.String(), oldFile.FileKey))
			}

			oldSlice := export.OpenedSlice
			if oldSlice.FileKey != fileKey {
				return task.ErrResult(errors.Errorf(`cannot close file "%s": unexpected export opened slice "%s"`, fileKey.String(), oldFile.FileKey))
			}

			if err := m.CreateFileForExport(ctx, rb, &export); err != nil {
				return task.ErrResult(errors.Errorf(`cannot close file "%s": cannot create new file: %w`, fileKey.String(), err))
			}

			if err := m.store.SwapFile(ctx, &oldFile, &oldSlice, export.OpenedFile, export.OpenedSlice); err != nil {
				return task.ErrResult(errors.Errorf(`cannot close file "%s": cannot swap old and new file: %w`, fileKey.String(), err))
			}

			return task.OkResult("new file created, the old is closing")
		},
	})
}
