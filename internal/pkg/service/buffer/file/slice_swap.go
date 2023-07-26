package file

import (
	"context"
	"strings"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/usererror"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	sliceSwapTaskType = "slice.swap"
)

func (m *AuthorizedManager) SwapSlice(sliceKey key.SliceKey, reason string) (err error) {
	return m.tasks.StartTaskOrErr(task.Config{
		Type: sliceSwapTaskType,
		Key: task.Key{
			ProjectID: sliceKey.ProjectID,
			TaskID: task.ID(strings.Join([]string{
				sliceKey.ReceiverID.String(),
				sliceKey.ExportID.String(),
				sliceKey.FileID.String(),
				sliceKey.SliceID.String(),
				sliceSwapTaskType,
			}, "/")),
		},
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), time.Minute)
		},
		Operation: func(ctx context.Context, logger log.Logger) (result task.Result) {
			defer usererror.CheckAndWrap(&result.Error)

			logger.Infof(`closing slice "%s": %s`, sliceKey, reason)

			rb := rollback.New(logger)
			defer rb.InvokeIfErr(ctx, &err)

			// Get export
			export, err := m.store.GetExport(ctx, sliceKey.ExportKey)
			if err != nil {
				return task.ErrResult(errors.Errorf(`cannot close slice "%s": %w`, sliceKey.String(), err))
			}

			oldSlice := export.OpenedSlice
			if oldSlice.SliceKey != sliceKey {
				return task.ErrResult(errors.Errorf(`cannot close slice "%s": unexpected export opened slice "%s"`, sliceKey.String(), oldSlice.FileKey))
			}

			export.OpenedSlice = model.NewSlice(oldSlice.FileKey, m.clock.Now(), oldSlice.Mapping, oldSlice.Number+1, oldSlice.StorageResource)
			if newSlice, err := m.store.SwapSlice(ctx, &oldSlice); err == nil {
				export.OpenedSlice = newSlice
			} else {
				return task.ErrResult(errors.Errorf(`cannot close slice "%s": cannot swap old and new slice: %w`, sliceKey.String(), err))
			}

			return task.OkResult("new slice created, the old is closing")
		},
	})
}
