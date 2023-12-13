package condition

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/usererror"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
)

const (
	sliceSwapTaskType = "slice.swap"
)

func (c *Checker) shouldUpload(ctx context.Context, now time.Time, sliceKey key.SliceKey) (ok bool, reason string, err error) {
	// Check minimal interval
	if interval := now.Sub(sliceKey.OpenedAt()); interval < c.config.MinimalUploadInterval {
		reason = fmt.Sprintf(`interval "%s" is less than the MinimalUploadInterval "%s"`, interval, c.config.MinimalUploadInterval)
		return false, reason, nil
	}

	// Get slice stats
	sliceStats, err := c.cachedStats.SliceStats(ctx, sliceKey)
	if err != nil {
		return false, "", err
	}

	// Evaluate upload conditions
	ok, reason = evaluate(c.config.UploadConditions, now, sliceKey.OpenedAt(), sliceStats.Total)
	return ok, reason, nil
}

func (c *Checker) StartSwapSliceTask(ctx context.Context, fileManager *file.AuthorizedManager, sliceKey key.SliceKey) error {
	return c.tasks.StartTaskOrErr(ctx, task.Config{
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

			if err := fileManager.SwapSlice(ctx, sliceKey); err != nil {
				return task.ErrResult(err)
			}

			return task.OkResult("new slice created, the old is closing")
		},
	})
}
