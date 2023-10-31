package condition

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/file"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/usererror"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
)

const (
	fileSwapTaskType = "file.swap"
)

func (c *Checker) shouldImport(ctx context.Context, now time.Time, sliceKey key.SliceKey, uploadCredExp time.Time) (ok bool, reason string, err error) {
	// Check minimal interval
	if interval := now.Sub(sliceKey.FileKey.OpenedAt()); interval < c.config.MinimalImportInterval {
		reason = fmt.Sprintf(`interval "%s" is less than the MinimalImportInterval "%s"`, interval, c.config.MinimalImportInterval)
		return false, reason, nil
	}

	// Check credentials CredExpiration
	if uploadCredExp.Sub(now) <= MinimalCredentialsExpiration {
		reason = fmt.Sprintf("upload credentials will expire soon, at %s", uploadCredExp.UTC().String())
		return true, reason, nil
	}

	// Get import conditions
	export, found := c.exports.Get(sliceKey.ExportKey.String())
	if !found {
		reason = "import conditions not found"
		return false, reason, nil
	}

	// Get file stats
	fileStats, err := c.cachedStats.FileStats(ctx, sliceKey.FileKey)
	if err != nil {
		return false, "", err
	}

	// Evaluate import conditions
	ok, reason = evaluate(export.ImportConditions, now, sliceKey.FileKey.OpenedAt(), fileStats.Total)
	return ok, reason, nil
}

func (c *Checker) startSwapFileTask(fileManager *file.AuthorizedManager, fileKey key.FileKey) error {
	return c.tasks.StartTaskOrErr(task.Config{
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

			if err := fileManager.SwapFile(ctx, rb, fileKey); err != nil {
				return task.ErrResult(err)
			}

			return task.OkResult("new file created, the old is closing")
		},
	})
}
