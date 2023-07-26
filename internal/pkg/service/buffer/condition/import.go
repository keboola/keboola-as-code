package condition

import (
	"context"
	"fmt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"time"
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
