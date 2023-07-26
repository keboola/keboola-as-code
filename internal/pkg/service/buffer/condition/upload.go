package condition

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
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
