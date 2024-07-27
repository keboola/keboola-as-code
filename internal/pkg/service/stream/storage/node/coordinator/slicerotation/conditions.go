package slicerotation

import (
	"fmt"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	stagingConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

func shouldUpload(cfg stagingConfig.UploadTrigger, now, openedAt time.Time, stats statistics.Value) (cause string, ok bool) {
	if stats.RecordsCount == 0 {
		return "no record", false
	}

	if threshold := cfg.Count; stats.RecordsCount >= threshold {
		return fmt.Sprintf("count threshold met, records count: %d, threshold: %d", stats.RecordsCount, threshold), true
	}

	if threshold := cfg.Size; stats.CompressedSize >= threshold {
		return fmt.Sprintf("size threshold met, compressed size: %s, threshold: %s", stats.CompressedSize.HumanReadable(), threshold.HumanReadable()), true
	}

	sinceOpened := now.Sub(openedAt).Truncate(time.Second)
	if threshold := cfg.Interval.Duration(); sinceOpened >= threshold {
		return fmt.Sprintf("time threshold met, opened at: %s, passed: %s threshold: %s", openedAt.Format(utctime.TimeFormat), sinceOpened.String(), threshold.String()), true
	}

	return "no condition met", false
}
