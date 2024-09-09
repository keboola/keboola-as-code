package slicerotation

import (
	"fmt"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	stagingConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

func shouldUpload(cfg stagingConfig.UploadConfig, now, openedAt time.Time, stats statistics.Value) (cause string, ok bool) {
	sinceOpened := now.Sub(openedAt).Truncate(time.Second)
	if threshold := cfg.MinInterval.Duration(); sinceOpened < threshold {
		// Min interval settings take precedence over other settings.
		return "min interval between uploads is not met", false
	}

	if stats.RecordsCount == 0 {
		return "no record", false
	}

	if threshold := cfg.Trigger.Count; stats.RecordsCount >= threshold {
		return fmt.Sprintf("count threshold met, records count: %d, threshold: %d", stats.RecordsCount, threshold), true
	}

	if threshold := cfg.Trigger.Size; stats.CompressedSize >= threshold {
		return fmt.Sprintf("size threshold met, compressed size: %s, threshold: %s", stats.CompressedSize.HumanReadable(), threshold.HumanReadable()), true
	}

	if threshold := cfg.Trigger.Interval.Duration(); sinceOpened >= threshold {
		return fmt.Sprintf("time threshold met, opened at: %s, passed: %s threshold: %s", openedAt.Format(utctime.TimeFormat), sinceOpened.String(), threshold.String()), true
	}

	return "no condition met", false
}
