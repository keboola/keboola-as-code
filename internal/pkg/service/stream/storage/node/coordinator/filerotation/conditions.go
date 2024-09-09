package filerotation

import (
	"fmt"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	targetConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

func shouldImport(cfg targetConfig.ImportConfig, now, openedAt, expiration time.Time, stats statistics.Value) (cause string, ok bool) {
	sinceOpened := now.Sub(openedAt).Truncate(time.Second)
	if threshold := cfg.MinInterval.Duration(); sinceOpened < threshold {
		// Min interval settings take precedence over other settings.
		return "min interval between imports is not met", false
	}

	untilExpiration := expiration.Sub(now).Truncate(time.Second)
	if threshold := cfg.Trigger.Expiration.Duration(); untilExpiration <= threshold {
		return fmt.Sprintf("expiration threshold met, expiration: %s, remains: %s, threshold: %s", expiration.Format(utctime.TimeFormat), untilExpiration.String(), threshold.String()), true
	}

	if threshold := cfg.Trigger.SlicesCount; stats.SlicesCount >= threshold {
		return fmt.Sprintf("slices count threshold met, slices count: %d, threshold: %d", stats.SlicesCount, threshold), true
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
