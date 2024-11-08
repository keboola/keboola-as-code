package filerotation

import (
	"fmt"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	targetConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

type fileRotationConditionResult struct {
	result int
	cause  string
}

const (
	noConditionMet int = iota
	expirationThreshold
	sliceCountThreshold
	recordCountThreshold
	sizeThreshold
	timeThreshold
	sinkThrottled
)

func newConditionResult(result int, message string) fileRotationConditionResult {
	return fileRotationConditionResult{
		result: result,
		cause:  message,
	}
}

func (c fileRotationConditionResult) IsSinkThrottled() bool {
	return c.result == sinkThrottled
}

func (c fileRotationConditionResult) ShouldImport() bool {
	return c.result != noConditionMet && c.result != sinkThrottled
}

func (c fileRotationConditionResult) Cause() string {
	return c.cause
}

func (c fileRotationConditionResult) String() string {
	switch c.result {
	case noConditionMet:
		return "none"
	case expirationThreshold:
		return "expiration"
	case sliceCountThreshold:
		return "sliceCount"
	case recordCountThreshold:
		return "recordCount"
	case sizeThreshold:
		return "size"
	case timeThreshold:
		return "time"
	case sinkThrottled:
		return "sinkThrottled"
	default:
		return "unknown"
	}
}

func shouldImport(cfg targetConfig.ImportConfig, now, openedAt, expiration time.Time, stats statistics.Value, sinkLimit int) fileRotationConditionResult {
	sinceOpened := now.Sub(openedAt).Truncate(time.Second)
	if threshold := cfg.MinInterval.Duration(); sinceOpened < threshold {
		// Min interval settings take precedence over other settings.
		return newConditionResult(noConditionMet, "min interval between imports is not met")
	}

	if threshold := cfg.SinkLimit; sinkLimit >= threshold {
		// When sink is throttled take precedence over other settings.
		return newConditionResult(sinkThrottled, "sink is throttled, waiting for next import check")
	}

	untilExpiration := expiration.Sub(now).Truncate(time.Second)
	if threshold := cfg.Trigger.Expiration.Duration(); untilExpiration <= threshold {
		return newConditionResult(expirationThreshold, fmt.Sprintf("expiration threshold met, expiration: %s, remains: %s, threshold: %s", expiration.Format(utctime.TimeFormat), untilExpiration.String(), threshold.String()))
	}

	if threshold := cfg.Trigger.SlicesCount; stats.SlicesCount >= threshold {
		return newConditionResult(sliceCountThreshold, fmt.Sprintf("slices count threshold met, slices count: %d, threshold: %d", stats.SlicesCount, threshold))
	}

	if stats.RecordsCount == 0 {
		return newConditionResult(noConditionMet, "no record")
	}

	if threshold := cfg.Trigger.Count; stats.RecordsCount >= threshold {
		return newConditionResult(recordCountThreshold, fmt.Sprintf("count threshold met, records count: %d, threshold: %d", stats.RecordsCount, threshold))
	}

	if threshold := cfg.Trigger.Size; stats.CompressedSize >= threshold {
		return newConditionResult(sizeThreshold, fmt.Sprintf("size threshold met, compressed size: %s, threshold: %s", stats.CompressedSize.HumanReadable(), threshold.HumanReadable()))
	}

	if threshold := cfg.Trigger.Interval.Duration(); sinceOpened >= threshold {
		return newConditionResult(timeThreshold, fmt.Sprintf("time threshold met, opened at: %s, passed: %s threshold: %s", openedAt.Format(utctime.TimeFormat), sinceOpened.String(), threshold.String()))
	}

	return newConditionResult(noConditionMet, "no condition met")
}
