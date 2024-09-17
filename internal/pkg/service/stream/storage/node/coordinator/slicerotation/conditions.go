package slicerotation

import (
	"fmt"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	stagingConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

type sliceRotationConditionResult struct {
	result int
	cause  string
}

const (
	noConditionMet int = iota
	recordCountThreshold
	sizeThreshold
	timeThreshold
)

func newConditionResult(condition int, message string) sliceRotationConditionResult {
	return sliceRotationConditionResult{
		result: condition,
		cause:  message,
	}
}

func (c sliceRotationConditionResult) ShouldImport() bool {
	return c.result != noConditionMet
}

func (c sliceRotationConditionResult) Cause() string {
	return c.cause
}

func (c sliceRotationConditionResult) String() string {
	switch c.result {
	case noConditionMet:
		return "none"
	case recordCountThreshold:
		return "recordCount"
	case sizeThreshold:
		return "size"
	case timeThreshold:
		return "time"
	default:
		return "unknown"
	}
}

func shouldUpload(cfg stagingConfig.UploadConfig, now, openedAt time.Time, stats statistics.Value) sliceRotationConditionResult {
	sinceOpened := now.Sub(openedAt).Truncate(time.Second)
	if threshold := cfg.MinInterval.Duration(); sinceOpened < threshold {
		// Min interval settings take precedence over other settings.
		return newConditionResult(noConditionMet, "min interval between uploads is not met")
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
