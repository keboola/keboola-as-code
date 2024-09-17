package slicerotation

import (
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	stagingConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

func TestShouldUpload(t *testing.T) {
	t.Parallel()

	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	openedBefore10Sec := now.Add(-10 * time.Second)
	openedBefore01Min := now.Add(-1 * time.Minute)
	openedBefore20Min := now.Add(-20 * time.Minute)

	// Defaults
	cfg := stagingConfig.UploadConfig{
		MinInterval: duration.From(15 * time.Second),
		Trigger: stagingConfig.UploadTrigger{
			Count:    10000,
			Size:     5 * datasize.MB,
			Interval: duration.From(5 * time.Minute),
		},
	}

	// Min interval
	condition := shouldUpload(cfg, now, openedBefore10Sec, statistics.Value{})
	assert.Equal(t, noConditionMet, condition.result)
	assert.False(t, condition.ShouldImport())
	assert.Equal(t, "min interval between uploads is not met", condition.Cause())

	// No record
	condition = shouldUpload(cfg, now, openedBefore01Min, statistics.Value{})
	assert.Equal(t, noConditionMet, condition.result)
	assert.False(t, condition.ShouldImport())
	assert.Equal(t, "no record", condition.Cause())

	// No result meet
	condition = shouldUpload(cfg, now, openedBefore01Min, statistics.Value{
		RecordsCount:   50,
		CompressedSize: 1 * datasize.KB,
	})
	assert.Equal(t, noConditionMet, condition.result)
	assert.False(t, condition.ShouldImport())
	assert.Equal(t, "no condition met", condition.Cause())

	// Records count met
	condition = shouldUpload(cfg, now, openedBefore01Min, statistics.Value{
		RecordsCount:   20000,
		CompressedSize: 1 * datasize.MB,
	})
	assert.Equal(t, recordCountThreshold, condition.result)
	assert.True(t, condition.ShouldImport())
	assert.Equal(t, "count threshold met, records count: 20000, threshold: 10000", condition.Cause())

	// Size met
	condition = shouldUpload(cfg, now, openedBefore01Min, statistics.Value{
		RecordsCount:   100,
		CompressedSize: 10 * datasize.MB,
	})
	assert.Equal(t, sizeThreshold, condition.result)
	assert.True(t, condition.ShouldImport())
	assert.Equal(t, "size threshold met, compressed size: 10.0 MB, threshold: 5.0 MB", condition.Cause())

	// Time met
	condition = shouldUpload(cfg, now, openedBefore20Min, statistics.Value{
		RecordsCount:   100,
		CompressedSize: 1 * datasize.KB,
	})
	assert.Equal(t, timeThreshold, condition.result)
	assert.True(t, condition.ShouldImport())
	assert.Equal(t, "time threshold met, opened at: 2000-01-01T00:40:00.000Z, passed: 20m0s threshold: 5m0s", condition.Cause())
}
