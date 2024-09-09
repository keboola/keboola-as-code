package filerotation

import (
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	targetConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

func TestShouldImport(t *testing.T) {
	t.Parallel()

	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	openedBefore30Sec := now.Add(-30 * time.Second)
	openedBefore01Min := now.Add(-1 * time.Minute)
	openedBefore20Min := now.Add(-20 * time.Minute)
	expirationIn60min := now.Add(60 * time.Minute)
	expirationIn05min := now.Add(5 * time.Minute)

	// Defaults
	cfg := targetConfig.ImportConfig{
		MinInterval: duration.From(60 * time.Second),
		Trigger: targetConfig.ImportTrigger{
			Count:       10000,
			Size:        5 * datasize.MB,
			Interval:    duration.From(5 * time.Minute),
			SlicesCount: 10,
			Expiration:  duration.From(15 * time.Minute),
		},
	}

	// Min interval
	cause, ok := shouldImport(cfg, now, openedBefore30Sec, expirationIn60min, statistics.Value{})
	assert.False(t, ok)
	assert.Equal(t, "min interval between imports is not met", cause)

	// No record
	cause, ok = shouldImport(cfg, now, openedBefore01Min, expirationIn60min, statistics.Value{})
	assert.False(t, ok)
	assert.Equal(t, "no record", cause)

	// No condition meet
	cause, ok = shouldImport(cfg, now, openedBefore01Min, expirationIn60min, statistics.Value{
		RecordsCount:   50,
		CompressedSize: 1 * datasize.KB,
	})
	assert.False(t, ok)
	assert.Equal(t, "no condition met", cause)

	// Remaining expiration time meet
	cause, ok = shouldImport(cfg, now, openedBefore01Min, expirationIn05min, statistics.Value{
		RecordsCount:   50,
		CompressedSize: 1 * datasize.KB,
	})
	assert.True(t, ok)
	assert.Equal(t, "expiration threshold met, expiration: 2000-01-01T01:05:00.000Z, remains: 5m0s, threshold: 15m0s", cause)

	// Slices count meet
	cause, ok = shouldImport(cfg, now, openedBefore01Min, expirationIn60min, statistics.Value{
		RecordsCount: 1000,
		SlicesCount:  20,
	})
	assert.True(t, ok)
	assert.Equal(t, "slices count threshold met, slices count: 20, threshold: 10", cause)

	// Records count met
	cause, ok = shouldImport(cfg, now, openedBefore01Min, expirationIn60min, statistics.Value{
		RecordsCount:   20000,
		CompressedSize: 1 * datasize.MB,
	})
	assert.True(t, ok)
	assert.Equal(t, "count threshold met, records count: 20000, threshold: 10000", cause)

	// Size met
	cause, ok = shouldImport(cfg, now, openedBefore01Min, expirationIn60min, statistics.Value{
		RecordsCount:   100,
		CompressedSize: 10 * datasize.MB,
	})
	assert.True(t, ok)
	assert.Equal(t, "size threshold met, compressed size: 10.0 MB, threshold: 5.0 MB", cause)

	// Time met
	cause, ok = shouldImport(cfg, now, openedBefore20Min, expirationIn60min, statistics.Value{
		RecordsCount:   100,
		CompressedSize: 1 * datasize.KB,
	})
	assert.True(t, ok)
	assert.Equal(t, "time threshold met, opened at: 2000-01-01T00:40:00.000Z, passed: 20m0s threshold: 5m0s", cause)
}
