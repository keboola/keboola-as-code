package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestSumStats(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create an etcd prefix for statistics values
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	pfx := etcdop.NewTypedPrefix[statistics.Value]("test/stats", serde.NewJSON(validator.New().Validate))

	// Put values
	assert.NoError(t, pfx.Key("0").Put(client, statistics.Value{}).Do(ctx).Err())

	// Reset value should be subtracted but only at the very end.
	assert.NoError(t, pfx.Key("1").Put(client, statistics.Value{
		ResetAt:          ptr.Ptr(utctime.MustParse("2000-02-01T00:00:00.000Z")),
		SlicesCount:      1,
		FirstRecordAt:    utctime.MustParse("2000-01-15T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-25T00:00:00.000Z"),
		RecordsCount:     5,
		UncompressedSize: 1,
		CompressedSize:   1,
		StagingSize:      1,
	}).Do(ctx).Err())

	assert.NoError(t, pfx.Key("2").Put(client, statistics.Value{
		SlicesCount:      1,
		FirstRecordAt:    utctime.MustParse("2000-01-10T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-20T00:00:00.000Z"),
		RecordsCount:     4,
		UncompressedSize: 2,
		CompressedSize:   1,
	}).Do(ctx).Err())
	assert.NoError(t, pfx.Key("3").Put(client, statistics.Value{
		SlicesCount:      1,
		FirstRecordAt:    utctime.MustParse("2000-01-05T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-20T00:00:00.000Z"),
		RecordsCount:     8,
		UncompressedSize: 4,
		CompressedSize:   2,
	}).Do(ctx).Err())
	assert.NoError(t, pfx.Key("4").Put(client, statistics.Value{
		SlicesCount:      1,
		FirstRecordAt:    utctime.MustParse("2000-01-15T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-25T00:00:00.000Z"),
		RecordsCount:     32,
		UncompressedSize: 16,
		CompressedSize:   3,
		StagingSize:      1,
	}).Do(ctx).Err())

	now, err := time.Parse(time.RFC3339, "2000-02-01T00:00:00.000Z")
	assert.NoError(t, err)

	// Sum
	sum, err := repository.SumStats(ctx, now, pfx.GetAll(client))
	assert.NoError(t, err)
	assert.Equal(t, statistics.Value{
		SlicesCount:      2,
		FirstRecordAt:    utctime.MustParse("2000-01-05T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-25T00:00:00.000Z"),
		RecordsCount:     39,
		UncompressedSize: 21,
		CompressedSize:   5,
		StagingSize:      0,
	}, sum)
}
