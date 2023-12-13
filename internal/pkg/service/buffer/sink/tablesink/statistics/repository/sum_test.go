package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
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
	assert.NoError(t, pfx.Key("1").Put(client, statistics.Value{
		FirstRecordAt:    utctime.MustParse("2000-01-10T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-20T00:00:00.000Z"),
		RecordsCount:     4,
		UncompressedSize: 2,
		CompressedSize:   1,
	}).Do(ctx).Err())
	assert.NoError(t, pfx.Key("2").Put(client, statistics.Value{
		FirstRecordAt:    utctime.MustParse("2000-01-05T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-20T00:00:00.000Z"),
		RecordsCount:     8,
		UncompressedSize: 4,
		CompressedSize:   2,
	}).Do(ctx).Err())
	assert.NoError(t, pfx.Key("3").Put(client, statistics.Value{
		FirstRecordAt:    utctime.MustParse("2000-01-15T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-25T00:00:00.000Z"),
		RecordsCount:     32,
		UncompressedSize: 16,
		CompressedSize:   3,
		StagingSize:      1,
	}).Do(ctx).Err())

	// Sum
	sum, err := repository.SumStats(ctx, pfx.GetAll(client))
	assert.NoError(t, err)
	assert.Equal(t, statistics.Value{
		FirstRecordAt:    utctime.MustParse("2000-01-05T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-25T00:00:00.000Z"),
		RecordsCount:     44,
		UncompressedSize: 22,
		CompressedSize:   6,
		StagingSize:      1,
	}, sum)
}
