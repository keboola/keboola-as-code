package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestRepository_Move_SameLevels_Panic(t *testing.T) {
	t.Parallel()

	d := dependencies.NewMocked(t, dependencies.WithEnabledEtcdClient())
	repo := repository.New(d)
	assert.PanicsWithError(t, `from and to categories are same and equal to "staging"`, func() {
		repo.Move(test.NewSliceKey(), storage.LevelStaging, storage.LevelStaging)
	})
}

func TestRepository_Move(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d := dependencies.NewMocked(t, dependencies.WithEnabledEtcdClient())
	client := d.EtcdClient()
	repo := repository.New(d)
	sliceKey := test.NewSliceKeyOpenedAt("2000-01-01T01:00:00.000Z")

	// Move non-existing statistics or empty statistics
	assert.NoError(t, repo.Move(sliceKey, storage.LevelStaging, storage.LevelTarget).Do(ctx).Err())
	etcdhelper.AssertKVsString(t, client, "")

	// Create a record in the storage.LevelLocal
	assert.NoError(t, repo.Put(ctx, []statistics.PerSlice{
		{
			SliceKey: sliceKey,
			Value: statistics.Value{
				FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   1,
			},
		},
	}))
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T01:00:00.000Z/value
-----
{
  "firstRecordAt": "2000-01-01T01:00:00.000Z",
  "lastRecordAt": "2000-01-01T02:00:00.000Z",
  "recordsCount": 1,
  "uncompressedSize": "1B",
  "compressedSize": "1B"
}
>>>>>
`)

	// Move record to the storage.LevelStaging
	addStagingSize := func(value *statistics.Value) {
		value.StagingSize = 1
	}
	assert.NoError(t, repo.Move(sliceKey, storage.LevelLocal, storage.LevelStaging, addStagingSize).Do(ctx).Err())
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/staging/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T01:00:00.000Z/value
-----
{
  "firstRecordAt": "2000-01-01T01:00:00.000Z",
  "lastRecordAt": "2000-01-01T02:00:00.000Z",
  "recordsCount": 1,
  "uncompressedSize": "1B",
  "compressedSize": "1B",
  "stagingSize": "1B"
}
>>>>>
`)

	// Move record to the storage.LevelTarget
	assert.NoError(t, repo.Move(sliceKey, storage.LevelStaging, storage.LevelTarget).Do(ctx).Err())
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/target/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T01:00:00.000Z/value
-----
{
  "firstRecordAt": "2000-01-01T01:00:00.000Z",
  "lastRecordAt": "2000-01-01T02:00:00.000Z",
  "recordsCount": 1,
  "uncompressedSize": "1B",
  "compressedSize": "1B",
  "stagingSize": "1B"
}
>>>>>
`)
}
