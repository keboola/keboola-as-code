package quota_test

import (
	"context"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/quota"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestQuota_Check(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Fixtures
	now := time.Now()
	openedAt := utctime.From(now)
	branchKey := key.BranchKey{ProjectID: 123, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-receiver"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-export-1"}
	fileKey := storage.FileKey{SinkKey: sinkKey, FileID: storage.FileID{OpenedAt: openedAt}}
	slice1Key := storage.SliceKey{
		FileVolumeKey: storage.FileVolumeKey{VolumeID: "my-volume-1", FileKey: fileKey},
		SliceID:       storage.SliceID{OpenedAt: openedAt},
	}
	slice2Key := storage.SliceKey{
		FileVolumeKey: storage.FileVolumeKey{VolumeID: "my-volume-2", FileKey: fileKey},
		SliceID:       storage.SliceID{OpenedAt: openedAt},
	}

	// Dependencies
	cfg := config.New()
	d, mocked := dependencies.NewMockedTableSinkScope(t, cfg)
	client := mocked.TestEtcdClient()
	repo := d.StatisticsRepository()
	quoteChecker := quota.New(d)
	updateStats := func(sliceKey storage.SliceKey, size datasize.ByteSize) {
		header := etcdhelper.ExpectModificationInPrefix(t, client, "storage/stats/", func() {
			require.NoError(t, repo.Put(ctx, []statistics.PerSlice{
				{
					SliceKey: sliceKey,
					Value: statistics.Value{
						SlicesCount:    1,
						RecordsCount:   123,
						CompressedSize: size,
					},
				},
			}))
		})

		// Wait for L1 cache update
		assert.Eventually(t, func() bool {
			return d.StatisticsL1Cache().Revision() == header.Revision
		}, 1*time.Second, 100*time.Millisecond)

		// Clear L2 cache
		d.StatisticsL2Cache().Clear()
	}

	// Define a quota
	quotaValue := 1000 * datasize.KB

	// No data, no error
	assert.NoError(t, quoteChecker.Check(ctx, sinkKey, quotaValue))

	// Received some data under quota: 600kB < 1000kB, no error
	updateStats(slice1Key, 600*datasize.KB)
	assert.NoError(t, quoteChecker.Check(ctx, sinkKey, quotaValue))

	// Received some data over quota: 1200kB > 1000kB, error
	expectedErr := `full storage buffer for the sink "123/456/my-receiver/my-export-1", buffered "1.2 MB", quota "1000.0 KB"`
	updateStats(slice2Key, 600*datasize.KB)
	err := quoteChecker.Check(ctx, sinkKey, quotaValue)
	if assert.Error(t, err) {
		assert.Equal(t, expectedErr, err.Error())
		errValue, ok := err.(errors.WithErrorLogEnabled)
		assert.True(t, ok)

		// Error is logged only once per quote.MinErrorLogInterval
		assert.True(t, errValue.ErrorLogEnabled())
	}

	// Error is not logged on second error
	err = quoteChecker.Check(ctx, sinkKey, quotaValue)
	if assert.Error(t, err) {
		assert.Equal(t, expectedErr, err.Error())
		errValue, ok := err.(errors.WithErrorLogEnabled)
		assert.True(t, ok)
		assert.False(t, errValue.ErrorLogEnabled())
	}
}
