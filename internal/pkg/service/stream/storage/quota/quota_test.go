package quota_test

import (
	"context"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/quota"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
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
	fileKey := model.FileKey{SinkKey: sinkKey, FileID: model.FileID{OpenedAt: openedAt}}
	slice1Key := model.SliceKey{
		FileVolumeKey: model.FileVolumeKey{VolumeID: "my-volume-1", FileKey: fileKey},
		SliceID:       model.SliceID{OpenedAt: openedAt},
	}
	slice2Key := model.SliceKey{
		FileVolumeKey: model.FileVolumeKey{VolumeID: "my-volume-2", FileKey: fileKey},
		SliceID:       model.SliceID{OpenedAt: openedAt},
	}

	// Dependencies
	d, mocked := dependencies.NewMockedCoordinatorScope(t)
	client := mocked.TestEtcdClient()
	repo := d.StatisticsRepository()
	quoteChecker := quota.New(d)
	updateStats := func(sliceKey model.SliceKey, size datasize.ByteSize) {
		header := etcdhelper.ExpectModificationInPrefix(t, client, "storage/stats/", func() {
			require.NoError(t, repo.Put(ctx, "test-node", []statistics.PerSlice{
				{
					SliceKey:       sliceKey,
					RecordsCount:   123,
					CompressedSize: size,
				},
			}))
		})

		// Wait for L1 cache update
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, header.Revision, d.StatisticsL1Cache().Revision())
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
