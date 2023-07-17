package quota_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive/quota"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestQuota_Check(t *testing.T) {
	t.Parallel()
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Prepare some keys
	now := time.Now()
	receiverKey := key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}
	exportKey1 := key.ExportKey{ReceiverKey: receiverKey, ExportID: "my-export-1"}
	file1Key := key.FileKey{ExportKey: exportKey1, FileID: key.FileID(now.Add(10 * time.Second))}
	slice1Key := key.SliceKey{FileKey: file1Key, SliceID: key.SliceID(now.Add(20 * time.Second))}
	exportKey2 := key.ExportKey{ReceiverKey: receiverKey, ExportID: "my-export-2"}
	file2Key := key.FileKey{ExportKey: exportKey2, FileID: key.FileID(now.Add(30 * time.Second))}
	slice2Key := key.SliceKey{FileKey: file2Key, SliceID: key.SliceID(now.Add(40 * time.Second))}

	limit := 1000 * datasize.KB
	cfg := config.NewAPIConfig()
	cfg.StatisticsSyncInterval = 50 * time.Millisecond
	cfg.ReceiverBufferSize = limit

	apiScope, mock := dependencies.NewMockedAPIScope(t, cfg)
	client := mock.TestEtcdClient()
	statsCollector := apiScope.StatsCollector()
	quoteChecker := quota.New(apiScope)
	notifyReceivedData := func(sliceKey key.SliceKey, recordSize datasize.ByteSize) {
		// Notify statistics collector
		header := etcdhelper.ExpectModificationInPrefix(t, client, "stats/", func() {
			statsCollector.Notify(now, sliceKey, recordSize, 123)
		})

		// Wait for L1 cache update
		assert.Eventually(t, func() bool {
			return apiScope.StatisticsProviders().CachedL1().Revision() == header.Revision
		}, 1*time.Second, 100*time.Millisecond)

		// Clear L2 cache
		apiScope.StatisticsProviders().CachedL2().ClearCache()
	}

	// No data, no error
	assert.NoError(t, quoteChecker.Check(ctx, receiverKey))

	// Received some data under limit: 600kB < 1000kB limit, no error
	notifyReceivedData(slice1Key, 600*datasize.KB)
	assert.NoError(t, quoteChecker.Check(ctx, receiverKey))

	// Received some data over limit: 1200kB > 1000kB limit, error
	notifyReceivedData(slice2Key, 600*datasize.KB)
	err := quoteChecker.Check(ctx, receiverKey)
	if assert.Error(t, err) {
		assert.Equal(t, `no free space in the buffer: receiver "my-receiver" has "1.2 MB" buffered for upload, limit is "1000.0 KB"`, err.Error())
		errValue, ok := err.(errors.WithErrorLogEnabled)
		assert.True(t, ok)
		// Error is logged only once per quote.MinErrorLogInterval
		assert.True(t, errValue.ErrorLogEnabled())
	}

	// Error is not logged on second error
	err = quoteChecker.Check(ctx, receiverKey)
	if assert.Error(t, err) {
		assert.Equal(t, `no free space in the buffer: receiver "my-receiver" has "1.2 MB" buffered for upload, limit is "1000.0 KB"`, err.Error())
		errValue, ok := err.(errors.WithErrorLogEnabled)
		assert.True(t, ok)
		assert.False(t, errValue.ErrorLogEnabled())
	}
}
