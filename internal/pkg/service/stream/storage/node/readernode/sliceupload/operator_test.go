package sliceupload_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/readernode/sliceupload"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestSliceUpload(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	slicesCheckInterval := time.Second

	volumesPath := t.TempDir()
	volumePath1 := filepath.Join(volumesPath, "hdd", "001")
	require.NoError(t, os.MkdirAll(volumePath1, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(volumePath1, volume.IDFile), []byte("vol-1"), 0o600))
	volumePath2 := filepath.Join(volumesPath, "hdd", "002")
	require.NoError(t, os.MkdirAll(volumePath2, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(volumePath2, volume.IDFile), []byte("vol-2"), 0o600))

	// Create dependencies
	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T00:00:00.000Z").Time())

	d, mock := dependencies.NewMockedStorageReaderScopeWithConfig(t, ctx, func(cfg *config.Config) {
		cfg.Storage.VolumesPath = volumesPath
		cfg.Storage.Level.Staging.Operator.SliceUploadCheckInterval = duration.From(slicesCheckInterval)
	}, commonDeps.WithClock(clk))

	// Start slice upload reader node
	require.NoError(t, sliceupload.Start(d, mock.TestConfig().Storage.Level.Staging.Operator))

	client := mock.TestEtcdClient()
	// Register some volumes
	session, err := concurrency.NewSession(client)
	require.NoError(t, err)
	defer func() { require.NoError(t, session.Close()) }()
	test.RegisterCustomWriterVolumes(t, ctx, d.StorageRepository().Volume(), session, []volume.Metadata{
		{
			ID:   "vol-1",
			Spec: volume.Spec{NodeID: "node", NodeAddress: "localhost:1234", Type: "hdd", Label: "1", Path: "hdd/1"},
		},
		{
			ID:   "vol-2",
			Spec: volume.Spec{NodeID: "node", NodeAddress: "localhost:1234", Type: "hdd", Label: "2", Path: "hdd/2"},
		},
	})

	logger := mock.DebugLogger()
	// Helpers
	waitForSlicesSync := func(t *testing.T) {
		t.Helper()
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"storage.node.operator.slice.upload"}`)
		}, 5*time.Second, 10*time.Millisecond)
	}

	// Fixtures
	branchKey := key.BranchKey{ProjectID: 123, BranchID: 111}
	branch := test.NewBranch(branchKey)
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	source := test.NewHTTPSource(sourceKey)
	sink := dummy.NewSinkWithLocalStorage(key.SinkKey{SourceKey: source.SourceKey, SinkID: "my-keboola-sink"})
	sink.Config = testconfig.LocalVolumeConfig(2, []string{"hdd"})
	require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, clk.Now(), test.ByUser()).Do(ctx).Err())
	require.NoError(t, d.DefinitionRepository().Source().Create(&source, clk.Now(), test.ByUser(), "create").Do(ctx).Err())
	require.NoError(t, d.DefinitionRepository().Sink().Create(&sink, clk.Now(), test.ByUser(), "create").Do(ctx).Err())

	// Prepare file and slice
	files, err := d.StorageRepository().File().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, model.FileWriting, files[0].State)
	slices, err := d.StorageRepository().Slice().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 2)
	require.Equal(t, model.SliceWriting, slices[0].State)
	require.Equal(t, model.SliceWriting, slices[1].State)

	// Prevent duplicate file slice keys
	clk.Add(1 * time.Second)
	require.NoError(t, d.StorageRepository().File().Rotate(sink.SinkKey, clk.Now()).Do(ctx).Err())
	require.NoError(t, d.StorageRepository().Slice().SwitchToUploading(slices[0].SliceKey, clk.Now(), false).Do(ctx).Err())
	logger.Truncate()
	require.NoError(t, d.StorageRepository().Slice().SwitchToUploading(slices[1].SliceKey, clk.Now(), false).Do(ctx).Err())

	// Check that rotation and switch was performed
	files, err = d.StorageRepository().File().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, files, 2)
	require.Equal(t, model.FileClosing, files[0].State)
	require.Equal(t, model.FileWriting, files[1].State)
	slices, err = d.StorageRepository().Slice().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 4)
	require.Equal(t, model.SliceUploading, slices[0].State)
	require.Equal(t, model.SliceUploading, slices[1].State)
	require.Equal(t, model.SliceWriting, slices[2].State)
	require.Equal(t, model.SliceWriting, slices[3].State)

	require.NoError(t, d.StatisticsRepository().Put(ctx, "node", []statistics.PerSlice{{SliceKey: slices[0].SliceKey, RecordsCount: 1}}))
	waitForSlicesSync(t)
	// Triggers slice upload
	clk.Add(slicesCheckInterval)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `{"level":"info","message":"uploading slice"}`)
		logger.AssertJSONMessages(c, `{"level":"info","message":"successfully uploaded slice"}`)
		logger.AssertJSONMessages(c, `{"level":"info","message":"successfully uploaded slice"}`)
	}, 5*time.Second, 10*time.Millisecond)
	logger.Truncate()

	// Test when error on operator occurs
	blaErr := errors.New("bla")
	mock.TestDummySinkController().UploadError = blaErr

	clk.Add(1 * time.Second)
	require.NoError(t, d.StorageRepository().File().Rotate(sink.SinkKey, clk.Now()).Do(ctx).Err())
	logger.Truncate()
	require.NoError(t, d.StorageRepository().Slice().SwitchToUploading(slices[2].SliceKey, clk.Now(), false).Do(ctx).Err())
	require.NoError(t, d.StorageRepository().Slice().SwitchToUploading(slices[3].SliceKey, clk.Now(), true).Do(ctx).Err())

	require.NoError(t, d.StatisticsRepository().Put(ctx, "node", []statistics.PerSlice{{SliceKey: slices[2].SliceKey, RecordsCount: 1}}))
	waitForSlicesSync(t)
	// Triggers slice upload
	clk.Add(slicesCheckInterval)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `{"level":"error","time":"%s","message":"error when waiting for slice upload: bla","component":"storage.node.operator.slice.upload"}`)
	}, 5*time.Second, 10*time.Millisecond)
	logger.Truncate()

	slice, err := d.StorageRepository().Slice().Get(slices[2].SliceKey).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	failedAt := utctime.MustParse("2000-01-01T00:00:04.000Z")
	retryAfter := utctime.MustParse("2000-01-01T00:02:04.000Z")
	assert.Equal(t, model.Retryable{
		RetryAttempt:  1,
		RetryReason:   "error when waiting for slice upload: bla",
		FirstFailedAt: &failedAt,
		LastFailedAt:  &failedAt,
		RetryAfter:    &retryAfter,
	}, slice.Retryable)

	clk.Add(-clk.Now().Sub(retryAfter.Time()) + slicesCheckInterval)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `{"level":"error","time":"%s","message":"error when waiting for slice upload: bla","component":"storage.node.operator.slice.upload"}`)
	}, 5*time.Second, 10*time.Millisecond)
	logger.Truncate()

	slice, err = d.StorageRepository().Slice().Get(slices[2].SliceKey).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	failedAt = utctime.MustParse("2000-01-01T00:00:04.000Z")
	retryAfter = utctime.MustParse("2000-01-01T00:10:05.000Z")
	lastFailed := utctime.MustParse("2000-01-01T00:02:05.000Z")
	assert.Equal(t, model.Retryable{
		RetryAttempt:  2,
		RetryReason:   "error when waiting for slice upload: bla",
		FirstFailedAt: &failedAt,
		LastFailedAt:  &lastFailed,
		RetryAfter:    &retryAfter,
	}, slice.Retryable)

	// Shutdown
	d.Process().Shutdown(ctx, errors.New("bye bye"))
	d.Process().WaitForShutdown()

	// No error is logged
	logger.AssertJSONMessages(t, "")
}
