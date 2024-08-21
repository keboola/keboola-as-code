package fileimport_test

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcdPkg "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/coordinator/fileimport"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type testState struct {
	interval time.Duration
	clk *clock.Mock
	logger log.DebugLogger
	client *etcdPkg.Client
	mock dependencies.Mocked
	dependencies dependencies.CoordinatorScope

	sink definition.Sink
}

func setup(t *testing.T, ctx context.Context) (ts *testState) {
	// The interval triggers importing files check
	importingFilesCheckInterval := time.Second

	// Create dependencies
	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T00:00:00.000Z").Time())
	d, mock := dependencies.NewMockedCoordinatorScopeWithConfig(t, func(cfg *config.Config) {
		cfg.Storage.Level.Target.Operator.FileImportCheckInterval = duration.From(importingFilesCheckInterval)
	}, commonDeps.WithClock(clk))

	ts = &testState{
		interval: importingFilesCheckInterval,
		clk: clk,
		mock: mock,
		logger: mock.DebugLogger(),
		client: mock.TestEtcdClient(),
		dependencies: d,
	}

	// Start file import coordinator
	require.NoError(t, fileimport.Start(d, mock.TestConfig().Storage.Level.Target.Operator))

	// Register some volumes
	session, err := concurrency.NewSession(ts.client)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close()) })
	test.RegisterWriterVolumes(t, ctx, d.StorageRepository().Volume(), session, 1)

	return
}

func (ts *testState) prepareFixtures(t *testing.T, ctx context.Context) {
	branchKey := key.BranchKey{ProjectID: 123, BranchID: 111}
	branch := test.NewBranch(branchKey)
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	source := test.NewHTTPSource(sourceKey)
	ts.sink = dummy.NewSinkWithLocalStorage(key.SinkKey{SourceKey: source.SourceKey, SinkID: "my-sink"})
	require.NoError(t, ts.dependencies.DefinitionRepository().Branch().Create(&branch, ts.clk.Now(), test.ByUser()).Do(ctx).Err())
	require.NoError(t, ts.dependencies.DefinitionRepository().Source().Create(&source, ts.clk.Now(), test.ByUser(), "create").Do(ctx).Err())
	require.NoError(t, ts.dependencies.DefinitionRepository().Sink().Create(&ts.sink, ts.clk.Now(), test.ByUser(), "create").Do(ctx).Err())
}

func (ts *testState) prepareFile(t *testing.T, ctx context.Context) model.File {
	files, err := ts.dependencies.StorageRepository().File().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, model.FileWriting, files[0].State)
	return files[0]
}

func (ts *testState) prepareSlice(t *testing.T, ctx context.Context) model.Slice {
	slices, err := ts.dependencies.StorageRepository().Slice().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 1)
	require.Equal(t, model.SliceWriting, slices[0].State)
	return slices[0]
}

func TestFileImport(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	ts := setup(t, ctx)
	ts.prepareFixtures(t, ctx)
	file := ts.prepareFile(t, ctx)
	slice := ts.prepareSlice(t, ctx)

	ts.clk.Add(time.Second)
	require.NoError(t, ts.dependencies.StorageRepository().File().Rotate(ts.sink.SinkKey, ts.clk.Now()).Do(ctx).Err())
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploading(slice.SliceKey, ts.clk.Now()).Do(ctx).Err())
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploaded(slice.SliceKey, ts.clk.Now(), false).Do(ctx).Err())
	ts.logger.Truncate()
	require.NoError(t, ts.dependencies.StorageRepository().File().SwitchToImporting(file.FileKey, ts.clk.Now(), false).Do(ctx).Err())

	// Check state
	files, err := ts.dependencies.StorageRepository().File().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	assert.Len(t, files, 2)
	assert.Equal(t, model.FileImporting, files[0].State)
	assert.Equal(t, model.FileWriting, files[1].State)
	slices, err := ts.dependencies.StorageRepository().Slice().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	assert.Len(t, slices, 2)
	assert.Equal(t, model.SliceUploaded, slices[0].State)
	assert.Equal(t, model.SliceWriting, slices[1].State)

	ts.clk.Add(ts.interval)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"storage.node.operator.file.import"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Check state
	files, err = ts.dependencies.StorageRepository().File().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	assert.Len(t, files, 2)
	assert.Equal(t, model.FileImported, files[0].State)
	assert.Equal(t, model.FileWriting, files[1].State)
	slices, err = ts.dependencies.StorageRepository().Slice().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	assert.Len(t, slices, 2)
	assert.Equal(t, model.SliceImported, slices[0].State)
	assert.Equal(t, model.SliceWriting, slices[1].State)

	// Shutdown
	ts.dependencies.Process().Shutdown(ctx, errors.New("bye bye"))
	ts.dependencies.Process().WaitForShutdown()

	// No error is logged
	ts.logger.AssertJSONMessages(t, "")
}

func TestFileImportError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	ts := setup(t, ctx)
	ts.prepareFixtures(t, ctx)
	file := ts.prepareFile(t, ctx)
	slice := ts.prepareSlice(t, ctx)

	// Fail first file import
	ts.mock.TestDummySinkController().ImportError = errors.New("File import to keboola failed")

	ts.clk.Add(time.Second)
	require.NoError(t, ts.dependencies.StorageRepository().File().Rotate(ts.sink.SinkKey, ts.clk.Now()).Do(ctx).Err())
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploading(slice.SliceKey, ts.clk.Now()).Do(ctx).Err())
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploaded(slice.SliceKey, ts.clk.Now(), false).Do(ctx).Err())
	ts.logger.Truncate()
	require.NoError(t, ts.dependencies.StorageRepository().File().SwitchToImporting(file.FileKey, ts.clk.Now(), false).Do(ctx).Err())

	// Check state
	files, err := ts.dependencies.StorageRepository().File().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	assert.Len(t, files, 2)
	assert.Equal(t, model.FileImporting, files[0].State)
	assert.Equal(t, 0, files[0].RetryAttempt)
	assert.Nil(t, files[0].FirstFailedAt)
	assert.Nil(t, files[0].LastFailedAt)
	assert.Nil(t, files[0].RetryAfter)
	assert.Equal(t, "", files[0].RetryReason)
	assert.Equal(t, model.FileWriting, files[1].State)
	slices, err := ts.dependencies.StorageRepository().Slice().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	assert.Len(t, slices, 2)
	assert.Equal(t, model.SliceUploaded, slices[0].State)
	assert.Equal(t, model.SliceWriting, slices[1].State)

	ts.clk.Add(ts.interval)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `{"level":"error","message":"error when waiting for file import:\n- File import to keboola failed","component":"storage.node.operator.file.import"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Check state
	files, err = ts.dependencies.StorageRepository().File().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	assert.Len(t, files, 2)
	assert.Equal(t, model.FileImporting, files[0].State)
	assert.Equal(t, 1, files[0].RetryAttempt)
	assert.NotNil(t, files[0].FirstFailedAt)
	assert.NotNil(t, files[0].LastFailedAt)
	assert.NotNil(t, files[0].RetryAfter)
	assert.Equal(t, "error when waiting for file import:\n- File import to keboola failed", files[0].RetryReason)
	assert.Equal(t, model.FileWriting, files[1].State)
	slices, err = ts.dependencies.StorageRepository().Slice().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	assert.Len(t, slices, 2)
	assert.Equal(t, model.SliceUploaded, slices[0].State)
	assert.Equal(t, model.SliceWriting, slices[1].State)

	// File import retry should succeed
	ts.mock.TestDummySinkController().ImportError = nil
	ts.logger.Truncate()
	ts.clk.Set(files[0].RetryAfter.Time())
	ts.clk.Add(ts.interval)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"storage.node.operator.file.import"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Check state
	files, err = ts.dependencies.StorageRepository().File().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	assert.Len(t, files, 2)
	assert.Equal(t, model.FileImported, files[0].State)
	assert.Equal(t, 0, files[0].RetryAttempt)
	assert.Nil(t, files[0].FirstFailedAt)
	assert.Nil(t, files[0].LastFailedAt)
	assert.Nil(t, files[0].RetryAfter)
	assert.Equal(t, "", files[0].RetryReason)
	assert.Equal(t, model.FileWriting, files[1].State)
	slices, err = ts.dependencies.StorageRepository().Slice().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	assert.Len(t, slices, 2)
	assert.Equal(t, model.SliceImported, slices[0].State)
	assert.Equal(t, model.SliceWriting, slices[1].State)

	// Shutdown
	ts.dependencies.Process().Shutdown(ctx, errors.New("bye bye"))
	ts.dependencies.Process().WaitForShutdown()

	// No error is logged
	ts.logger.AssertJSONMessages(t, "")
}

func TestFileImportEmpty(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	ts := setup(t, ctx)
	ts.prepareFixtures(t, ctx)
	file := ts.prepareFile(t, ctx)
	slice := ts.prepareSlice(t, ctx)

	// Import should pass regardless of this error because the file is empty
	ts.mock.TestDummySinkController().ImportError = errors.New("File import to keboola failed")

	ts.clk.Add(time.Second)
	require.NoError(t, ts.dependencies.StorageRepository().File().Rotate(ts.sink.SinkKey, ts.clk.Now()).Do(ctx).Err())
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploading(slice.SliceKey, ts.clk.Now()).Do(ctx).Err())
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploaded(slice.SliceKey, ts.clk.Now(), true).Do(ctx).Err())
	ts.logger.Truncate()
	require.NoError(t, ts.dependencies.StorageRepository().File().SwitchToImporting(file.FileKey, ts.clk.Now(), true).Do(ctx).Err())

	// Check state
	files, err := ts.dependencies.StorageRepository().File().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	assert.Len(t, files, 2)
	assert.Equal(t, model.FileImporting, files[0].State)
	assert.Equal(t, model.FileWriting, files[1].State)
	slices, err := ts.dependencies.StorageRepository().Slice().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	assert.Len(t, slices, 2)
	assert.Equal(t, model.SliceUploaded, slices[0].State)
	assert.Equal(t, model.SliceWriting, slices[1].State)

	ts.clk.Add(ts.interval)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"storage.node.operator.file.import"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Check state
	files, err = ts.dependencies.StorageRepository().File().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	assert.Len(t, files, 2)
	assert.Equal(t, model.FileImported, files[0].State)
	assert.Equal(t, model.FileWriting, files[1].State)
	slices, err = ts.dependencies.StorageRepository().Slice().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	assert.Len(t, slices, 2)
	assert.Equal(t, model.SliceImported, slices[0].State)
	assert.Equal(t, model.SliceWriting, slices[1].State)

	// Shutdown
	ts.dependencies.Process().Shutdown(ctx, errors.New("bye bye"))
	ts.dependencies.Process().WaitForShutdown()

	// No error is logged
	ts.logger.AssertJSONMessages(t, "")
}
