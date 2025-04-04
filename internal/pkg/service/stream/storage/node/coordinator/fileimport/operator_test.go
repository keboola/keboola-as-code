package fileimport_test

import (
	"context"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcdPkg "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/coordinator/fileimport"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestFileImport(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	ts := setup(t, ctx)
	defer ts.teardown(t)
	ts.prepareFixtures(t, ctx)
	file := ts.getFile(t, ctx)
	slice := ts.getSlice(t, ctx)

	ts.clk.Advance(time.Second)
	require.NoError(t, ts.dependencies.StorageRepository().File().Rotate(ts.sink.SinkKey, ts.clk.Now()).Do(ctx).Err())
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploading(slice.SliceKey, ts.clk.Now(), false).Do(ctx).Err())
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploaded(slice.SliceKey, ts.clk.Now()).Do(ctx).Err())

	// Clear logs before this change so that we wait for mirror sync
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

	// Wait for ETCD mirror to sync to latest state
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"storage.node.operator.file.import"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Trigger import
	ts.clk.Advance(ts.interval)

	// Await import success
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `{"level":"info","message":"importing file","file.id":"2000-01-01T00:00:00.000Z","component":"storage.node.operator.file.import"}`)
		ts.logger.AssertJSONMessages(c, `{"level":"info","message":"imported file","file.id":"2000-01-01T00:00:00.000Z","component":"storage.node.operator.file.import"}`)
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
	ts.logger.AssertNoErrorMessage(t)
}

func TestFileImportError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	ts := setup(t, ctx)
	defer ts.teardown(t)
	ts.prepareFixtures(t, ctx)
	file := ts.getFile(t, ctx)
	slice := ts.getSlice(t, ctx)

	// Fail first file import
	ts.mock.TestDummySinkController().ImportError = errors.New("File import to keboola failed")

	ts.clk.Advance(time.Second)
	require.NoError(t, ts.dependencies.StorageRepository().File().Rotate(ts.sink.SinkKey, ts.clk.Now()).Do(ctx).Err())
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploading(slice.SliceKey, ts.clk.Now(), false).Do(ctx).Err())
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploaded(slice.SliceKey, ts.clk.Now()).Do(ctx).Err())

	// Clear logs before this change so that we wait for mirror sync
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
	assert.Empty(t, files[0].RetryReason)
	assert.Equal(t, model.FileWriting, files[1].State)
	slices, err := ts.dependencies.StorageRepository().Slice().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	assert.Len(t, slices, 2)
	assert.Equal(t, model.SliceUploaded, slices[0].State)
	assert.Equal(t, model.SliceWriting, slices[1].State)

	// Wait for ETCD mirror to sync to latest state
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"storage.node.operator.file.import"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Trigger import
	ts.clk.Advance(ts.interval)

	// Await import error
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `
{"level":"error","message":"file import failed: File import to keboola failed","file.id":"2000-01-01T00:00:00.000Z","component":"storage.node.operator.file.import"}
{"level":"info","message":"file import will be retried after \"2000-01-01T00:02:02.000Z\"","file.id":"2000-01-01T00:00:00.000Z","component":"storage.node.operator.file.import"}
`)
	}, 5*time.Second, 10*time.Millisecond)

	// Check state
	failedAt := utctime.MustParse("2000-01-01T00:00:02.000Z")
	retryAfter := utctime.MustParse("2000-01-01T00:02:02.000Z")
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		files, err = ts.dependencies.StorageRepository().File().ListIn(ts.sink.SinkKey).Do(ctx).All()
		require.NoError(c, err)
		assert.Len(c, files, 2)
		assert.Equal(c, model.FileImporting, files[0].State)
		assert.Equal(c, 1, files[0].RetryAttempt)
		assert.Equal(c, &failedAt, files[0].FirstFailedAt)
		assert.Equal(c, &failedAt, files[0].LastFailedAt)
		assert.Equal(c, &retryAfter, files[0].RetryAfter)
		assert.Equal(c, "file import failed: File import to keboola failed", files[0].RetryReason)
		assert.Equal(c, model.FileWriting, files[1].State)
		slices, err = ts.dependencies.StorageRepository().Slice().ListIn(ts.sink.SinkKey).Do(ctx).All()
		require.NoError(c, err)
		assert.Len(c, slices, 2)
		assert.Equal(c, model.SliceUploaded, slices[0].State)
		assert.Equal(c, model.SliceWriting, slices[1].State)
	}, 5*time.Second, 10*time.Millisecond)

	// File import retry should succeed
	ts.mock.TestDummySinkController().ImportError = nil
	ts.logger.Truncate()
	ts.clk.Advance(files[0].RetryAfter.Time().Sub(ts.clk.Now()))
	ts.clk.Advance(ts.interval)

	// Await import success
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `{"level":"info","message":"imported file","file.id":"2000-01-01T00:00:00.000Z","component":"storage.node.operator.file.import"}`)
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
	assert.Empty(t, files[0].RetryReason)
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
	ts.logger.AssertNoErrorMessage(t)
}

func TestFileImportDisabledSink(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	ts := setup(t, ctx)
	defer ts.teardown(t)
	ts.prepareFixtures(t, ctx)
	file := ts.getFile(t, ctx)
	slice := ts.getSlice(t, ctx)

	// Make plugin throw an error, it should not be called during this test
	ts.mock.TestDummySinkController().ImportError = errors.New("File import to keboola failed")
	// Disable sink, this causes fileimport operator to skip the files in this sink
	require.NoError(t, ts.dependencies.DefinitionRepository().Sink().Disable(ts.sink.SinkKey, ts.clk.Now(), test.ByUser(), "create").Do(ctx).Err())

	ts.clk.Advance(time.Second)
	require.NoError(t, ts.dependencies.StorageRepository().File().Rotate(ts.sink.SinkKey, ts.clk.Now()).Do(ctx).Err())
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploading(slice.SliceKey, ts.clk.Now(), false).Do(ctx).Err())
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploaded(slice.SliceKey, ts.clk.Now()).Do(ctx).Err())

	// Clear logs before this change so that we wait for mirror sync
	ts.logger.Truncate()
	require.NoError(t, ts.dependencies.StorageRepository().File().SwitchToImporting(file.FileKey, ts.clk.Now(), false).Do(ctx).Err())

	// Wait for ETCD mirror to sync to latest state
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"storage.node.operator.file.import"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Trigger import
	ts.clk.Advance(ts.interval)

	// Shutdown
	ts.dependencies.Process().Shutdown(ctx, errors.New("bye bye"))
	ts.dependencies.Process().WaitForShutdown()

	// No error is logged
	ts.logger.AssertNoErrorMessage(t)
}

type testState struct {
	interval     time.Duration
	clk          *clockwork.FakeClock
	logger       log.DebugLogger
	client       *etcdPkg.Client
	mock         dependencies.Mocked
	dependencies dependencies.CoordinatorScope
	session      *concurrency.Session
	sink         definition.Sink
}

func setup(t *testing.T, ctx context.Context) *testState {
	t.Helper()

	// The interval triggers importing files check
	importingFilesCheckInterval := time.Second

	// Create dependencies
	clk := clockwork.NewFakeClockAt(utctime.MustParse("2000-01-01T00:00:00.000Z").Time())
	d, mock := dependencies.NewMockedCoordinatorScopeWithConfig(t, ctx, func(cfg *config.Config) {
		cfg.Storage.Level.Target.Operator.FileImportCheckInterval = duration.From(importingFilesCheckInterval)
	}, commonDeps.WithClock(clk))

	// Start file import coordinator
	require.NoError(t, fileimport.Start(d, mock.TestConfig().Storage.Level.Target.Operator))

	client := mock.TestEtcdClient()

	// Register some volumes
	session, err := concurrency.NewSession(client)
	require.NoError(t, err)
	test.RegisterWriterVolumes(t, ctx, d.StorageRepository().Volume(), session, 1)

	return &testState{
		interval:     importingFilesCheckInterval,
		clk:          clk,
		mock:         mock,
		logger:       mock.DebugLogger(),
		client:       client,
		dependencies: d,
		session:      session,
	}
}

func (ts *testState) teardown(t *testing.T) {
	t.Helper()
	require.NoError(t, ts.session.Close())
}

func (ts *testState) prepareFixtures(t *testing.T, ctx context.Context) {
	t.Helper()
	branchKey := key.BranchKey{ProjectID: 123, BranchID: 111}
	branch := test.NewBranch(branchKey)
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	source := test.NewHTTPSource(sourceKey)
	ts.sink = dummy.NewSinkWithLocalStorage(key.SinkKey{SourceKey: source.SourceKey, SinkID: "my-sink"})
	require.NoError(t, ts.dependencies.DefinitionRepository().Branch().Create(&branch, ts.clk.Now(), test.ByUser()).Do(ctx).Err())
	require.NoError(t, ts.dependencies.DefinitionRepository().Source().Create(&source, ts.clk.Now(), test.ByUser(), "create").Do(ctx).Err())
	require.NoError(t, ts.dependencies.DefinitionRepository().Sink().Create(&ts.sink, ts.clk.Now(), test.ByUser(), "create").Do(ctx).Err())
}

func (ts *testState) getFile(t *testing.T, ctx context.Context) model.File {
	t.Helper()
	files, err := ts.dependencies.StorageRepository().File().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, model.FileWriting, files[0].State)
	return files[0]
}

func (ts *testState) getSlice(t *testing.T, ctx context.Context) model.Slice {
	t.Helper()
	slices, err := ts.dependencies.StorageRepository().Slice().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 1)
	require.Equal(t, model.SliceWriting, slices[0].State)
	return slices[0]
}
