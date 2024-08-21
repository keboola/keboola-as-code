package fileimport_test

import (
	"context"
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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/coordinator/fileimport"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestFileImport(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The interval triggers importing files check
	importingFilesCheckInterval := time.Second

	// Create dependencies
	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T00:00:00.000Z").Time())
	d, mock := dependencies.NewMockedCoordinatorScopeWithConfig(t, func(cfg *config.Config) {
		cfg.Storage.Level.Target.Operator.FileImportCheckInterval = duration.From(importingFilesCheckInterval)
	}, commonDeps.WithClock(clk))
	logger := mock.DebugLogger()
	client := mock.TestEtcdClient()

	// Start file import coordinator
	require.NoError(t, fileimport.Start(d, mock.TestConfig().Storage.Level.Target.Operator))

	// Register some volumes
	session, err := concurrency.NewSession(client)
	require.NoError(t, err)
	defer func() { require.NoError(t, session.Close()) }()
	test.RegisterWriterVolumes(t, ctx, d.StorageRepository().Volume(), session, 1)

	// Fixtures
	branchKey := key.BranchKey{ProjectID: 123, BranchID: 111}
	branch := test.NewBranch(branchKey)
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	source := test.NewHTTPSource(sourceKey)
	sink := dummy.NewSinkWithLocalStorage(key.SinkKey{SourceKey: source.SourceKey, SinkID: "my-sink"})
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
	require.Len(t, slices, 1)
	require.Equal(t, model.SliceWriting, slices[0].State)

	clk.Add(time.Second)
	require.NoError(t, d.StorageRepository().File().Rotate(sink.SinkKey, clk.Now()).Do(ctx).Err())
	require.NoError(t, d.StorageRepository().Slice().SwitchToUploading(slices[0].SliceKey, clk.Now()).Do(ctx).Err())
	require.NoError(t, d.StorageRepository().Slice().SwitchToUploaded(slices[0].SliceKey, clk.Now(), false).Do(ctx).Err())
	logger.Truncate()
	require.NoError(t, d.StorageRepository().File().SwitchToImporting(files[0].FileKey, clk.Now(), false).Do(ctx).Err())

	// Check state
	files, err = d.StorageRepository().File().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, files, 2)
	require.Equal(t, model.FileImporting, files[0].State)
	require.Equal(t, model.FileWriting, files[1].State)
	slices, err = d.StorageRepository().Slice().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 2)
	require.Equal(t, model.SliceUploaded, slices[0].State)
	require.Equal(t, model.SliceWriting, slices[1].State)

	clk.Add(importingFilesCheckInterval)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"storage.node.operator.file.import"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Check state
	files, err = d.StorageRepository().File().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, files, 2)
	require.Equal(t, model.FileImported, files[0].State)
	require.Equal(t, model.FileWriting, files[1].State)
	slices, err = d.StorageRepository().Slice().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 2)
	require.Equal(t, model.SliceImported, slices[0].State)
	require.Equal(t, model.SliceWriting, slices[1].State)

	// Shutdown
	d.Process().Shutdown(ctx, errors.New("bye bye"))
	d.Process().WaitForShutdown()

	// No error is logged
	logger.AssertJSONMessages(t, "")
}

func TestFileImportError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The interval triggers importing files check
	importingFilesCheckInterval := time.Second

	// Create dependencies
	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T00:00:00.000Z").Time())
	d, mock := dependencies.NewMockedCoordinatorScopeWithConfig(t, func(cfg *config.Config) {
		cfg.Storage.Level.Target.Operator.FileImportCheckInterval = duration.From(importingFilesCheckInterval)
	}, commonDeps.WithClock(clk))
	logger := mock.DebugLogger()
	client := mock.TestEtcdClient()

	// Start file import coordinator
	require.NoError(t, fileimport.Start(d, mock.TestConfig().Storage.Level.Target.Operator))

	// Register some volumes
	session, err := concurrency.NewSession(client)
	require.NoError(t, err)
	defer func() { require.NoError(t, session.Close()) }()
	test.RegisterWriterVolumes(t, ctx, d.StorageRepository().Volume(), session, 1)

	// Fixtures
	branchKey := key.BranchKey{ProjectID: 123, BranchID: 111}
	branch := test.NewBranch(branchKey)
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	source := test.NewHTTPSource(sourceKey)
	sink := dummy.NewSinkWithLocalStorage(key.SinkKey{SourceKey: source.SourceKey, SinkID: "my-sink"})
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
	require.Len(t, slices, 1)
	require.Equal(t, model.SliceWriting, slices[0].State)

	// Fail first file import
	mock.TestDummySinkController().ImportError = errors.New("File import to keboola failed")

	clk.Add(time.Second)
	require.NoError(t, d.StorageRepository().File().Rotate(sink.SinkKey, clk.Now()).Do(ctx).Err())
	require.NoError(t, d.StorageRepository().Slice().SwitchToUploading(slices[0].SliceKey, clk.Now()).Do(ctx).Err())
	require.NoError(t, d.StorageRepository().Slice().SwitchToUploaded(slices[0].SliceKey, clk.Now(), false).Do(ctx).Err())
	logger.Truncate()
	require.NoError(t, d.StorageRepository().File().SwitchToImporting(files[0].FileKey, clk.Now(), false).Do(ctx).Err())

	// Check state
	files, err = d.StorageRepository().File().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, files, 2)
	require.Equal(t, model.FileImporting, files[0].State)
	require.Equal(t, 0, files[0].RetryAttempt)
	require.Nil(t, files[0].FirstFailedAt)
	require.Nil(t, files[0].LastFailedAt)
	require.Nil(t, files[0].RetryAfter)
	require.Equal(t, "", files[0].RetryReason)
	require.Equal(t, model.FileWriting, files[1].State)
	slices, err = d.StorageRepository().Slice().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 2)
	require.Equal(t, model.SliceUploaded, slices[0].State)
	require.Equal(t, model.SliceWriting, slices[1].State)

	clk.Add(importingFilesCheckInterval)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `{"level":"error","message":"error when waiting for file import:\n- File import to keboola failed","component":"storage.node.operator.file.import"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Check state
	files, err = d.StorageRepository().File().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, files, 2)
	require.Equal(t, model.FileImporting, files[0].State)
	require.Equal(t, 1, files[0].RetryAttempt)
	require.NotNil(t, files[0].FirstFailedAt)
	require.NotNil(t, files[0].LastFailedAt)
	require.NotNil(t, files[0].RetryAfter)
	require.Equal(t, "error when waiting for file import:\n- File import to keboola failed", files[0].RetryReason)
	require.Equal(t, model.FileWriting, files[1].State)
	slices, err = d.StorageRepository().Slice().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 2)
	require.Equal(t, model.SliceUploaded, slices[0].State)
	require.Equal(t, model.SliceWriting, slices[1].State)

	// File import retry should succeed
	mock.TestDummySinkController().ImportError = nil
	logger.Truncate()
	clk.Set(files[0].RetryAfter.Time())
	clk.Add(importingFilesCheckInterval)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"storage.node.operator.file.import"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Check state
	files, err = d.StorageRepository().File().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, files, 2)
	require.Equal(t, model.FileImported, files[0].State)
	require.Equal(t, 0, files[0].RetryAttempt)
	require.Nil(t, files[0].FirstFailedAt)
	require.Nil(t, files[0].LastFailedAt)
	require.Nil(t, files[0].RetryAfter)
	require.Equal(t, "", files[0].RetryReason)
	require.Equal(t, model.FileWriting, files[1].State)
	slices, err = d.StorageRepository().Slice().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 2)
	require.Equal(t, model.SliceImported, slices[0].State)
	require.Equal(t, model.SliceWriting, slices[1].State)

	// Shutdown
	d.Process().Shutdown(ctx, errors.New("bye bye"))
	d.Process().WaitForShutdown()

	// No error is logged
	logger.AssertJSONMessages(t, "")
}
