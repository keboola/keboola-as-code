package diskcleanup_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	sliceRepoPkg "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository/slice"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/writernode/diskcleanup"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
)

func TestDiskCleanup(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	clk := clockwork.NewFakeClockAt(utctime.MustParse("2000-01-01T00:00:00.000Z").Time())
	by := test.ByUser()

	volumeID := volume.ID("my-volume")
	cleanupInterval := 5 * time.Minute
	d, mock := dependencies.NewMockedStorageWriterScopeWithConfig(t, ctx, func(cfg *config.Config) {
		cfg.Storage.DiskCleanup.Interval = cleanupInterval

		// Create volume dir
		volumesPath := cfg.Storage.VolumesPath
		volumePath := filepath.Join(volumesPath, "hdd", "my-volume")
		require.NoError(t, os.MkdirAll(volumePath, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(volumePath, volume.IDFile), []byte(volumeID), 0o640))
	}, commonDeps.WithClock(clk))
	logger := mock.DebugLogger()
	client := mock.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	fileRepo := storageRepo.File()
	sliceRepo := storageRepo.Slice()

	// Register volume - simulate disk writer node
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterCustomWriterVolumes(t, ctx, storageRepo.Volume(), session, []volume.Metadata{
			{
				ID:   volumeID,
				Spec: volume.Spec{NodeID: "node-a", NodeAddress: "localhost:1234", Type: "hdd", Label: "1", Path: "hdd/1"},
			},
		})
	}
	// Start disk cleanup node
	// -----------------------------------------------------------------------------------------------------------------
	require.NoError(t, diskcleanup.Start(d, mock.TestConfig().Storage.DiskCleanup))

	// Prepare doCleanup helper
	// -----------------------------------------------------------------------------------------------------------------
	var doCleanup func()
	{
		cleanupAttempt := 0
		doCleanup = func() {
			cleanupAttempt++
			logger.Truncate()
			clk.Advance(cleanupInterval)
			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				logger.AssertJSONMessages(c, `{"level":"info","message":"removed \"%d\" directories"}`)
			}, 2*time.Second, 100*time.Millisecond)
		}
	}

	// Create parent branch, source, sink, file
	// -----------------------------------------------------------------------------------------------------------------
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink := dummy.NewSinkWithLocalStorage(sinkKey)
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(ctx).Err())
	}

	// Get volume
	// -----------------------------------------------------------------------------------------------------------------
	var vol *diskwriter.Volume
	{
		volumes := d.Volumes().Collection().All()
		require.Len(t, volumes, 1)
		vol = volumes[0]
	}

	// Get slice
	// -----------------------------------------------------------------------------------------------------------------
	var slice model.Slice
	{
		slices, err := sliceRepo.ListIn(sinkKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 1)
		slice = slices[0]
	}

	// Write data to slice
	// -----------------------------------------------------------------------------------------------------------------
	{
		writer, err := vol.OpenWriter("my-source", slice.SliceKey, slice.LocalStorage, false)
		require.NoError(t, err)
		_, err = writer.Write(ctx, true, []byte("foo\n"))
		require.NoError(t, err)
		require.NoError(t, writer.Close(ctx))
		require.DirExists(t, filepath.Join(vol.Path(), slice.LocalStorage.Dir))
	}

	// Create another slice directory, without a record in the DB
	// -----------------------------------------------------------------------------------------------------------------
	{
		unexpectedSliceKey := slice.SliceKey
		unexpectedSliceKey.SinkID = "unexpected-sink"
		unexpectedSliceDir := unexpectedSliceKey.FileKey.String() + string(filepath.Separator) + unexpectedSliceKey.SliceID.String() // without volume ID
		require.Equal(t, sliceRepoPkg.DirPathSegments-1, strings.Count(unexpectedSliceDir, string(filepath.Separator)), unexpectedSliceDir)
		require.NoError(t, os.MkdirAll(filepath.Join(vol.Path(), unexpectedSliceDir), 0o750))
		require.NoError(t, os.WriteFile(filepath.Join(vol.Path(), unexpectedSliceDir, "foo"), []byte("bar\n"), 0o640))
		require.DirExists(t, filepath.Join(vol.Path(), "123", "456", "my-source", "unexpected-sink"))
	}

	// Trigger cleanup
	// -----------------------------------------------------------------------------------------------------------------
	{
		doCleanup()
		logger.AssertJSONMessages(t, `
{"level":"info","message":"removing expired files without DB record from disk","component":"storage.disk.cleanup"}
{"level":"debug","message":"removed directory \"%s/001/hdd/my-volume/123/456/my-source/unexpected-sink/2000-01-01T00:00:00.000Z/2000-01-01T00:00:00.000Z\"","path":"%s/001/hdd/my-volume/123/456/my-source/unexpected-sink/2000-01-01T00:00:00.000Z/2000-01-01T00:00:00.000Z","volume.ID":"my-volume","component":"storage.disk.cleanup"}
{"level":"info","message":"removed \"1\" directories","nodeId":"test-node","removedDirectoriesCount":1,"component":"storage.disk.cleanup"}
`)
		assert.DirExists(t, vol.Path())
		assert.DirExists(t, filepath.Join(vol.Path(), "123", "456", "my-source", "my-sink"))
		assert.NoDirExists(t, filepath.Join(vol.Path(), "123", "456", "my-source", "unexpected-sink"))
	}

	// Trigger cleanup again
	// -----------------------------------------------------------------------------------------------------------------
	{
		doCleanup()
		logger.AssertJSONMessages(t, `
{"level":"info","message":"removing expired files without DB record from disk","component":"storage.disk.cleanup"}
{"level":"info","message":"removed \"0\" directories","nodeId":"test-node","removedDirectoriesCount":0,"component":"storage.disk.cleanup"}
`)
	}

	// Remove slice from DB
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Advance(time.Second)
		require.NoError(t, defRepo.Sink().Disable(sinkKey, clk.Now(), by, "reason").Do(ctx).Err())
		clk.Advance(time.Second)
		require.NoError(t, fileRepo.Delete(slice.FileKey, clk.Now()).Do(ctx).Err())
	}

	// Trigger cleanup
	// -----------------------------------------------------------------------------------------------------------------
	{
		doCleanup()
		logger.AssertJSONMessages(t, `
{"level":"info","message":"removing expired files without DB record from disk","component":"storage.disk.cleanup"}
{"level":"debug","message":"removed directory \"%s/001/hdd/my-volume/123/456/my-source/my-sink/2000-01-01T00-00-00-000Z/2000-01-01T00-00-00-000Z\"","path":"%s/001/hdd/my-volume/123/456/my-source/my-sink/2000-01-01T00-00-00-000Z/2000-01-01T00-00-00-000Z","volume.ID":"my-volume","component":"storage.disk.cleanup"}
{"level":"info","message":"removed \"1\" directories","nodeId":"test-node","removedDirectoriesCount":1,"component":"storage.disk.cleanup"}
`)
		assert.DirExists(t, vol.Path())
		assert.NoDirExists(t, filepath.Join(vol.Path(), "123"))
		assert.NoDirExists(t, filepath.Join(vol.Path(), "123", "456", "my-source", "my-sink"))
	}
}
