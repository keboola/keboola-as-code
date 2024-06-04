package repository_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testconfig"
)

func TestFileRepository_ListRecent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey1 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-2"}
	nonExistentFileKey := model.FileKey{
		SinkKey: sinkKey1,
		FileID:  model.FileID{OpenedAt: utctime.MustParse("2000-01-01T18:00:00.000Z")},
	}

	// Get services
	d, mocked := dependencies.NewMockedTableSinkScope(t, commonDeps.WithClock(clk))
	rb := rollback.New(d.Logger())
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	fileRepo := storageRepo.File()
	sliceRepo := storageRepo.Slice()
	tokenRepo := storageRepo.Token()
	volumeRepo := storageRepo.Volume()

	// Mock file API calls
	transport := mocked.MockedHTTPTransport()
	test.MockCreateFilesStorageAPICalls(t, clk, branchKey, transport)
	test.MockDeleteFilesStorageAPICalls(t, branchKey, transport)

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 5)
	}

	// Empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		// List - empty
		files, err := fileRepo.ListRecentIn(projectID).Do(ctx).AllKVs()
		require.NoError(t, err)
		assert.Empty(t, files)
		files, err = fileRepo.ListRecentIn(sinkKey1).Do(ctx).AllKVs()
		require.NoError(t, err)
		assert.Empty(t, files)
	}
	{
		// Get - not found
		if err := fileRepo.Get(nonExistentFileKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `file "123/456/my-source/my-sink-1/2000-01-01T18:00:00.000Z" not found in the sink`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create (the first Rotate) - parent sink doesn't exists
	// -----------------------------------------------------------------------------------------------------------------
	// Entity exists only in memory
	{
		if err := fileRepo.Rotate(rb, clk.Now(), sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "my-sink-1" not found in the source`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create parent branch, source, sinks and tokens
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(clk.Now(), &branch).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(clk.Now(), "Create source", &source).Do(ctx).Err())

		sink1 := test.NewSink(sinkKey1)
		sink1.Config = sink1.Config.With(testconfig.LocalVolumeConfig(3, []string{"hdd"}))
		require.NoError(t, defRepo.Sink().Create(clk.Now(), "Create sink", &sink1).Do(ctx).Err())

		sink2 := test.NewSink(sinkKey2)
		sink2.Config = sink2.Config.With(testconfig.LocalVolumeConfig(3, []string{"ssd"}))
		require.NoError(t, defRepo.Sink().Create(clk.Now(), "Create sink", &sink2).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink1.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink2.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
	}

	// Create (the first Rotate)
	// See TestFileRepository_Rotate for more rotation tests.
	// -----------------------------------------------------------------------------------------------------------------
	var fileKey1, fileKey2 model.FileKey
	{
		// Create 2 files in different sinks
		clk.Add(time.Hour)
		file1, err := fileRepo.Rotate(rb, clk.Now(), sinkKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.NotNil(t, file1.StagingStorage.UploadCredentials)
		fileKey1 = file1.FileKey

		clk.Add(time.Hour)
		file2, err := fileRepo.Rotate(rb, clk.Now(), sinkKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.NotNil(t, file2.StagingStorage.UploadCredentials)
		fileKey2 = file2.FileKey
	}
	{
		// List
		files, err := fileRepo.ListRecentIn(projectID).Do(ctx).AllKVs()
		require.NoError(t, err)
		assert.Len(t, files, 2)
		files, err = fileRepo.ListRecentIn(branchKey).Do(ctx).AllKVs()
		require.NoError(t, err)
		assert.Len(t, files, 2)
		files, err = fileRepo.ListRecentIn(sourceKey).Do(ctx).AllKVs()
		require.NoError(t, err)
		assert.Len(t, files, 2)
		files, err = fileRepo.ListRecentIn(sinkKey1).Do(ctx).AllKVs()
		require.NoError(t, err)
		assert.Len(t, files, 1)
		files, err = fileRepo.ListRecentIn(sinkKey2).Do(ctx).AllKVs()
		require.NoError(t, err)
		assert.Len(t, files, 1)
	}
	{
		// Get
		result1, err := fileRepo.Get(fileKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "2000-01-01T02:00:00.000Z", result1.OpenedAt().String())
		result2, err := fileRepo.Get(fileKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "2000-01-01T03:00:00.000Z", result2.OpenedAt().String())
	}

	// File rotation has created slices in assigned volumes
	// -----------------------------------------------------------------------------------------------------------------
	var sliceKeys1, sliceKeys2 []model.SliceKey
	{
		// Slices in file1
		sliceID1 := model.SliceID{OpenedAt: fileKey1.OpenedAt()}
		require.NoError(t, sliceRepo.ListIn(fileKey1).Do(ctx).ForEachValue(
			func(value model.Slice, header *iterator.Header) error {
				sliceKeys1 = append(sliceKeys1, value.SliceKey)
				return nil
			},
		))
		assert.Equal(t, []model.SliceKey{
			{SliceID: sliceID1, FileVolumeKey: model.FileVolumeKey{FileKey: fileKey1, VolumeID: "my-volume-1"}}, // hdd
			{SliceID: sliceID1, FileVolumeKey: model.FileVolumeKey{FileKey: fileKey1, VolumeID: "my-volume-3"}}, // hdd
			{SliceID: sliceID1, FileVolumeKey: model.FileVolumeKey{FileKey: fileKey1, VolumeID: "my-volume-5"}}, // hdd
		}, sliceKeys1)

		// Slices in file2
		sliceID2 := model.SliceID{OpenedAt: fileKey2.OpenedAt()}
		require.NoError(t, sliceRepo.ListIn(fileKey2).Do(ctx).ForEachValue(
			func(value model.Slice, header *iterator.Header) error {
				sliceKeys2 = append(sliceKeys2, value.SliceKey)
				return nil
			},
		))
		assert.Equal(t, []model.SliceKey{
			{SliceID: sliceID2, FileVolumeKey: model.FileVolumeKey{FileKey: fileKey2, VolumeID: "my-volume-2"}}, // ssd
			{SliceID: sliceID2, FileVolumeKey: model.FileVolumeKey{FileKey: fileKey2, VolumeID: "my-volume-4"}}, // ssd
			{SliceID: sliceID2, FileVolumeKey: model.FileVolumeKey{FileKey: fileKey2, VolumeID: "my-volume-5"}}, // hdd
		}, sliceKeys2)
	}
}
