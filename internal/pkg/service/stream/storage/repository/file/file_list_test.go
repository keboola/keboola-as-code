package file_test

import (
	"bytes"
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
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

func TestFileRepository_List(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())
	by := test.ByUser()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}

	// Get services
	d, mocked := dependencies.NewMockedLocalStorageScope(t, commonDeps.WithClock(clk))
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	fileRepo := storageRepo.File()
	volumeRepo := storageRepo.Volume()

	// Log etcd operations
	var etcdLogs bytes.Buffer
	rawClient := d.EtcdClient()
	rawClient.KV = etcdlogger.KVLogWrapper(rawClient.KV, &etcdLogs, etcdlogger.WithMinimal())

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 1)
	}

	// Create parent branch, source, sink, file, slice
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink := test.NewSinkWithLocalStorage(sinkKey)
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(ctx).Err())
	}

	// Create the second file
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, fileRepo.Rotate(sinkKey, clk.Now()).Do(ctx).Err())
	}

	// List files
	// -----------------------------------------------------------------------------------------------------------------
	{
		files, err := fileRepo.ListIn(sinkKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, files, 2)
		require.NotEmpty(t, files[0])
		require.NotEmpty(t, files[1])
		assert.Equal(t, model.FileClosing, files[0].State)
		assert.Equal(t, model.FileWriting, files[1].State)

		result, err := fileRepo.ListIn(projectID).Do(ctx).All()
		require.NoError(t, err)
		require.Equal(t, files, result)
		result, err = fileRepo.ListIn(branchKey).Do(ctx).All()
		require.NoError(t, err)
		require.Equal(t, files, result)
		result, err = fileRepo.ListIn(projectID).Do(ctx).All()
		require.NoError(t, err)
		require.Equal(t, files, result)
		result, err = fileRepo.ListIn(sourceKey).Do(ctx).All()
		require.NoError(t, err)
		require.Equal(t, files, result)
	}
}

func TestFileRepository_ListRecent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())
	by := test.ByUser()

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
	d, mocked := dependencies.NewMockedLocalStorageScope(t, commonDeps.WithClock(clk))
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	fileRepo := storageRepo.File()
	volumeRepo := storageRepo.Volume()

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
		assert.NoError(t, err)
		assert.Empty(t, files)
		files, err = fileRepo.ListRecentIn(sinkKey1).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, files)
	}
	{
		// Get - not found
		if err := fileRepo.Get(nonExistentFileKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `file "123/456/my-source/my-sink-1/2000-01-01T18:00:00.000Z" not found in the sink`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create parent branch, source, sink, files, slices
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink1 := test.NewSinkWithLocalStorage(sinkKey1)
		require.NoError(t, defRepo.Sink().Create(&sink1, clk.Now(), by, "Create sink").Do(ctx).Err())
		sink2 := test.NewSinkWithLocalStorage(sinkKey2)
		require.NoError(t, defRepo.Sink().Create(&sink2, clk.Now(), by, "Create sink").Do(ctx).Err())
	}

	// ListRecentIn
	// -----------------------------------------------------------------------------------------------------------------
	{
		// List
		files, err := fileRepo.ListRecentIn(projectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, files, 2)
		files, err = fileRepo.ListRecentIn(branchKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, files, 2)
		files, err = fileRepo.ListRecentIn(sourceKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, files, 2)
		files, err = fileRepo.ListRecentIn(sinkKey1).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, files, 1)
		files, err = fileRepo.ListRecentIn(sinkKey2).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, files, 1)
	}
}
