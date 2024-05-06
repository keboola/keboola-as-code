package state_test

import (
	"context"
	"strings"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	deps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testconfig"
)

func TestRepository_FileAndSliceStateTransitions(t *testing.T) {
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
	d, mocked := dependencies.NewMockedLocalStorageScope(t, deps.WithClock(clk))
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	fileRepo := storageRepo.File()
	sliceRepo := storageRepo.Slice()
	volumeRepo := storageRepo.Volume()

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 5)
	}

	// Create parent branch, source, sink and token
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink := test.NewSink(sinkKey)
		sink.Config = sink.Config.With(testconfig.LocalVolumeConfig(3, []string{"default"}))
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(ctx).Err())
	}

	// Create file (the first rotate) - with 3 slices, see Sink config above
	// -----------------------------------------------------------------------------------------------------------------
	var file model.File
	var sliceKey1, sliceKey2, sliceKey3 model.SliceKey
	{
		var err error
		file, err = fileRepo.Rotate(sinkKey, clk.Now()).Do(ctx).ResultOrErr()
		require.NoError(t, err)

		slices, err := sliceRepo.ListIn(file.FileKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 3)
		sliceKey1 = slices[0].SliceKey
		sliceKey2 = slices[1].SliceKey
		sliceKey3 = slices[2].SliceKey
	}

	// INVALID: SliceUploading - invalid transition
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := sliceRepo.StateTransition(sliceKey1, clk.Now(), model.SliceWriting, model.SliceUploading).Do(ctx).Err()
		require.Error(t, err)
		assert.Equal(t, `unexpected slice "123/456/my-source/my-sink/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z" state transition from "writing" to "uploading"`, err.Error())
	}

	// INVALID: FileImporting - invalid transition
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := fileRepo.StateTransition(file.FileKey, clk.Now(), model.FileWriting, model.FileImporting).Do(ctx).Err()
		require.Error(t, err)
		assert.Equal(t, `unexpected file "123/456/my-source/my-sink/2000-01-01T01:00:00.000Z" state transition from "writing" to "importing"`, err.Error())
	}

	// VALID: SliceClosing
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, sliceRepo.Close(clk.Now(), sliceKey1.FileVolumeKey).Do(ctx).Err())
		require.NoError(t, sliceRepo.Close(clk.Now(), sliceKey2.FileVolumeKey).Do(ctx).Err())
		slice1KV, err := sliceRepo.Get(sliceKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.SliceClosing, slice1KV.State)
		slice2KV, err := sliceRepo.Get(sliceKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.SliceClosing, slice2KV.State)
	}

	// VALID: FileClosing, slices in SliceWriting state are switched to SliceClosing state
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, fileRepo.Close(sinkKey, clk.Now()).Do(ctx).Err())
		fileKV, err := fileRepo.Get(file.FileKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.FileClosing, fileKV.State)
		slice3KV, err := sliceRepo.Get(sliceKey3).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.SliceClosing, slice3KV.State)
	}

	// INVALID: from argument doesn't match
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := sliceRepo.StateTransition(sliceKey1, clk.Now(), model.SliceWriting, model.SliceUploading).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `slice "123/456/my-source/my-sink/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z" is in "closing" state, expected "writing"`, err.Error())
		}
	}

	// VALID: SliceUploading
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, sliceRepo.StateTransition(sliceKey1, clk.Now(), model.SliceClosing, model.SliceUploading).Do(ctx).Err())
		require.NoError(t, sliceRepo.StateTransition(sliceKey2, clk.Now(), model.SliceClosing, model.SliceUploading).Do(ctx).Err())
		slice1KV, err := sliceRepo.Get(sliceKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.SliceUploading, slice1KV.State)
		slice2KV, err := sliceRepo.Get(sliceKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.SliceUploading, slice2KV.State)
	}

	// VALID: SliceUploaded
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, sliceRepo.StateTransition(sliceKey1, clk.Now(), model.SliceUploading, model.SliceUploaded).Do(ctx).Err())
		require.NoError(t, sliceRepo.StateTransition(sliceKey2, clk.Now(), model.SliceUploading, model.SliceUploaded).Do(ctx).Err())
		slice1KV, err := sliceRepo.Get(sliceKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.SliceUploaded, slice1KV.State)
		slice2KV, err := sliceRepo.Get(sliceKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.SliceUploaded, slice2KV.State)
	}

	// INVALID: FileImporting (1) - a slice is not uploaded
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := fileRepo.StateTransition(file.FileKey, clk.Now(), model.FileClosing, model.FileImporting).Do(ctx).Err()
		require.Error(t, err)
		assert.Equal(t, strings.TrimSpace(`
unexpected slice "123/456/my-source/my-sink/2000-01-01T01:00:00.000Z/my-volume-5/2000-01-01T01:00:00.000Z" state:
- unexpected combination: file state "importing" and slice state "closing"
`), err.Error())
	}

	// VALID: SliceUploading
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, sliceRepo.StateTransition(sliceKey3, clk.Now(), model.SliceClosing, model.SliceUploading).Do(ctx).Err())
		slice1KV, err := sliceRepo.Get(sliceKey3).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.SliceUploading, slice1KV.State)
	}

	// INVALID: FileImporting (2) - a slice is not uploaded
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := fileRepo.StateTransition(file.FileKey, clk.Now(), model.FileClosing, model.FileImporting).Do(ctx).Err()
		require.Error(t, err)
		assert.Equal(t, strings.TrimSpace(`
unexpected slice "123/456/my-source/my-sink/2000-01-01T01:00:00.000Z/my-volume-5/2000-01-01T01:00:00.000Z" state:
- unexpected combination: file state "importing" and slice state "uploading"
`), err.Error())
	}

	// VALID: SliceUploaded
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, sliceRepo.StateTransition(sliceKey3, clk.Now(), model.SliceUploading, model.SliceUploaded).Do(ctx).Err())
		slice1KV, err := sliceRepo.Get(sliceKey3).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.SliceUploaded, slice1KV.State)
	}

	// INVALID: from argument doesn't match
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := fileRepo.StateTransition(file.FileKey, clk.Now(), model.FileWriting, model.FileImporting).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `file "123/456/my-source/my-sink/2000-01-01T01:00:00.000Z" is in "closing" state, expected "writing"`, err.Error())
		}
	}

	// VALID: FileImporting - all slices are in SliceUploaded state
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, fileRepo.StateTransition(file.FileKey, clk.Now(), model.FileClosing, model.FileImporting).Do(ctx).Err())
		fileKV, err := fileRepo.Get(file.FileKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.FileImporting, fileKV.State)
		slice3KV, err := sliceRepo.Get(sliceKey3).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.SliceUploaded, slice3KV.State)
	}

	// VALID: FileImported - slices in SliceUploaded state are switched to SliceImported state
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, fileRepo.StateTransition(file.FileKey, clk.Now(), model.FileImporting, model.FileImported).Do(ctx).Err())
		fileKV, err := fileRepo.Get(file.FileKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.FileImported, fileKV.State)
		slice3KV, err := sliceRepo.Get(sliceKey3).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.SliceImported, slice3KV.State)
	}
}
