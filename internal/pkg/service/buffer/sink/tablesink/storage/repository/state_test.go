package repository_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	deps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

func TestRepository_FileAndSliceStateTransitions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T19:00:00.000Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	volumeID1 := storage.VolumeID("my-volume-1")
	volumeID2 := storage.VolumeID("my-volume-2")
	volumeID3 := storage.VolumeID("my-volume-3")

	// Get services
	d, mocked := dependencies.NewMockedTableSinkScope(t, config.New(), deps.WithClock(clk))
	rb := rollback.New(d.Logger())
	defRepo := d.DefinitionRepository()
	r := d.StorageRepository()

	// Mock file API calls
	transport := mocked.MockedHTTPTransport()
	mockStorageAPICalls(t, clk, branchKey, transport)

	// Create parent branch, source, sink and token
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create("Create source", &source).Do(ctx).Err())
		sink := test.NewSink(sinkKey)
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink).Do(ctx).Err())
		require.NoError(t, r.Token().Put(sink.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
	}

	// Create file (the first rotate) - with 3 slices
	// -----------------------------------------------------------------------------------------------------------------
	var file storage.File
	var slice1, slice2, slice3 storage.Slice
	{
		var err error
		file, err = r.File().Rotate(rb, clk.Now(), sinkKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		clk.Add(time.Hour)
		fileVolumeKey1 := storage.FileVolumeKey{FileKey: file.FileKey, VolumeID: volumeID1}
		slice1, err = r.Slice().Rotate(clk.Now(), fileVolumeKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		clk.Add(time.Hour)
		fileVolumeKey2 := storage.FileVolumeKey{FileKey: file.FileKey, VolumeID: volumeID2}
		slice2, err = r.Slice().Rotate(clk.Now(), fileVolumeKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		clk.Add(time.Hour)
		fileVolumeKey3 := storage.FileVolumeKey{FileKey: file.FileKey, VolumeID: volumeID3}
		slice3, err = r.Slice().Rotate(clk.Now(), fileVolumeKey3).Do(ctx).ResultOrErr()
		require.NoError(t, err)
	}

	// INVALID: SliceUploading - invalid transition
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := r.Slice().StateTransition(clk.Now(), slice1.SliceKey, storage.SliceWriting, storage.SliceUploading).Do(ctx).Err()
		require.Error(t, err)
		assert.Equal(t, `unexpected slice "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume-1/2000-01-01T20:00:00.000Z" state transition from "writing" to "uploading"`, err.Error())
	}

	// INVALID: FileImporting - invalid transition
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := r.File().StateTransition(clk.Now(), file.FileKey, storage.FileWriting, storage.FileImporting).Do(ctx).Err()
		require.Error(t, err)
		assert.Equal(t, `unexpected file "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z" state transition from "writing" to "importing"`, err.Error())
	}

	// VALID: SliceClosing
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, r.Slice().Close(clk.Now(), slice1.FileVolumeKey).Do(ctx).Err())
		require.NoError(t, r.Slice().Close(clk.Now(), slice2.FileVolumeKey).Do(ctx).Err())
		slice1KV, err := r.Slice().Get(slice1.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceClosing, slice1KV.State)
		slice2KV, err := r.Slice().Get(slice2.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceClosing, slice2KV.State)
	}

	// VALID: FileClosing, slices in SliceWriting state are switched to SliceClosing state
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, r.File().CloseAllIn(clk.Now(), sinkKey).Do(ctx).Err())
		fileKV, err := r.File().Get(file.FileKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.FileClosing, fileKV.State)
		slice3KV, err := r.Slice().Get(slice3.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceClosing, slice3KV.State)
	}

	// INVALID: from argument doesn't match
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := r.Slice().StateTransition(clk.Now(), slice1.SliceKey, storage.SliceWriting, storage.SliceUploading).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `slice "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume-1/2000-01-01T20:00:00.000Z" is in "closing" state, expected "writing"`, err.Error())
		}
	}

	// VALID: SliceUploading
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, r.Slice().StateTransition(clk.Now(), slice1.SliceKey, storage.SliceClosing, storage.SliceUploading).Do(ctx).Err())
		require.NoError(t, r.Slice().StateTransition(clk.Now(), slice2.SliceKey, storage.SliceClosing, storage.SliceUploading).Do(ctx).Err())
		slice1KV, err := r.Slice().Get(slice1.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceUploading, slice1KV.State)
		slice2KV, err := r.Slice().Get(slice2.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceUploading, slice2KV.State)
	}

	// VALID: SliceUploaded
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, r.Slice().StateTransition(clk.Now(), slice1.SliceKey, storage.SliceUploading, storage.SliceUploaded).Do(ctx).Err())
		require.NoError(t, r.Slice().StateTransition(clk.Now(), slice2.SliceKey, storage.SliceUploading, storage.SliceUploaded).Do(ctx).Err())
		slice1KV, err := r.Slice().Get(slice1.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceUploaded, slice1KV.State)
		slice2KV, err := r.Slice().Get(slice2.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceUploaded, slice2KV.State)
	}

	// INVALID: FileImporting (1) - a slice is not uploaded
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := r.File().StateTransition(clk.Now(), file.FileKey, storage.FileClosing, storage.FileImporting).Do(ctx).Err()
		require.Error(t, err)
		assert.Equal(t, strings.TrimSpace(`
unexpected slice "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume-3/2000-01-01T22:00:00.000Z" state:
- unexpected combination: file state "importing" and slice state "closing"
`), err.Error())
	}

	// VALID: SliceUploading
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, r.Slice().StateTransition(clk.Now(), slice3.SliceKey, storage.SliceClosing, storage.SliceUploading).Do(ctx).Err())
		slice1KV, err := r.Slice().Get(slice3.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceUploading, slice1KV.State)
	}

	// INVALID: FileImporting (2) - a slice is not uploaded
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := r.File().StateTransition(clk.Now(), file.FileKey, storage.FileClosing, storage.FileImporting).Do(ctx).Err()
		require.Error(t, err)
		assert.Equal(t, strings.TrimSpace(`
unexpected slice "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume-3/2000-01-01T22:00:00.000Z" state:
- unexpected combination: file state "importing" and slice state "uploading"
`), err.Error())
	}

	// VALID: SliceUploaded
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, r.Slice().StateTransition(clk.Now(), slice3.SliceKey, storage.SliceUploading, storage.SliceUploaded).Do(ctx).Err())
		slice1KV, err := r.Slice().Get(slice3.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceUploaded, slice1KV.State)
	}

	// INVALID: from argument doesn't match
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := r.File().StateTransition(clk.Now(), file.FileKey, storage.FileWriting, storage.FileImporting).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `file "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z" is in "closing" state, expected "writing"`, err.Error())
		}
	}

	// VALID: FileImporting - all slices are in SliceUploaded state
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, r.File().StateTransition(clk.Now(), file.FileKey, storage.FileClosing, storage.FileImporting).Do(ctx).Err())
		fileKV, err := r.File().Get(file.FileKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.FileImporting, fileKV.State)
		slice3KV, err := r.Slice().Get(slice3.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceUploaded, slice3KV.State)
	}

	// VALID: FileImported - slices in SliceUploaded state are switched to SliceImported state
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, r.File().StateTransition(clk.Now(), file.FileKey, storage.FileImporting, storage.FileImported).Do(ctx).Err())
		fileKV, err := r.File().Get(file.FileKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.FileImported, fileKV.State)
		slice3KV, err := r.Slice().Get(slice3.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceImported, slice3KV.State)
	}
}
