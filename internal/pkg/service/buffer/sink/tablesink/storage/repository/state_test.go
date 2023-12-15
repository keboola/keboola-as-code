package repository

import (
	"context"
	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/s3"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	defRepository "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	deps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/relvacode/iso8601"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestRepository_FileAndSliceStateTransitions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	now := utctime.MustParse("2000-01-01T19:00:00.000Z").Time()
	clk := clock.NewMock()
	clk.Set(now)

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	volumeID := storage.VolumeID("my-volume")
	cfg := storage.NewConfig()
	credentials := &keboola.FileUploadCredentials{
		S3UploadParams: &s3.UploadParams{
			Credentials: s3.Credentials{
				Expiration: iso8601.Time{Time: now.Add(time.Hour)},
			},
		},
	}

	d := deps.NewMocked(t, deps.WithEnabledEtcdClient(), deps.WithClock(clk))
	//client := d.TestEtcdClient()
	defRepo := defRepository.New(d)
	backoff := storage.NoRandomizationBackoff()
	r := newWithBackoff(d, defRepo, cfg, backoff)

	// Create parent branch, source and sinks
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := branchTemplate(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch).Do(ctx).Err())
		source := sourceTemplate(sourceKey)
		require.NoError(t, defRepo.Source().Create("Create source", &source).Do(ctx).Err())
		sink := sinkTemplate(sinkKey)
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink).Do(ctx).Err())
	}

	// Create file with 3 slices
	// -----------------------------------------------------------------------------------------------------------------
	var file storage.File
	var slice1, slice2, slice3 storage.Slice
	{
		var err error
		file, err = r.File().Create(sinkKey, credentials).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		clk.Add(time.Hour)
		slice1, err = r.Slice().Create(file.FileKey, volumeID).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		clk.Add(time.Hour)
		slice2, err = r.Slice().Create(file.FileKey, volumeID).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		clk.Add(time.Hour)
		slice3, err = r.Slice().Create(file.FileKey, volumeID).Do(ctx).ResultOrErr()
		require.NoError(t, err)
	}

	// INVALID: SliceUploading - invalid transition
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := r.Slice().StateTransition(slice1.SliceKey, storage.SliceUploading).Do(ctx).Err()
		require.Error(t, err)
		assert.Equal(t, `unexpected slice "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T20:00:00.000Z" state transition from "writing" to "uploading"`, err.Error())
	}

	// INVALID: FileImporting - invalid transition
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := r.File().StateTransition(file.FileKey, storage.FileImporting).Do(ctx).Err()
		require.Error(t, err)
		assert.Equal(t, `unexpected file "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z" state transition from "writing" to "importing"`, err.Error())
	}

	// VALID: SliceClosing
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, r.Slice().StateTransition(slice1.SliceKey, storage.SliceClosing).Do(ctx).Err())
		require.NoError(t, r.Slice().StateTransition(slice2.SliceKey, storage.SliceClosing).Do(ctx).Err())
		slice1KV, err := r.Slice().Get(slice1.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceClosing, slice1KV.Value.State)
		slice2KV, err := r.Slice().Get(slice2.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceClosing, slice2KV.Value.State)
	}

	// VALID: FileClosing, slices in SliceWriting state are switched to SliceClosing state
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, r.File().StateTransition(file.FileKey, storage.FileClosing).Do(ctx).Err())
		fileKV, err := r.File().Get(file.FileKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.FileClosing, fileKV.Value.State)
		slice3KV, err := r.Slice().Get(slice3.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceClosing, slice3KV.Value.State)
	}

	// VALID: SliceUploading
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, r.Slice().StateTransition(slice1.SliceKey, storage.SliceUploading).Do(ctx).Err())
		require.NoError(t, r.Slice().StateTransition(slice2.SliceKey, storage.SliceUploading).Do(ctx).Err())
		slice1KV, err := r.Slice().Get(slice1.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceUploading, slice1KV.Value.State)
		slice2KV, err := r.Slice().Get(slice2.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceUploading, slice2KV.Value.State)
	}

	// VALID: SliceUploaded
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, r.Slice().StateTransition(slice1.SliceKey, storage.SliceUploaded).Do(ctx).Err())
		require.NoError(t, r.Slice().StateTransition(slice2.SliceKey, storage.SliceUploaded).Do(ctx).Err())
		slice1KV, err := r.Slice().Get(slice1.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceUploaded, slice1KV.Value.State)
		slice2KV, err := r.Slice().Get(slice2.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceUploaded, slice2KV.Value.State)
	}

	// INVALID: FileImporting (1) - a slice is not uploaded
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := r.File().StateTransition(file.FileKey, storage.FileImporting).Do(ctx).Err()
		require.Error(t, err)
		assert.Equal(t, strings.TrimSpace(`
unexpected slice "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T22:00:00.000Z" state:
- unexpected combination: file state "importing" and slice state "closing"
`), err.Error())
	}

	// VALID: SliceUploading
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, r.Slice().StateTransition(slice3.SliceKey, storage.SliceUploading).Do(ctx).Err())
		slice1KV, err := r.Slice().Get(slice3.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceUploading, slice1KV.Value.State)
	}

	// INVALID: FileImporting (2) - a slice is not uploaded
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := r.File().StateTransition(file.FileKey, storage.FileImporting).Do(ctx).Err()
		require.Error(t, err)
		assert.Equal(t, strings.TrimSpace(`
unexpected slice "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T22:00:00.000Z" state:
- unexpected combination: file state "importing" and slice state "uploading"
`), err.Error())
	}

	// VALID: SliceUploaded
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, r.Slice().StateTransition(slice3.SliceKey, storage.SliceUploaded).Do(ctx).Err())
		slice1KV, err := r.Slice().Get(slice3.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceUploaded, slice1KV.Value.State)
	}

	// VALID: FileImporting - all slices are in SliceUploaded state
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, r.File().StateTransition(file.FileKey, storage.FileImporting).Do(ctx).Err())
		fileKV, err := r.File().Get(file.FileKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.FileImporting, fileKV.Value.State)
		slice3KV, err := r.Slice().Get(slice3.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceUploaded, slice3KV.Value.State)
	}

	// VALID: FileImported - slices in SliceUploaded state are switched to SliceImported state
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, r.File().StateTransition(file.FileKey, storage.FileImported).Do(ctx).Err())
		fileKV, err := r.File().Get(file.FileKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.FileImported, fileKV.Value.State)
		slice3KV, err := r.Slice().Get(slice3.SliceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storage.SliceImported, slice3KV.Value.State)
	}
}
