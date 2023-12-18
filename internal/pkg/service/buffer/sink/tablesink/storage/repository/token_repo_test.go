package repository

import (
	"context"
	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	defRepository "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	deps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

func TestRepository_Token(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	now := utctime.MustParse("2000-01-03T01:00:00.000Z").Time()
	clk := clock.NewMock()
	clk.Set(now)

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey1 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-2"}
	nonExistentSinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "non-existent-sink"}
	storageToken1 := keboola.Token{Token: "1234"}
	storageToken2 := keboola.Token{Token: "5678"}

	d := deps.NewMocked(t, deps.WithEnabledEtcdClient(), deps.WithClock(clk))
	client := d.TestEtcdClient()
	defRepo := defRepository.New(d)
	cfg := storage.NewConfig()
	backoff := storage.NoRandomizationBackoff()
	r := newWithBackoff(d, defRepo, cfg, backoff).Token()

	// Empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Get - not found
		if err := r.Get(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `token "123/456/my-source/my-sink-1" not found in the sink`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - parent sink doesn't exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := r.Put(sinkKey1, storageToken1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "123/456/my-source/my-sink-1" not found in the source`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create parent branch, source, sinks
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := branchTemplate(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch).Do(ctx).Err())
		source := sourceTemplate(sourceKey)
		require.NoError(t, defRepo.Source().Create("Create source", &source).Do(ctx).Err())
		sink1 := sinkTemplate(sinkKey1)
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink1).Do(ctx).Err())
		sink2 := sinkTemplate(sinkKey2)
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink2).Do(ctx).Err())
	}

	// Create
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Create 2 tokens for different sinks
		result1, err := r.Put(sinkKey1, storageToken1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storageToken1, result1.Token)

		result2, err := r.Put(sinkKey2, storageToken2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storageToken2, result2.Token)
	}
	{
		// Get
		result1, err := r.Get(sinkKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storageToken1, result1.Token)
		result2, err := r.Get(sinkKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storageToken2, result2.Token)
	}

	// Update
	// -----------------------------------------------------------------------------------------------------------------
	storageToken1.Token = "abcd"
	storageToken2.Token = "efgh"
	{
		// Create 2 slices in different files
		result1, err := r.Put(sinkKey1, storageToken1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storageToken1, result1.Token)

		result2, err := r.Put(sinkKey2, storageToken2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storageToken2, result2.Token)
	}
	{
		// Get
		result1, err := r.Get(sinkKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storageToken1, result1.Token)
		result2, err := r.Get(sinkKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, storageToken2, result2.Token)
	}

	// Delete
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, r.Delete(sinkKey2).Do(ctx).Err())
	}
	{
		// Get - not found
		if err := r.Get(sinkKey2).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `token "123/456/my-source/my-sink-2" not found in the sink`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Delete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.Delete(nonExistentSinkKey).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `token "123/456/my-source/non-existent-sink" not found in the sink`, err.Error())
		serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Check etcd state - slice2 has been deleted, but slice 1 exists
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, expectedTokenEtcdState(), etcdhelper.WithIgnoredKeyPattern("^definition/"))

	// DeleteAll
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, r.DeleteAll(projectID).Do(ctx).Err())
	}
	{
		// Get - not found
		if err := r.Get(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `token "123/456/my-source/my-sink-1" not found in the sink`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// No token in etcd
		etcdhelper.AssertKVsString(t, client, "", etcdhelper.WithIgnoredKeyPattern("^definition/"))
	}

}

func expectedTokenEtcdState() string {
	return `
<<<<<
storage/secret/token/123/456/my-source/my-sink-1
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "token": {
    "token": "abcd",
    %A
  }
}
>>>>>
`
}
