package token_test

//
// import (
//	"context"
//	"net/http"
//	"testing"
//
//	"github.com/benbjohnson/clock"
//	"github.com/keboola/go-client/pkg/keboola"
//	"github.com/stretchr/testify/assert"
//	"github.com/stretchr/testify/require"
//
//	deps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
//	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
//	tokenRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/keboola/repository/token"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
//	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
//)
//
// func TestRepository_Token(t *testing.T) {
//	t.Parallel()
//	ctx := context.Background()
//
//	clk := clock.NewMock()
//	clk.Set(utctime.MustParse("2000-01-03T01:00:00.000Z").Time())
//
//	// Fixtures
//	projectID := keboola.ProjectID(123)
//	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
//	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
//	sinkKey1 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
//	sinkKey2 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-2"}
//	nonExistentSinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "non-existent-sink"}
//
//	// Get services
//	d, mocked := dependencies.NewMockedLocalStorageScope(t, deps.WithClock(clk))
//	client := mocked.TestEtcdClient()
//	defRepo := d.DefinitionRepository()
//	var r *tokenRepo.Repository
//
//	// Empty
//	// -----------------------------------------------------------------------------------------------------------------
//	{
//		// Get - not found
//		if err := r.Get(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
//			assert.Equal(t, `sink token "123/456/my-source/my-sink-1" not found in the database`, err.Error())
//			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
//		}
//	}
//
//	// Create parent branch, source, sinks
//	// -----------------------------------------------------------------------------------------------------------------
//	{
//		branch := test.NewBranch(branchKey)
//		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now()).Do(ctx).Err())
//		source := test.NewSource(sourceKey)
//		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), "Create source").Do(ctx).Err())
//		sink1 := test.NewSink(sinkKey1)
//		require.NoError(t, defRepo.Sink().Create(&sink1, clk.Now(), "Create sink").Do(ctx).Err())
//		sink2 := test.NewSink(sinkKey2)
//		require.NoError(t, defRepo.Sink().Create(&sink2, clk.Now(), "Create sink").Do(ctx).Err())
//	}
//
//	// Token are created together with sinks
//	// -----------------------------------------------------------------------------------------------------------------
//	{
//		// Get
//		result1, err := r.Get(sinkKey1).Do(ctx).ResultOrErr()
//		require.NoError(t, err)
//		assert.NotEqual(t, "", result1.Token.ID)
//		assert.NotEqual(t, "0", result1.Token.ID)
//		result1.Token.ID = ""
//		assert.Equal(t, model.Token{
//			SinkKey: sinkKey1,
//			Token: keboola.Token{
//				Token:                 "my-token",
//				Description:           "[_internal] Stream Sink my-source/my-sink-1",
//				CanReadAllFileUploads: true,
//				BucketPermissions: keboola.BucketPermissions{
//					keboola.MustParseBucketID("in.bucket"): keboola.BucketPermissionWrite,
//				},
//			},
//		}, result1)
//		result2, err := r.Get(sinkKey2).Do(ctx).ResultOrErr()
//		require.NoError(t, err)
//		assert.NotEqual(t, "", result2.Token.ID)
//		assert.NotEqual(t, "0", result2.Token.ID)
//		result2.Token.ID = ""
//		assert.Equal(t, model.Token{
//			SinkKey: sinkKey2,
//			Token: keboola.Token{
//				Token:                 "my-token",
//				Description:           "[_internal] Stream Sink my-source/my-sink-2",
//				CanReadAllFileUploads: true,
//				BucketPermissions: keboola.BucketPermissions{
//					keboola.MustParseBucketID("in.bucket"): keboola.BucketPermissionWrite,
//				},
//			},
//		}, result2)
//	}
//
//	// Update
//	// -----------------------------------------------------------------------------------------------------------------
//	{
//		// Create 2 slices in different files
//		result1, err := r.Put(sinkKey1, keboola.Token{Token: "abcd"}).Do(ctx).ResultOrErr()
//		require.NoError(t, err)
//		assert.Equal(t, "abcd", result1.Token.Token)
//
//		result2, err := r.Put(sinkKey2, keboola.Token{Token: "efgh"}).Do(ctx).ResultOrErr()
//		require.NoError(t, err)
//		assert.Equal(t, "efgh", result2.Token.Token)
//	}
//	{
//		// Get
//		result1, err := r.Get(sinkKey1).Do(ctx).ResultOrErr()
//		require.NoError(t, err)
//		assert.Equal(t, "abcd", result1.Token.Token)
//		result2, err := r.Get(sinkKey2).Do(ctx).ResultOrErr()
//		require.NoError(t, err)
//		assert.Equal(t, "efgh", result2.Token.Token)
//	}
//
//	// Delete
//	// -----------------------------------------------------------------------------------------------------------------
//	{
//		assert.NoError(t, r.Delete(sinkKey2).Do(ctx).Err())
//	}
//	{
//		// Get - not found
//		if err := r.Get(sinkKey2).Do(ctx).Err(); assert.Error(t, err) {
//			assert.Equal(t, `sink token "123/456/my-source/my-sink-2" not found in the database`, err.Error())
//			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
//		}
//	}
//
//	// Delete - not found
//	// -----------------------------------------------------------------------------------------------------------------
//	if err := r.Delete(nonExistentSinkKey).Do(ctx).Err(); assert.Error(t, err) {
//		assert.Equal(t, `sink token "123/456/my-source/non-existent-sink" not found in the database`, err.Error())
//		serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
//	}
//
//	// Check etcd state - slice2 has been deleted, but slice 1 exists
//	// -----------------------------------------------------------------------------------------------------------------
//	etcdhelper.AssertKVsString(t, client, expectedTokenEtcdState(), etcdhelper.WithIgnoredKeyPattern("^definition/"))
//
//	// DeleteAll
//	// -----------------------------------------------------------------------------------------------------------------
//	{
//		assert.NoError(t, r.DeleteAll(projectID).Do(ctx).Err())
//	}
//	{
//		// Get - not found
//		if err := r.Get(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
//			assert.Equal(t, `sink token "123/456/my-source/my-sink-1" not found in the database`, err.Error())
//			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
//		}
//	}
//	{
//		// No token in etcd
//		etcdhelper.AssertKVsString(t, client, "", etcdhelper.WithIgnoredKeyPattern("^definition/"))
//	}
//}
//
//func expectedTokenEtcdState() string {
//	return `
//<<<<<
//storage/secret/token/123/456/my-source/my-sink-1
//-----
//{
//  "projectId": 123,
//  "branchId": 456,
//  "sourceId": "my-source",
//  "sinkId": "my-sink-1",
//  "token": {
//    "token": "abcd",
//    %A
//  }
//}
//>>>>>
//`
//}
