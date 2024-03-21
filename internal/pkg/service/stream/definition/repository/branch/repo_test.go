package branch_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	deps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	serviceErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestRepository_Branch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2006-01-02T15:04:05.123Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey1 := key.BranchKey{ProjectID: projectID, BranchID: 567}
	branchKey2 := key.BranchKey{ProjectID: 456, BranchID: 789}

	// Get services
	d, mocked := dependencies.NewMockedServiceScope(t, deps.WithClock(clk))
	client := mocked.TestEtcdClient()
	branchRepo := d.DefinitionRepository().Branch()
	sourceRepo := d.DefinitionRepository().Source()
	sinkRepo := d.DefinitionRepository().Sink()

	// Empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		// List - empty
		branches, err := branchRepo.List(projectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, branches)
		branches, err = branchRepo.List(branchKey1.ProjectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, branches)
	}
	{
		// ListDeleted - empty
		branches, err := branchRepo.ListDeleted(projectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, branches)
		branches, err = branchRepo.ListDeleted(branchKey1.ProjectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, branches)
	}
	{
		// Get - not found
		if err := branchRepo.Get(branchKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `branch "567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// GetDefault - not found
		if err := branchRepo.GetDefault(projectID).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `branch "default" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// GetDeleted - not found
		if err := branchRepo.GetDeleted(branchKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted branch "567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create
	// -----------------------------------------------------------------------------------------------------------------
	var branch1, branch2 definition.Branch
	{
		branch1 = test.NewBranch(branchKey1)
		branch1.IsDefault = true
		result1, err := branchRepo.Create(&branch1, clk.Now()).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, branch1, result1)

		branch2 = test.NewBranch(branchKey2)
		result2, err := branchRepo.Create(&branch2, clk.Now()).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, branch2, result2)
	}
	{
		// List
		branches, err := branchRepo.List(branchKey1.ProjectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, branches, 1)
		branches, err = branchRepo.List(branchKey2.ProjectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, branches, 1)
	}
	{
		// Get
		result1, err := branchRepo.Get(branchKey1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, keboola.BranchID(567), result1.BranchID)
		result2, err := branchRepo.Get(branchKey2).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, keboola.BranchID(789), result2.BranchID)
	}
	{
		// GetDefault
		result, err := branchRepo.GetDefault(projectID).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, branch1, result)
	}
	{
		// GetDeleted - not found
		if err := branchRepo.GetDeleted(branchKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted branch "567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - already exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch1 := test.NewBranch(branchKey1)
		if err := branchRepo.Create(&branch1, clk.Now()).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `branch "567" already exists in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}

	// Create some sources and sinks to test SoftDelete, in the next step
	// -----------------------------------------------------------------------------------------------------------------
	var source1, source2, source3 definition.Source
	var sink1, sink2, sink3 definition.Sink
	{
		// Create 3 sources
		source1 = test.NewSource(key.SourceKey{BranchKey: branchKey1, SourceID: "source-1"})
		require.NoError(t, sourceRepo.Create(&source1, clk.Now(), "Create source").Do(ctx).Err())
		source2 = test.NewSource(key.SourceKey{BranchKey: branchKey1, SourceID: "source-2"})
		require.NoError(t, sourceRepo.Create(&source2, clk.Now(), "Create source").Do(ctx).Err())
		source3 = test.NewSource(key.SourceKey{BranchKey: branchKey1, SourceID: "source-3"})
		require.NoError(t, sourceRepo.Create(&source3, clk.Now(), "Create source").Do(ctx).Err())
	}
	{
		// Create 3 sinks
		sink1 = test.NewSink(key.SinkKey{SourceKey: source1.SourceKey, SinkID: "sink-1"})
		require.NoError(t, sinkRepo.Create(&sink1, clk.Now(), "Create sink").Do(ctx).Err())
		sink2 = test.NewSink(key.SinkKey{SourceKey: source1.SourceKey, SinkID: "sink-2"})
		require.NoError(t, sinkRepo.Create(&sink2, clk.Now(), "Create sink").Do(ctx).Err())
		sink3 = test.NewSink(key.SinkKey{SourceKey: source1.SourceKey, SinkID: "sink-3"})
		require.NoError(t, sinkRepo.Create(&sink3, clk.Now(), "Create sink").Do(ctx).Err())
	}
	{
		// Delete source3 manually, so it should not be undeleted with the branch1 later
		require.NoError(t, sourceRepo.SoftDelete(source3.SourceKey, clk.Now()).Do(ctx).Err())

		// Delete sink3 manually, so it should not be undeleted with the branch1/source1 later
		require.NoError(t, sinkRepo.SoftDelete(sink3.SinkKey, clk.Now()).Do(ctx).Err())
	}

	// SoftDelete
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, branchRepo.SoftDelete(branchKey1, clk.Now()).Do(ctx).Err())
	}
	{
		// Get - not found
		if err := branchRepo.Get(branchKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `branch "567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// GetDeleted - found
		result, err := branchRepo.GetDeleted(branchKey1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, keboola.BranchID(567), result.BranchID)
	}
	{
		// List - empty
		branches, err := branchRepo.List(branchKey1.ProjectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, branches)
	}
	{
		// ListDeleted
		branches, err := branchRepo.ListDeleted(branchKey1.ProjectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, branches, 1)
	}

	// SoftDelete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := branchRepo.SoftDelete(branchKey1, clk.Now()).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `branch "567" not found in the project`, err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Undelete
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Undelete
		result, err := branchRepo.Undelete(branchKey1, clk.Now()).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, keboola.BranchID(567), result.BranchID)
	}
	{
		// ExistsOrErr
		assert.NoError(t, branchRepo.ExistsOrErr(branchKey1).Do(ctx).Err())
	}
	{
		// Get
		branch1, err := branchRepo.Get(branchKey1).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			assert.Equal(t, keboola.BranchID(567), branch1.BranchID)
		}
	}
	{
		// GetDeleted - not found
		if err := branchRepo.GetDeleted(branchKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted branch "567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// List
		branches, err := branchRepo.List(branchKey1.ProjectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, branches, 1)
	}
	{
		// ListDeleted - empty
		branches, err := branchRepo.ListDeleted(branchKey1.ProjectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, branches)
	}

	// Undelete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := branchRepo.Undelete(branchKey1, clk.Now()).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `deleted branch "567" not found in the project`, err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Re-create causes Undelete
	// -----------------------------------------------------------------------------------------------------------------
	{
		// SoftDelete
		assert.NoError(t, branchRepo.SoftDelete(branchKey1, clk.Now()).Do(ctx).Err())
	}
	{
		//  Re-create
		branch1 := test.NewBranch(branchKey1)
		assert.NoError(t, branchRepo.Create(&branch1, clk.Now()).Do(ctx).Err())
		assert.Equal(t, keboola.BranchID(567), branch1.BranchID)
		assert.False(t, branch1.Deleted)
		assert.Nil(t, branch1.DeletedAt)
	}
	{
		// Get
		branch1, err := branchRepo.Get(branchKey1).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			assert.Equal(t, keboola.BranchID(567), branch1.BranchID)
		}
	}

	// Check etcd final state
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, `
<<<<<
definition/branch/active/123/567
-----
{
  "projectId": 123,
  "branchId": 567,
  "undeletedAt": "2006-01-02T15:04:05.123Z",
  "isDefault": false
}
>>>>>

<<<<<
definition/branch/active/456/789
-----
%A
>>>>>

<<<<<
definition/source/active/123/567/source-1
-----
%A
>>>>>

<<<<<
definition/source/active/123/567/source-2
-----
%A
>>>>>

<<<<<
definition/source/deleted/123/567/source-3
-----
{
%A
  "deleted": true,
  "deletedAt": "2006-01-02T15:04:05.123Z",
  "type": "http",
%A
}
>>>>>

<<<<<
definition/source/version/123/567/source-1/0000000001
-----
%A
>>>>>

<<<<<
definition/source/version/123/567/source-2/0000000001
-----
%A
>>>>>

<<<<<
definition/source/version/123/567/source-3/0000000001
-----
%A
>>>>>

<<<<<
definition/sink/active/123/567/source-1/sink-1
-----
%A
>>>>>

<<<<<
definition/sink/active/123/567/source-1/sink-2
-----
%A
>>>>>

<<<<<
definition/sink/deleted/123/567/source-1/sink-3
-----
{
%A
  "deleted": true,
  "deletedAt": "2006-01-02T15:04:05.123Z",
  "type": "table",
%A
}
>>>>>

<<<<<
definition/sink/version/123/567/source-1/sink-1/0000000001
-----
%A
>>>>>

<<<<<
definition/sink/version/123/567/source-1/sink-2/0000000001
-----
%A
>>>>>

<<<<<
definition/sink/version/123/567/source-1/sink-3/0000000001
-----
%A
>>>>>
	`)
}
