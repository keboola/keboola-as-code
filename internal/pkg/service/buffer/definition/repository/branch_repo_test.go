package repository

import (
	"context"
	"net/http"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	deps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	serviceErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func branchTemplate(k key.BranchKey) definition.Branch {
	return definition.Branch{BranchKey: k}
}

func TestRepository_Branch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey1 := key.BranchKey{ProjectID: 123, BranchID: 567}
	branchKey2 := key.BranchKey{ProjectID: 456, BranchID: 789}

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2006-01-02T15:04:05.123Z").Time())

	d := deps.NewMocked(t, deps.WithEnabledEtcdClient(), deps.WithClock(clk))
	client := d.TestEtcdClient()
	r := New(d).Branch()

	// Empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		// List - empty
		branches, err := r.List(projectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, branches)
		branches, err = r.List(branchKey1.ProjectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, branches)
	}
	{
		// ListDeleted - empty
		branches, err := r.ListDeleted(projectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, branches)
		branches, err = r.ListDeleted(branchKey1.ProjectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, branches)
	}
	{
		// Get - not found
		if err := r.Get(branchKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `branch "123/567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// GetDeleted - not found
		if err := r.GetDeleted(branchKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted branch "123/567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch1 := branchTemplate(branchKey1)
		branch2 := branchTemplate(branchKey2)

		result1, err := r.Create(&branch1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, branch1, result1)
		result2, err := r.Create(&branch2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, branch2, result2)
	}
	{
		// List
		branches, err := r.List(branchKey1.ProjectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, branches, 1)
		branches, err = r.List(branchKey2.ProjectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, branches, 1)
	}
	{
		// Get
		result1, err := r.Get(branchKey1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, keboola.BranchID(567), result1.BranchID)
		result2, err := r.Get(branchKey2).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, keboola.BranchID(789), result2.BranchID)
	}
	{
		// GetDeleted - not found
		if err := r.GetDeleted(branchKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted branch "123/567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - already exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch1 := branchTemplate(branchKey1)
		if err := r.Create(&branch1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `branch "123/567" already exists in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}

	// Create some sources and sinks to test SoftDelete, in the next step
	// -----------------------------------------------------------------------------------------------------------------
	var source1, source2, source3 definition.Source
	var sink1, sink2, sink3 definition.Sink
	{
		// Create 3 sources
		source1 = sourceTemplate(key.SourceKey{BranchKey: branchKey1, SourceID: "source-1"})
		require.NoError(t, r.all.source.Create("Create source", &source1).Do(ctx).Err())
		source2 = sourceTemplate(key.SourceKey{BranchKey: branchKey1, SourceID: "source-2"})
		require.NoError(t, r.all.source.Create("Create source", &source2).Do(ctx).Err())
		source3 = sourceTemplate(key.SourceKey{BranchKey: branchKey1, SourceID: "source-3"})
		require.NoError(t, r.all.source.Create("Create source", &source3).Do(ctx).Err())
	}
	{
		// Create 3 sinks
		sink1 = sinkTemplate(key.SinkKey{SourceKey: source1.SourceKey, SinkID: "sink-1"})
		require.NoError(t, r.all.sink.Create("Create sink", &sink1).Do(ctx).Err())
		sink2 = sinkTemplate(key.SinkKey{SourceKey: source1.SourceKey, SinkID: "sink-2"})
		require.NoError(t, r.all.sink.Create("Create sink", &sink2).Do(ctx).Err())
		sink3 = sinkTemplate(key.SinkKey{SourceKey: source1.SourceKey, SinkID: "sink-3"})
		require.NoError(t, r.all.sink.Create("Create sink", &sink3).Do(ctx).Err())
	}
	{
		// Delete source3 manually, so it should not be undeleted with the branch1 later
		require.NoError(t, r.all.Source().SoftDelete(source3.SourceKey).Do(ctx).Err())

		// Delete sink3 manually, so it should not be undeleted with the branch1/source1 later
		require.NoError(t, r.all.Sink().SoftDelete(sink3.SinkKey).Do(ctx).Err())
	}

	// SoftDelete
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, r.SoftDelete(branchKey1).Do(ctx).Err())
	}
	{
		// Get - not found
		if err := r.Get(branchKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `branch "123/567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// GetDeleted - found
		result, err := r.GetDeleted(branchKey1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, keboola.BranchID(567), result.BranchID)
	}
	{
		// List - empty
		branches, err := r.List(branchKey1.ProjectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, branches)
	}
	{
		// ListDeleted
		branches, err := r.ListDeleted(branchKey1.ProjectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, branches, 1)
	}

	// SoftDelete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.SoftDelete(branchKey1).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `branch "123/567" not found in the project`, err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Undelete
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Undelete
		result, err := r.Undelete(branchKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, keboola.BranchID(567), result.BranchID)
	}
	{
		// ExistsOrErr
		assert.NoError(t, r.ExistsOrErr(branchKey1).Do(ctx).Err())
	}
	{
		// Get
		branch1, err := r.Get(branchKey1).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			assert.Equal(t, keboola.BranchID(567), branch1.BranchID)
		}
	}
	{
		// GetDeleted - not found
		if err := r.GetDeleted(branchKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted branch "123/567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// List
		branches, err := r.List(branchKey1.ProjectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, branches, 1)

	}
	{
		// ListDeleted - empty
		branches, err := r.ListDeleted(branchKey1.ProjectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, branches)
	}

	// Undelete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.Undelete(branchKey1).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `deleted branch "123/567" not found in the project`, err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Re-create causes Undelete
	// -----------------------------------------------------------------------------------------------------------------
	{
		// SoftDelete
		assert.NoError(t, r.SoftDelete(branchKey1).Do(ctx).Err())
	}
	{
		//  Re-create
		branch1 := branchTemplate(branchKey1)
		assert.NoError(t, r.Create(&branch1).Do(ctx).Err())
		assert.Equal(t, keboola.BranchID(567), branch1.BranchID)
		assert.False(t, branch1.Deleted)
		assert.Nil(t, branch1.DeletedAt)
	}
	{
		// Get
		branch1, err := r.Get(branchKey1).Do(ctx).ResultOrErr()
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

	// Test limit: branches per project
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Create branches up to maximum count
		// Note: multiple puts are merged to a transaction to improve test speed
		txn := op.NewTxnOp(client)
		ops := 0
		for i := 2; i <= MaxBranchesPerProject; i++ {
			v := branchTemplate(key.BranchKey{ProjectID: branchKey1.ProjectID, BranchID: keboola.BranchID(1000 + i)})
			txn.Then(r.schema.Active().ByKey(v.BranchKey).Put(client, v))

			// Send the txn it is full, or after the last item
			ops++
			if ops == 100 || i == MaxBranchesPerProject {
				// Send
				assert.NoError(t, txn.Do(ctx).Err())
				// Reset
				ops = 0
				txn = op.NewTxnOp(client)
			}
		}
		branches, err := r.List(branchKey1.ProjectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, branches, MaxBranchesPerProject)
	}
	{
		// Exceed the limit
		branch := branchTemplate(key.BranchKey{ProjectID: 123, BranchID: 111111})
		if err := r.Create(&branch).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, "branch count limit reached in the project, the maximum is 100", err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}
}
