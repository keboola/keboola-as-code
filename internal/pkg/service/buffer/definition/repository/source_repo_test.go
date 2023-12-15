package repository

import (
	"context"
	"fmt"
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

func sourceTemplate(k key.SourceKey) definition.Source {
	return definition.Source{
		SourceKey:   k,
		Type:        definition.SourceTypeHTTP,
		Name:        "My Source",
		Description: "My Description",
		HTTP:        &definition.HTTPSource{Secret: "012345678901234567890123456789012345678912345678"},
	}
}

func TestRepository_Source(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Fixtures
	projectID := keboola.ProjectID(123)
	nonExistentSourceKey := key.SourceKey{
		BranchKey: key.BranchKey{ProjectID: projectID, BranchID: 456},
		SourceID:  "non-existent",
	}
	source1 := sourceTemplate(key.SourceKey{BranchKey: key.BranchKey{ProjectID: projectID, BranchID: 456}, SourceID: "my-source-1"})
	source1.Name = "My Source 1"
	source2 := sourceTemplate(key.SourceKey{BranchKey: key.BranchKey{ProjectID: 123, BranchID: 789}, SourceID: "my-source-2"})
	source2.Name = "My Source 2"

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2006-01-02T15:04:05.123Z").Time())

	d := deps.NewMocked(t, deps.WithEnabledEtcdClient(), deps.WithClock(clk))
	client := d.TestEtcdClient()
	r := New(d).Source()

	// Empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		// List - empty
		sources, err := r.List(projectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, sources)
		sources, err = r.List(source1.BranchKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, sources)
	}
	{
		// ListDeleted - empty
		sources, err := r.ListDeleted(projectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, sources)
		sources, err = r.ListDeleted(source1.BranchKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, sources)
	}
	{
		// Get - not found
		if err := r.Get(source1.SourceKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source "123/456/my-source-1" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// GetDeleted - not found
		if err := r.GetDeleted(source1.SourceKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted source "123/456/my-source-1" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - parent Branch doesn't exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := r.Create("Create description", &source1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `branch "123/456" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create parent branches
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch1 := branchTemplate(source1.BranchKey)
		branch1.IsDefault = true
		require.NoError(t, r.all.Branch().Create(&branch1).Do(ctx).Err())
		branch2 := branchTemplate(source2.BranchKey)
		require.NoError(t, r.all.Branch().Create(&branch2).Do(ctx).Err())
	}

	// Create
	// -----------------------------------------------------------------------------------------------------------------
	{
		result1, err := r.Create("Create description", &source1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, result1, source1)
		assert.Equal(t, definition.VersionNumber(1), source1.VersionNumber())
		assert.NotEmpty(t, source1.VersionHash())
		result2, err := r.Create("Create description", &source2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, result2, source2)
		assert.Equal(t, definition.VersionNumber(1), source2.VersionNumber())
		assert.NotEmpty(t, source2.VersionHash())
	}
	{
		// List
		sources, err := r.List(projectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sources, 2)
		sources, err = r.List(source1.BranchKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sources, 1)
		sources, err = r.List(source2.BranchKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sources, 1)
	}
	{
		// Get
		result1, err := r.Get(source1.SourceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "My Source 1", result1.Value.Name)
		assert.Equal(t, definition.VersionNumber(1), result1.Value.VersionNumber())
		result2, err := r.Get(source2.SourceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "My Source 2", result2.Value.Name)
		assert.Equal(t, definition.VersionNumber(1), result2.Value.VersionNumber())
	}
	{
		// GetDeleted - not found
		if err := r.GetDeleted(source1.SourceKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted source "123/456/my-source-1" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// Versions
		versions, err := r.Versions(source1.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, versions, 1)
	}

	// Create - already exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := r.Create("Create description", &source1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source "123/456/my-source-1" already exists in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}

	// Update
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Modify name
		result, err := r.Update(source1.SourceKey, "Update description", func(v definition.Source) definition.Source {
			v.Name = "Modified Name"
			return v
		}).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "Modified Name", result.Name)
		assert.Equal(t, definition.VersionNumber(2), result.VersionNumber())
		kv, err := r.Get(source1.SourceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, result, kv.Value)
	}
	{
		// Modify description
		result, err := r.Update(source1.SourceKey, "Update description", func(v definition.Source) definition.Source {
			v.Description = "Modified Description"
			return v
		}).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "Modified Description", result.Description)
		assert.Equal(t, definition.VersionNumber(3), result.VersionNumber())
		kv, err := r.Get(source1.SourceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, result, kv.Value)
		source1 = kv.Value
	}

	// Update - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := r.Update(nonExistentSourceKey, "Update description", func(v definition.Source) definition.Source {
			v.Name = "Modified Name"
			return v
		}).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `source "123/456/non-existent" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Version - found
	// -----------------------------------------------------------------------------------------------------------------
	{
		source, err := r.Version(source1.SourceKey, 1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Source 1", source.Value.Name)
		source, err = r.Version(source1.SourceKey, 2).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "Modified Name", source.Value.Name)
	}

	// Version - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := r.Version(source1.SourceKey, 10).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source version "123/456/my-source-1/0000000010" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create some Sinks to test SoftDelete, in the next step
	// -----------------------------------------------------------------------------------------------------------------
	var sink1, sink2, sink3 definition.Sink
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
		// Delete sink3 manually, so it should not be undeleted with the source1 later
		require.NoError(t, r.all.Sink().SoftDelete(sink3.SinkKey).Do(ctx).Err())
	}

	// SoftDelete
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, r.SoftDelete(source1.SourceKey).Do(ctx).Err())
	}
	{
		// Get - not found
		if err := r.Get(source1.SourceKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source "123/456/my-source-1" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// GetDeleted - found
		result, err := r.GetDeleted(source1.SourceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "Modified Name", result.Value.Name)
		assert.Equal(t, definition.VersionNumber(3), result.Value.VersionNumber())
	}
	{
		// Version - found
		result, err := r.Version(source1.SourceKey, 1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Source 1", result.Value.Name)
		assert.Equal(t, definition.VersionNumber(1), result.Value.VersionNumber())
	}
	{
		// List - empty
		sources, err := r.List(source1.BranchKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, sources)
	}
	{
		// ListDeleted
		sources, err := r.ListDeleted(source1.BranchKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sources, 1)
	}

	// SoftDelete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.SoftDelete(source1.SourceKey).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `source "123/456/my-source-1" not found in the branch`, err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Undelete
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Undelete
		result, err := r.Undelete(source1.SourceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "Modified Name", result.Name)
		assert.Equal(t, definition.VersionNumber(3), result.VersionNumber())
	}
	{
		// ExistsOrErr
		assert.NoError(t, r.ExistsOrErr(source1.SourceKey).Do(ctx).Err())
	}
	{
		// Get
		kv, err := r.Get(source1.SourceKey).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			source1 = kv.Value
			assert.Equal(t, "Modified Name", source1.Name)
			assert.Equal(t, definition.VersionNumber(3), source1.VersionNumber())
		}
	}
	{
		// GetDeleted - not found
		if err := r.GetDeleted(source1.SourceKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted source "123/456/my-source-1" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// List
		sources, err := r.List(source1.BranchKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sources, 1)

	}
	{
		// ListDeleted - empty
		sources, err := r.ListDeleted(source1.BranchKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, sources)
	}

	// Undelete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.Undelete(source1.SourceKey).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `deleted source "123/456/my-source-1" not found in the branch`, err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Re-create causes Undelete + new version record
	// -----------------------------------------------------------------------------------------------------------------
	{
		// SoftDelete
		assert.Equal(t, definition.VersionNumber(3), source1.VersionNumber())
		assert.NoError(t, r.SoftDelete(source1.SourceKey).Do(ctx).Err())
	}
	{
		//  Re-create
		source1 = sourceTemplate(source1.SourceKey)
		assert.NoError(t, r.Create("Re-create", &source1).Do(ctx).Err())
		assert.Equal(t, definition.VersionNumber(4), source1.VersionNumber())
		assert.Equal(t, "My Source", source1.Name)
		assert.Equal(t, "My Description", source1.Description)
	}
	{
		// Get
		kv, err := r.Get(source1.SourceKey).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			source1 = kv.Value
			assert.Equal(t, definition.VersionNumber(4), source1.VersionNumber())
			assert.Equal(t, "My Source", source1.Name)
			assert.Equal(t, "My Description", source1.Description)
		}
	}
	{
		// Versions
		versions, err := r.Versions(source1.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, versions, 4)
	}

	// Rollback version
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Rollback
		assert.NoError(t, r.Rollback(source1.SourceKey, 2).Do(ctx).Err())
	}
	{
		// State after rollback
		result1, err := r.Get(source1.SourceKey).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Description", result1.Value.Description)
		assert.Equal(t, definition.VersionNumber(5), result1.Value.VersionNumber())
		assert.Equal(t, "Rollback to version 2", result1.Value.VersionDescription())
	}

	// Rollback version - object not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.Rollback(nonExistentSourceKey, 1).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `source "123/456/non-existent" not found in the branch`, err.Error())
	}

	// Rollback version - version not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.Rollback(source1.SourceKey, 10).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `source version "123/456/my-source-1/0000000010" not found in the branch`, err.Error())
	}

	// Check etcd final state
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, `
<<<<<
definition/branch/active/123/456
-----
%A
>>>>>

<<<<<
definition/branch/active/123/789
-----
%A
>>>>>

<<<<<
definition/source/active/123/456/my-source-1
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source-1",
  "version": {
    "number": 5,
    "hash": "217862c1da330b9b",
    "modifiedAt": "2006-01-02T15:04:05.123Z",
    "description": "Rollback to version 2"
  },
  "type": "http",
  "name": "Modified Name",
  "description": "My Description",
  "http": {
    "secret": "012345678901234567890123456789012345678912345678"
  }
}
>>>>>

<<<<<
definition/source/active/123/789/my-source-2
-----
%A
>>>>>

<<<<<
definition/source/version/123/456/my-source-1/0000000001
-----
%A
>>>>>

<<<<<
definition/source/version/123/456/my-source-1/0000000002
-----
%A
>>>>>

<<<<<
definition/source/version/123/456/my-source-1/0000000003
-----
%A
>>>>>

<<<<<
definition/source/version/123/456/my-source-1/0000000004
-----
%A
>>>>>

<<<<<
definition/source/version/123/456/my-source-1/0000000005
-----
%A
>>>>>

<<<<<
definition/source/version/123/789/my-source-2/0000000001
-----
%A
>>>>>

<<<<<
definition/sink/active/123/456/my-source-1/sink-1
-----
%A
>>>>>

<<<<<
definition/sink/active/123/456/my-source-1/sink-2
-----
%A
>>>>>

<<<<<
definition/sink/deleted/123/456/my-source-1/sink-3
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
definition/sink/version/123/456/my-source-1/sink-1/0000000001
-----
%A
>>>>>

<<<<<
definition/sink/version/123/456/my-source-1/sink-2/0000000001
-----
%A
>>>>>

<<<<<
definition/sink/version/123/456/my-source-1/sink-3/0000000001
-----
%A
>>>>>
	`)

	// Test limit: sources per branch
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Create sources up to maximum count
		// Note: multiple puts are merged to a transaction to improve test speed
		txn := op.NewTxnOp(client)
		ops := 0
		for i := 2; i <= MaxSourcesPerBranch; i++ {
			v := sourceTemplate(key.SourceKey{BranchKey: source1.BranchKey, SourceID: key.SourceID(fmt.Sprintf("my-source-%d", i))})
			v.IncrementVersion(v, clk.Now(), "Create")
			txn.Then(r.schema.Active().ByKey(v.SourceKey).Put(client, v))

			// Send the txn it is full, or after the last item
			ops++
			if ops == 100 || i == MaxSourcesPerBranch {
				// Send
				assert.NoError(t, txn.Do(ctx).Err())
				// Reset
				ops = 0
				txn = op.NewTxnOp(client)
			}
		}
		sources, err := r.List(source1.BranchKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sources, MaxSourcesPerBranch)
	}
	{
		// Exceed the limit
		source := sourceTemplate(key.SourceKey{BranchKey: key.BranchKey{ProjectID: projectID, BranchID: 456}, SourceID: "over-maximum-count"})
		if err := r.Create("Create description", &source).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, "source count limit reached in the branch, the maximum is 100", err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}

	// Test limit: versions per source limit
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Create versions up to maximum count
		// Note: multiple puts are merged to a transaction to improve test speed
		txn := op.NewTxnOp(client)
		ops := 0
		for i := source1.VersionNumber() + 1; i <= MaxSourceVersionsPerSource; i++ {
			source1.Description = fmt.Sprintf("Description %04d", i)
			source1.IncrementVersion(source1, clk.Now(), "Some Update")
			txn.Then(r.schema.Versions().Of(source1.SourceKey).Version(source1.VersionNumber()).Put(client, source1))

			// Send the txn it is full, or after the last item
			ops++
			if ops == 100 || i == MaxSourceVersionsPerSource {
				// Send
				assert.NoError(t, txn.Do(ctx).Err())
				// Reset
				ops = 0
				txn = op.NewTxnOp(client)
			}
		}
		// Check that the maximum count is reached
		sources, err := r.Versions(source1.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sources, MaxSourceVersionsPerSource)
	}
	{
		// Exceed the limit
		err := r.Update(source1.SourceKey, "Some update", func(v definition.Source) definition.Source {
			v.Description = "foo"
			return v
		}).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, "version count limit reached in the source, the maximum is 1000", err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}
}
