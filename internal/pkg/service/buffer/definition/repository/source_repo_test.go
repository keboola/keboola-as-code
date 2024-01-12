package repository_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	deps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	serviceErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestRepository_Source(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2006-01-02T15:04:05.123Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	nonExistentSourceKey := key.SourceKey{
		BranchKey: key.BranchKey{ProjectID: projectID, BranchID: 456},
		SourceID:  "non-existent",
	}
	sourceKey1 := key.SourceKey{BranchKey: key.BranchKey{ProjectID: projectID, BranchID: 456}, SourceID: "my-source-1"}
	sourceKey2 := key.SourceKey{BranchKey: key.BranchKey{ProjectID: 123, BranchID: 789}, SourceID: "my-source-2"}

	// Get services
	d, mocked := dependencies.NewMockedDefinitionScope(t, config.New(), deps.WithClock(clk))
	client := mocked.TestEtcdClient()
	branchRepo := d.DefinitionRepository().Branch()
	sourceRepo := d.DefinitionRepository().Source()
	sinkRepo := d.DefinitionRepository().Sink()

	// Empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		// List - empty
		sources, err := sourceRepo.List(projectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, sources)
		sources, err = sourceRepo.List(sourceKey1.BranchKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, sources)
	}
	{
		// ListDeleted - empty
		sources, err := sourceRepo.ListDeleted(projectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, sources)
		sources, err = sourceRepo.ListDeleted(sourceKey1.BranchKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, sources)
	}
	{
		// Get - not found
		if err := sourceRepo.Get(sourceKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source "123/456/my-source-1" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// GetDeleted - not found
		if err := sourceRepo.GetDeleted(sourceKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted source "123/456/my-source-1" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - parent Branch doesn't exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		source1 := test.NewSource(sourceKey1)
		if err := sourceRepo.Create("Create description", &source1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `branch "123/456" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create parent branches
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch1 := test.NewBranch(sourceKey1.BranchKey)
		branch1.IsDefault = true
		require.NoError(t, branchRepo.Create(&branch1).Do(ctx).Err())
		branch2 := test.NewBranch(sourceKey2.BranchKey)
		require.NoError(t, branchRepo.Create(&branch2).Do(ctx).Err())
	}

	// Create
	// -----------------------------------------------------------------------------------------------------------------
	{
		source1 := test.NewSource(sourceKey1)
		source1.Name = "My Source 1"
		result1, err := sourceRepo.Create("Create description", &source1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, result1, source1)
		assert.Equal(t, definition.VersionNumber(1), source1.VersionNumber())
		assert.NotEmpty(t, source1.VersionHash())

		source2 := test.NewSource(sourceKey2)
		source2.Name = "My Source 2"
		result2, err := sourceRepo.Create("Create description", &source2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, result2, source2)
		assert.Equal(t, definition.VersionNumber(1), source2.VersionNumber())
		assert.NotEmpty(t, source2.VersionHash())
	}
	{
		// List
		sources, err := sourceRepo.List(projectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, sources, 2)
		sources, err = sourceRepo.List(sourceKey1.BranchKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, sources, 1)
		sources, err = sourceRepo.List(sourceKey2.BranchKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, sources, 1)
	}
	{
		// Get
		result1, err := sourceRepo.Get(sourceKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "My Source 1", result1.Name)
		assert.Equal(t, definition.VersionNumber(1), result1.VersionNumber())
		result2, err := sourceRepo.Get(sourceKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "My Source 2", result2.Name)
		assert.Equal(t, definition.VersionNumber(1), result2.VersionNumber())
	}
	{
		// GetDeleted - not found
		if err := sourceRepo.GetDeleted(sourceKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted source "123/456/my-source-1" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// Versions
		versions, err := sourceRepo.Versions(sourceKey1).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, versions, 1)
	}

	// Create - already exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		source1 := test.NewSource(sourceKey1)
		if err := sourceRepo.Create("Create description", &source1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source "123/456/my-source-1" already exists in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}

	// Update
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Modify name
		result, err := sourceRepo.Update(sourceKey1, "Update description", func(v definition.Source) definition.Source {
			v.Name = "Modified Name"
			return v
		}).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "Modified Name", result.Name)
		assert.Equal(t, definition.VersionNumber(2), result.VersionNumber())

		source1, err := sourceRepo.Get(sourceKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, result, source1)
	}
	{
		// Modify description
		result, err := sourceRepo.Update(sourceKey1, "Update description", func(v definition.Source) definition.Source {
			v.Description = "Modified Description"
			return v
		}).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "Modified Description", result.Description)
		assert.Equal(t, definition.VersionNumber(3), result.VersionNumber())

		source1, err := sourceRepo.Get(sourceKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, result, source1)
	}

	// Update - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := sourceRepo.Update(nonExistentSourceKey, "Update description", func(v definition.Source) definition.Source {
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
		source, err := sourceRepo.Version(sourceKey1, 1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Source 1", source.Name)
		source, err = sourceRepo.Version(sourceKey1, 2).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "Modified Name", source.Name)
	}

	// Version - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := sourceRepo.Version(sourceKey1, 10).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source version "123/456/my-source-1/0000000010" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create some Sinks to test SoftDelete, in the next step
	// -----------------------------------------------------------------------------------------------------------------
	var sink1, sink2, sink3 definition.Sink
	{
		// Create 3 sinks
		sink1 = test.NewSink(key.SinkKey{SourceKey: sourceKey1, SinkID: "sink-1"})
		require.NoError(t, sinkRepo.Create("Create sink", &sink1).Do(ctx).Err())
		sink2 = test.NewSink(key.SinkKey{SourceKey: sourceKey1, SinkID: "sink-2"})
		require.NoError(t, sinkRepo.Create("Create sink", &sink2).Do(ctx).Err())
		sink3 = test.NewSink(key.SinkKey{SourceKey: sourceKey1, SinkID: "sink-3"})
		require.NoError(t, sinkRepo.Create("Create sink", &sink3).Do(ctx).Err())
	}
	{
		// Delete sink3 manually, so it should not be undeleted with the source1 later
		require.NoError(t, sinkRepo.SoftDelete(sink3.SinkKey).Do(ctx).Err())
	}

	// SoftDelete
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, sourceRepo.SoftDelete(sourceKey1).Do(ctx).Err())
	}
	{
		// Get - not found
		if err := sourceRepo.Get(sourceKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source "123/456/my-source-1" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// GetDeleted - found
		result, err := sourceRepo.GetDeleted(sourceKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "Modified Name", result.Name)
		assert.Equal(t, definition.VersionNumber(3), result.VersionNumber())
	}
	{
		// Version - found
		result, err := sourceRepo.Version(sourceKey1, 1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Source 1", result.Name)
		assert.Equal(t, definition.VersionNumber(1), result.VersionNumber())
	}
	{
		// List - empty
		sources, err := sourceRepo.List(sourceKey1.BranchKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, sources)
	}
	{
		// ListDeleted
		sources, err := sourceRepo.ListDeleted(sourceKey1.BranchKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, sources, 1)
	}

	// SoftDelete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := sourceRepo.SoftDelete(sourceKey1).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `source "123/456/my-source-1" not found in the branch`, err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Undelete
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Undelete
		result, err := sourceRepo.Undelete(sourceKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "Modified Name", result.Name)
		assert.Equal(t, definition.VersionNumber(3), result.VersionNumber())
	}
	{
		// ExistsOrErr
		assert.NoError(t, sourceRepo.ExistsOrErr(sourceKey1).Do(ctx).Err())
	}
	{
		// Get
		source1, err := sourceRepo.Get(sourceKey1).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			assert.Equal(t, "Modified Name", source1.Name)
			assert.Equal(t, definition.VersionNumber(3), source1.VersionNumber())
		}
	}
	{
		// GetDeleted - not found
		if err := sourceRepo.GetDeleted(sourceKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted source "123/456/my-source-1" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// List
		sources, err := sourceRepo.List(sourceKey1.BranchKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, sources, 1)
	}
	{
		// ListDeleted - empty
		sources, err := sourceRepo.ListDeleted(sourceKey1.BranchKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, sources)
	}

	// Undelete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := sourceRepo.Undelete(sourceKey1).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `deleted source "123/456/my-source-1" not found in the branch`, err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Re-create causes Undelete + new version record
	// -----------------------------------------------------------------------------------------------------------------
	{
		// SoftDelete
		source1, err := sourceRepo.Get(sourceKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, definition.VersionNumber(3), source1.VersionNumber())
		assert.NoError(t, sourceRepo.SoftDelete(sourceKey1).Do(ctx).Err())
	}
	{
		//  Re-create
		source1 := test.NewSource(sourceKey1)
		assert.NoError(t, sourceRepo.Create("Re-create", &source1).Do(ctx).Err())
		assert.Equal(t, definition.VersionNumber(4), source1.VersionNumber())
		assert.Equal(t, "My Source", source1.Name)
		assert.Equal(t, "My Description", source1.Description)
	}
	{
		// Get
		source1, err := sourceRepo.Get(sourceKey1).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			assert.Equal(t, definition.VersionNumber(4), source1.VersionNumber())
			assert.Equal(t, "My Source", source1.Name)
			assert.Equal(t, "My Description", source1.Description)
		}
	}
	{
		// Versions
		versions, err := sourceRepo.Versions(sourceKey1).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, versions, 4)
	}

	// Rollback version
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Rollback
		assert.NoError(t, sourceRepo.Rollback(sourceKey1, 2).Do(ctx).Err())
	}
	{
		// State after rollback
		result1, err := sourceRepo.Get(sourceKey1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Description", result1.Description)
		assert.Equal(t, definition.VersionNumber(5), result1.VersionNumber())
		assert.Equal(t, "Rollback to version 2", result1.VersionDescription())
	}

	// Rollback version - object not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := sourceRepo.Rollback(nonExistentSourceKey, 1).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `source "123/456/non-existent" not found in the branch`, err.Error())
	}

	// Rollback version - version not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := sourceRepo.Rollback(sourceKey1, 10).Do(ctx).Err(); assert.Error(t, err) {
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
}
