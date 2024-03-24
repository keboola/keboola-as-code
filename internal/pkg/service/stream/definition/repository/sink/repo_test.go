package sink_test

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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestRepository_Sink(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2006-01-02T15:04:05.123Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	nonExistentSinkKey := key.SinkKey{SourceKey: key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}, SinkID: "non-existent"}
	sinkKey1 := key.SinkKey{SourceKey: key.SourceKey{BranchKey: branchKey, SourceID: "my-source-1"}, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: key.SourceKey{BranchKey: branchKey, SourceID: "my-source-2"}, SinkID: "my-sink-2"}

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
		sinks, err := sinkRepo.List(projectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, sinks)
		sinks, err = sinkRepo.List(sinkKey1.SourceKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, sinks)
	}
	{
		// ListDeleted - empty
		sinks, err := sinkRepo.ListDeleted(projectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, sinks)
		sinks, err = sinkRepo.ListDeleted(sinkKey1.SourceKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, sinks)
	}
	{
		// ExistsOrErr - not found
		if err := sinkRepo.ExistsOrErr(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "my-sink-1" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// Get - not found
		if err := sinkRepo.Get(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "my-sink-1" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// GetDeleted - not found
		if err := sinkRepo.GetDeleted(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted sink "my-sink-1" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - parent Source doesn't exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		sink1 := test.NewSink(sinkKey1)
		if err := sinkRepo.Create(&sink1, clk.Now(), "Create description").Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source "my-source-1" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create parent branch and sources
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(sinkKey1.BranchKey)
		require.NoError(t, branchRepo.Create(&branch, clk.Now()).Do(ctx).Err())
		source1 := test.NewSource(sinkKey1.SourceKey)
		require.NoError(t, sourceRepo.Create(&source1, clk.Now(), "Create source").Do(ctx).Err())
		source2 := test.NewSource(sinkKey2.SourceKey)
		require.NoError(t, sourceRepo.Create(&source2, clk.Now(), "Create source").Do(ctx).Err())
	}

	// Create
	// -----------------------------------------------------------------------------------------------------------------
	{
		sink1 := test.NewSink(sinkKey1)
		sink1.Name = "My Sink 1"
		sink1.Config = sink1.Config.With(testconfig.StorageConfigPatch())
		result1, err := sinkRepo.Create(&sink1, clk.Now(), "Create description").Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, sink1, result1)
		assert.Equal(t, definition.VersionNumber(1), sink1.VersionNumber())
		assert.NotEmpty(t, sink1.VersionHash())

		sink2 := test.NewSink(sinkKey2)
		sink2.Name = "My Sink 2"
		result2, err := sinkRepo.Create(&sink2, clk.Now(), "Create description").Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, sink2, result2)
		assert.Equal(t, definition.VersionNumber(1), sink2.VersionNumber())
		assert.NotEmpty(t, sink2.VersionHash())
	}
	{
		// List
		sinks, err := sinkRepo.List(projectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, sinks, 2)
		sinks, err = sinkRepo.List(sinkKey1.BranchKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, sinks, 2)
		sinks, err = sinkRepo.List(sinkKey1.SourceKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, sinks, 1)
		sinks, err = sinkRepo.List(sinkKey2.SourceKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, sinks, 1)
	}
	{
		// ExistsOrErr
		assert.NoError(t, sinkRepo.ExistsOrErr(sinkKey1).Do(ctx).Err())
	}
	{
		// Get
		result1, err := sinkRepo.Get(sinkKey1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Sink 1", result1.Name)
		assert.Equal(t, definition.VersionNumber(1), result1.VersionNumber())
		result2, err := sinkRepo.Get(sinkKey2).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Sink 2", result2.Name)
		assert.Equal(t, definition.VersionNumber(1), result2.VersionNumber())
	}
	{
		// GetDeleted - not found
		if err := sinkRepo.GetDeleted(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted sink "my-sink-1" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// Versions
		versions, err := sinkRepo.Versions(sinkKey1).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, versions, 1)
	}

	// Create - already exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		sink1 := test.NewSink(sinkKey1)
		if err := sinkRepo.Create(&sink1, clk.Now(), "Create description").Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "my-sink-1" already exists in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}

	// Update
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Modify name
		result, err := sinkRepo.Update(sinkKey1, clk.Now(), "Update description", func(v definition.Sink) (definition.Sink, error) {
			v.Name = "Modified Name"
			return v, nil
		}).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "Modified Name", result.Name)
		assert.Equal(t, definition.VersionNumber(2), result.VersionNumber())

		sink1, err := sinkRepo.Get(sinkKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, result, sink1)
	}
	{
		// Modify description
		assert.NoError(t, sinkRepo.Update(sinkKey1, clk.Now(), "Update description", func(v definition.Sink) (definition.Sink, error) {
			v.Description = "Modified Description"
			return v, nil
		}).Do(ctx).Err())

		sink1, err := sinkRepo.Get(sinkKey1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "Modified Description", sink1.Description)
		assert.Equal(t, definition.VersionNumber(3), sink1.VersionNumber())
	}

	// Update - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := sinkRepo.Update(nonExistentSinkKey, clk.Now(), "Update description", func(v definition.Sink) (definition.Sink, error) {
			v.Name = "Modified Name"
			return v, nil
		}).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `sink "non-existent" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// GetVersion - found
	// -----------------------------------------------------------------------------------------------------------------
	{
		sink, err := sinkRepo.Version(sinkKey1, 1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Sink 1", sink.Name)
		sink, err = sinkRepo.Version(sinkKey1, 2).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "Modified Name", sink.Name)
	}

	// GetVersion - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := sinkRepo.Version(sinkKey1, 10).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink version "my-sink-1/0000000010" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// SoftDelete
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, sinkRepo.SoftDelete(sinkKey1, clk.Now()).Do(ctx).Err())
	}
	{
		// ExistsOrErr - not found
		if err := sinkRepo.ExistsOrErr(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "my-sink-1" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// Get - not found
		if err := sinkRepo.Get(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "my-sink-1" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// GetDeleted - found
		result, err := sinkRepo.GetDeleted(sinkKey1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "Modified Name", result.Name)
		assert.Equal(t, definition.VersionNumber(3), result.VersionNumber())
	}
	{
		// GetVersion - found
		result, err := sinkRepo.Version(sinkKey1, 1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Sink 1", result.Name)
		assert.Equal(t, definition.VersionNumber(1), result.VersionNumber())
	}
	{
		// List - empty
		sinks, err := sinkRepo.List(sinkKey1.SourceKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, sinks)
	}
	{
		// ListDeleted
		sinks, err := sinkRepo.ListDeleted(sinkKey1.SourceKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, sinks, 1)
	}

	// SoftDelete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := sinkRepo.SoftDelete(sinkKey1, clk.Now()).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `sink "my-sink-1" not found in the source`, err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Undelete
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Undelete
		result, err := sinkRepo.Undelete(sinkKey1, clk.Now()).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "Modified Name", result.Name)
		assert.Equal(t, definition.VersionNumber(4), result.VersionNumber())
		assert.Equal(t, `Undeleted to version "3".`, result.Version.Description)
	}
	{
		// ExistsOrErr
		assert.NoError(t, sinkRepo.ExistsOrErr(sinkKey1).Do(ctx).Err())
	}
	{
		// Get
		sink1, err := sinkRepo.Get(sinkKey1).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			assert.Equal(t, "Modified Name", sink1.Name)
			assert.Equal(t, definition.VersionNumber(4), sink1.VersionNumber())
		}
	}
	{
		// GetDeleted - not found
		if err := sinkRepo.GetDeleted(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted sink "my-sink-1" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// List
		sinks, err := sinkRepo.List(sinkKey1.SourceKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, sinks, 1)
	}
	{
		// ListDeleted - empty
		sinks, err := sinkRepo.ListDeleted(sinkKey1.SourceKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, sinks)
	}

	// Undelete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := sinkRepo.Undelete(sinkKey1, clk.Now()).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `deleted sink "my-sink-1" not found in the source`, err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Re-create causes Undelete + new version record
	// -----------------------------------------------------------------------------------------------------------------
	{
		// SoftDelete
		sink1, err := sinkRepo.Get(sinkKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, definition.VersionNumber(4), sink1.VersionNumber())
		assert.NoError(t, sinkRepo.SoftDelete(sinkKey1, clk.Now()).Do(ctx).Err())
	}
	{
		//  Re-create
		sink1 := test.NewSink(sinkKey1)
		assert.NoError(t, sinkRepo.Create(&sink1, clk.Now(), "Re-create").Do(ctx).Err())
		assert.Equal(t, definition.VersionNumber(5), sink1.VersionNumber())
		assert.Equal(t, "My Sink", sink1.Name)
		assert.Equal(t, "My Description", sink1.Description)
		assert.False(t, sink1.Deleted)
		assert.Nil(t, sink1.DeletedAt)
	}
	{
		// ExistsOrErr
		assert.NoError(t, sinkRepo.ExistsOrErr(sinkKey1).Do(ctx).Err())
	}
	{
		// Get
		sink1, err := sinkRepo.Get(sinkKey1).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			assert.Equal(t, definition.VersionNumber(5), sink1.VersionNumber())
			assert.Equal(t, "My Sink", sink1.Name)
			assert.Equal(t, "My Description", sink1.Description)
		}
	}
	{
		// Versions
		versions, err := sinkRepo.Versions(sinkKey1).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, versions, 5)
	}

	// Rollback version
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Rollback
		assert.NoError(t, sinkRepo.Rollback(sinkKey1, clk.Now(), 2).Do(ctx).Err())
	}
	{
		// State after rollback
		result1, err := sinkRepo.Get(sinkKey1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Description", result1.Description)
		assert.Equal(t, definition.VersionNumber(6), result1.VersionNumber())
		assert.Equal(t, `Rollback to version "2".`, result1.VersionDescription())
	}

	// Rollback version - object not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := sinkRepo.Rollback(nonExistentSinkKey, clk.Now(), 1).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `sink "non-existent" not found in the source`, err.Error())
	}

	// Rollback version - version not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := sinkRepo.Rollback(sinkKey1, clk.Now(), 10).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `sink version "my-sink-1/0000000010" not found in the source`, err.Error())
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
definition/source/active/123/456/my-source-1
-----
%A
>>>>>

<<<<<
definition/source/active/123/456/my-source-2
-----
%A
>>>>>

<<<<<
definition/source/version/123/456/my-source-1/0000000001
-----
%A
>>>>>

<<<<<
definition/source/version/123/456/my-source-2/0000000001
-----
%A
>>>>>

<<<<<
definition/sink/active/123/456/my-source-1/my-sink-1
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source-1",
  "sinkId": "my-sink-1",
  "version": {
    "number": 6,
    "hash": "482251bb3368c3e2",
    "modifiedAt": "2006-01-02T15:04:05.123Z",
    "description": "Rollback to version \"2\"."
  },
  "type": "table",
  "name": "Modified Name",
  "description": "My Description",
  "config": [
    {
      "key": "storage.level.local.volume.assignment.count",
      "value": 1
    },
    {
      "key": "storage.level.local.volume.assignment.preferredTypes",
      "value": [
        "default"
      ]
    },
    {
      "key": "storage.level.local.volume.sync.bytesTrigger",
      "value": "100KB"
    },
    {
      "key": "storage.level.local.volume.sync.checkInterval",
      "value": "1ms"
    },
    {
      "key": "storage.level.local.volume.sync.countTrigger",
      "value": 100
    },
    {
      "key": "storage.level.local.volume.sync.intervalTrigger",
      "value": "100ms"
    },
    {
      "key": "storage.level.local.volume.sync.mode",
      "value": "disk"
    },
    {
      "key": "storage.level.local.volume.sync.wait",
      "value": false
    }
  ],
  "table": {
    "type": "keboola",
    "keboola": {
      "tableId": "in.bucket.table"
    },
    "mapping": {
      "columns": [
        {
          "type": "datetime",
          "name": "datetime"
        },
        {
          "type": "body",
          "name": "body"
        }
      ]
    }
  }
}
>>>>>

<<<<<
definition/sink/active/123/456/my-source-2/my-sink-2
-----
%A
>>>>>

<<<<<
definition/sink/version/123/456/my-source-1/my-sink-1/0000000001
-----
%A
>>>>>

<<<<<
definition/sink/version/123/456/my-source-1/my-sink-1/0000000002
-----
%A
>>>>>

<<<<<
definition/sink/version/123/456/my-source-1/my-sink-1/0000000003
-----
%A
>>>>>

<<<<<
definition/sink/version/123/456/my-source-1/my-sink-1/0000000004
-----
%A
>>>>>

<<<<<
definition/sink/version/123/456/my-source-1/my-sink-1/0000000005
-----
%A
>>>>>

<<<<<
definition/sink/version/123/456/my-source-1/my-sink-1/0000000006
-----
%A
>>>>>

<<<<<
definition/sink/version/123/456/my-source-2/my-sink-2/0000000001
-----
%A
>>>>>
	`)
}
