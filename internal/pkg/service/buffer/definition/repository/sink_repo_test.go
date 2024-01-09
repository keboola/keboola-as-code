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
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	deps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	serviceErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestRepository_Sink(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	nonExistentSinkKey := key.SinkKey{SourceKey: key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}, SinkID: "non-existent"}
	sinkKey1 := key.SinkKey{SourceKey: key.SourceKey{BranchKey: branchKey, SourceID: "my-source-1"}, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: key.SourceKey{BranchKey: branchKey, SourceID: "my-source-2"}, SinkID: "my-sink-2"}
	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2006-01-02T15:04:05.123Z").Time())

	d := deps.NewMocked(t, deps.WithEnabledEtcdClient(), deps.WithClock(clk))
	client := d.TestEtcdClient()
	r := New(d).Sink()

	// Empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		// List - empty
		sinks, err := r.List(projectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, sinks)
		sinks, err = r.List(sinkKey1.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, sinks)
	}
	{
		// ListDeleted - empty
		sinks, err := r.ListDeleted(projectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, sinks)
		sinks, err = r.ListDeleted(sinkKey1.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, sinks)
	}
	{
		// ExistsOrErr - not found
		if err := r.ExistsOrErr(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "123/456/my-source-1/my-sink-1" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// Get - not found
		if err := r.Get(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "123/456/my-source-1/my-sink-1" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// GetDeleted - not found
		if err := r.GetDeleted(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted sink "123/456/my-source-1/my-sink-1" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - parent Source doesn't exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		sink1 := sinkTemplate(sinkKey1)
		if err := r.Create("Create description", &sink1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source "123/456/my-source-1" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create parent branch and sources
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := branchTemplate(sinkKey1.BranchKey)
		require.NoError(t, r.all.Branch().Create(&branch).Do(ctx).Err())
		source1 := sourceTemplate(sinkKey1.SourceKey)
		require.NoError(t, r.all.Source().Create("Create source", &source1).Do(ctx).Err())
		source2 := sourceTemplate(sinkKey2.SourceKey)
		require.NoError(t, r.all.Source().Create("Create source", &source2).Do(ctx).Err())
	}

	// Create
	// -----------------------------------------------------------------------------------------------------------------
	{
		sink1 := sinkTemplate(sinkKey1)
		sink1.Name = "My Sink 1"
		result1, err := r.Create("Create description", &sink1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, sink1, result1)
		assert.Equal(t, definition.VersionNumber(1), sink1.VersionNumber())
		assert.NotEmpty(t, sink1.VersionHash())

		sink2 := sinkTemplate(sinkKey2)
		sink2.Name = "My Sink 2"
		result2, err := r.Create("Create description", &sink2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, sink2, result2)
		assert.Equal(t, definition.VersionNumber(1), sink2.VersionNumber())
		assert.NotEmpty(t, sink2.VersionHash())
	}
	{
		// List
		sinks, err := r.List(projectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sinks, 2)
		sinks, err = r.List(sinkKey1.BranchKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sinks, 2)
		sinks, err = r.List(sinkKey1.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sinks, 1)
		sinks, err = r.List(sinkKey2.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sinks, 1)
	}
	{
		// ExistsOrErr
		assert.NoError(t, r.ExistsOrErr(sinkKey1).Do(ctx).Err())
	}
	{
		// Get
		result1, err := r.Get(sinkKey1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Sink 1", result1.Name)
		assert.Equal(t, definition.VersionNumber(1), result1.VersionNumber())
		result2, err := r.Get(sinkKey2).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Sink 2", result2.Name)
		assert.Equal(t, definition.VersionNumber(1), result2.VersionNumber())
	}
	{
		// GetDeleted - not found
		if err := r.GetDeleted(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted sink "123/456/my-source-1/my-sink-1" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// Versions
		versions, err := r.Versions(sinkKey1).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, versions, 1)
	}

	// Create - already exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		sink1 := sinkTemplate(sinkKey1)
		if err := r.Create("Create description", &sink1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "123/456/my-source-1/my-sink-1" already exists in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}

	// Update
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Modify name
		result, err := r.Update(sinkKey1, "Update description", func(v definition.Sink) definition.Sink {
			v.Name = "Modified Name"
			return v
		}).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "Modified Name", result.Name)
		assert.Equal(t, definition.VersionNumber(2), result.VersionNumber())

		sink1, err := r.Get(sinkKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, result, sink1)
	}
	{
		// Modify description
		assert.NoError(t, r.Update(sinkKey1, "Update description", func(v definition.Sink) definition.Sink {
			v.Description = "Modified Description"
			return v
		}).Do(ctx).Err())

		sink1, err := r.Get(sinkKey1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "Modified Description", sink1.Description)
		assert.Equal(t, definition.VersionNumber(3), sink1.VersionNumber())
	}

	// Update - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := r.Update(nonExistentSinkKey, "Update description", func(v definition.Sink) definition.Sink {
			v.Name = "Modified Name"
			return v
		}).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `sink "123/456/my-source/non-existent" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Version - found
	// -----------------------------------------------------------------------------------------------------------------
	{
		sink, err := r.Version(sinkKey1, 1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Sink 1", sink.Name)
		sink, err = r.Version(sinkKey1, 2).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "Modified Name", sink.Name)
	}

	// Version - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := r.Version(sinkKey1, 10).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink version "123/456/my-source-1/my-sink-1/0000000010" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// SoftDelete
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, r.SoftDelete(sinkKey1).Do(ctx).Err())
	}
	{
		// ExistsOrErr - not found
		if err := r.ExistsOrErr(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "123/456/my-source-1/my-sink-1" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// Get - not found
		if err := r.Get(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "123/456/my-source-1/my-sink-1" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// GetDeleted - found
		result, err := r.GetDeleted(sinkKey1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "Modified Name", result.Name)
		assert.Equal(t, definition.VersionNumber(3), result.VersionNumber())
	}
	{
		// Version - found
		result, err := r.Version(sinkKey1, 1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Sink 1", result.Name)
		assert.Equal(t, definition.VersionNumber(1), result.VersionNumber())
	}
	{
		// List - empty
		sinks, err := r.List(sinkKey1.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, sinks)
	}
	{
		// ListDeleted
		sinks, err := r.ListDeleted(sinkKey1.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sinks, 1)
	}

	// SoftDelete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.SoftDelete(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `sink "123/456/my-source-1/my-sink-1" not found in the source`, err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Undelete
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Undelete
		result, err := r.Undelete(sinkKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "Modified Name", result.Name)
		assert.Equal(t, definition.VersionNumber(3), result.VersionNumber())
	}
	{
		// ExistsOrErr
		assert.NoError(t, r.ExistsOrErr(sinkKey1).Do(ctx).Err())
	}
	{
		// Get
		sink1, err := r.Get(sinkKey1).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			assert.Equal(t, "Modified Name", sink1.Name)
			assert.Equal(t, definition.VersionNumber(3), sink1.VersionNumber())
		}
	}
	{
		// GetDeleted - not found
		if err := r.GetDeleted(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted sink "123/456/my-source-1/my-sink-1" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// List
		sinks, err := r.List(sinkKey1.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sinks, 1)

	}
	{
		// ListDeleted - empty
		sinks, err := r.ListDeleted(sinkKey1.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, sinks)
	}

	// Undelete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.Undelete(sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `deleted sink "123/456/my-source-1/my-sink-1" not found in the source`, err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Re-create causes Undelete + new version record
	// -----------------------------------------------------------------------------------------------------------------
	{
		// SoftDelete
		sink1, err := r.Get(sinkKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, definition.VersionNumber(3), sink1.VersionNumber())
		assert.NoError(t, r.SoftDelete(sinkKey1).Do(ctx).Err())
	}
	{
		//  Re-create
		sink1 := sinkTemplate(sinkKey1)
		assert.NoError(t, r.Create("Re-create", &sink1).Do(ctx).Err())
		assert.Equal(t, definition.VersionNumber(4), sink1.VersionNumber())
		assert.Equal(t, "My Sink", sink1.Name)
		assert.Equal(t, "My Description", sink1.Description)
		assert.False(t, sink1.Deleted)
		assert.Nil(t, sink1.DeletedAt)
	}
	{
		// ExistsOrErr
		assert.NoError(t, r.ExistsOrErr(sinkKey1).Do(ctx).Err())
	}
	{
		// Get
		sink1, err := r.Get(sinkKey1).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			assert.Equal(t, definition.VersionNumber(4), sink1.VersionNumber())
			assert.Equal(t, "My Sink", sink1.Name)
			assert.Equal(t, "My Description", sink1.Description)
		}
	}
	{
		// Versions
		versions, err := r.Versions(sinkKey1).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, versions, 4)
	}

	// Rollback version
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Rollback
		assert.NoError(t, r.Rollback(sinkKey1, 2).Do(ctx).Err())
	}
	{
		// State after rollback
		result1, err := r.Get(sinkKey1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Description", result1.Description)
		assert.Equal(t, definition.VersionNumber(5), result1.VersionNumber())
		assert.Equal(t, "Rollback to version 2", result1.VersionDescription())
	}

	// Rollback version - object not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.Rollback(nonExistentSinkKey, 1).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `sink "123/456/my-source/non-existent" not found in the source`, err.Error())
	}

	// Rollback version - version not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.Rollback(sinkKey1, 10).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `sink version "123/456/my-source-1/my-sink-1/0000000010" not found in the source`, err.Error())
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
    "number": 5,
    "hash": "29c9075589e26aa6",
    "modifiedAt": "2006-01-02T15:04:05.123Z",
    "description": "Rollback to version 2"
  },
  "type": "table",
  "name": "Modified Name",
  "description": "My Description",
  "table": {
    "mapping": {
      "tableId": "in.bucket.table",
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
definition/sink/version/123/456/my-source-2/my-sink-2/0000000001
-----
%A
>>>>>
	`)

	// Test limit: sinks per branch
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Create sinks up to maximum count
		// Note: multiple puts are merged to a transaction to improve test speed
		txn := op.NewTxnOp(client)
		ops := 0
		for i := 2; i <= MaxSinksPerSource; i++ {
			v := sinkTemplate(key.SinkKey{SourceKey: sinkKey1.SourceKey, SinkID: key.SinkID(fmt.Sprintf("my-sink-%d", i))})
			v.IncrementVersion(v, clk.Now(), "Create")
			txn.Then(r.schema.Active().ByKey(v.SinkKey).Put(client, v))

			// Send the txn it is full, or after the last item
			ops++
			if ops == 100 || i == MaxSinksPerSource {
				// Send
				assert.NoError(t, txn.Do(ctx).Err())
				// Reset
				ops = 0
				txn = op.NewTxnOp(client)
			}
		}
		sinks, err := r.List(sinkKey1.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sinks, MaxSinksPerSource)
	}
	{
		// Exceed the limit
		sink := sinkTemplate(key.SinkKey{SourceKey: sinkKey1.SourceKey, SinkID: "over-maximum-count"})
		if err := r.Create("Create description", &sink).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, "sink count limit reached in the source, the maximum is 100", err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}

	// Test limit: versions per sink limit
	// -----------------------------------------------------------------------------------------------------------------
	{
		sink1, err := r.Get(sinkKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)

		// Create versions up to maximum count
		// Note: multiple puts are merged to a transaction to improve test speed
		txn := op.NewTxnOp(client)
		ops := 0
		for i := sink1.VersionNumber() + 1; i <= MaxSourceVersionsPerSource; i++ {
			sink1.Description = fmt.Sprintf("Description %04d", i)
			sink1.IncrementVersion(sink1, clk.Now(), "Some Update")
			txn.Then(r.schema.Versions().Of(sinkKey1).Version(sink1.VersionNumber()).Put(client, sink1))

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
		sinks, err := r.Versions(sinkKey1).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sinks, MaxSourceVersionsPerSource)
	}
	{
		// Exceed the limit
		err := r.Update(sinkKey1, "Some update", func(v definition.Sink) definition.Sink {
			v.Description = "foo"
			return v
		}).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, "version count limit reached in the sink, the maximum is 1000", err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}
}
