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
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func sinkTemplate() definition.Sink {
	return definition.Sink{
		Type:        definition.SinkTypeTable,
		Name:        "My Sink",
		Description: "My Description",
		Table: &definition.TableSink{
			ImportConditions: definition.DefaultTableImportConditions(),
			Mapping: definition.TableMapping{
				TableID: keboola.MustParseTableID("in.bucket.table"),
				Columns: column.Columns{
					column.Datetime{Name: "datetime"},
					column.Body{Name: "body"},
				},
			},
		},
	}
}

func TestRepository_Sink(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Fixtures
	projectID := keboola.ProjectID(123)
	nonExistentSinkKey := key.SinkKey{
		SourceKey: key.SourceKey{
			BranchKey: key.BranchKey{ProjectID: projectID, BranchID: 456},
			SourceID:  "my-source",
		},
		SinkID: "non-existent",
	}
	sink1 := sinkTemplate()
	sink1.Name = "My Sink 1"
	sink1.SinkKey = key.SinkKey{
		SourceKey: key.SourceKey{
			BranchKey: key.BranchKey{ProjectID: projectID, BranchID: 456},
			SourceID:  "my-source-1",
		},
		SinkID: "my-sink-1",
	}
	sink2 := sinkTemplate()
	sink2.Name = "My Sink 2"
	sink2.SinkKey = key.SinkKey{
		SourceKey: key.SourceKey{
			BranchKey: key.BranchKey{ProjectID: projectID, BranchID: 456},
			SourceID:  "my-source-2",
		},
		SinkID: "my-sink-2",
	}

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2006-01-02T15:04:05.123Z").Time())

	d := deps.NewMocked(t, deps.WithEnabledEtcdClient(), deps.WithClock(clk))
	client := d.TestEtcdClient()
	r := NewRepository(d).Sink()

	// Empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		// List - empty
		sinks, err := r.List(projectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, sinks)
		sinks, err = r.List(sink1.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, sinks)
	}
	{
		// ListDeleted - empty
		sinks, err := r.ListDeleted(projectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, sinks)
		sinks, err = r.ListDeleted(sink1.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, sinks)
	}
	{
		// ExistsOrErr - not found
		if err := r.ExistsOrErr(sink1.SinkKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "123/456/my-source-1/my-sink-1" not found in the source`, err.Error())
			var errWithStatus serviceErrors.WithStatusCode
			if assert.True(t, errors.As(err, &errWithStatus)) {
				assert.Equal(t, http.StatusNotFound, errWithStatus.StatusCode())
			}
		}
	}
	{
		// Get - not found
		if err := r.Get(sink1.SinkKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "123/456/my-source-1/my-sink-1" not found in the source`, err.Error())
			var errWithStatus serviceErrors.WithStatusCode
			if assert.True(t, errors.As(err, &errWithStatus)) {
				assert.Equal(t, http.StatusNotFound, errWithStatus.StatusCode())
			}
		}
	}
	{
		// GetDeleted - not found
		if err := r.GetDeleted(sink1.SinkKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted sink "123/456/my-source-1/my-sink-1" not found in the source`, err.Error())
			var errWithStatus serviceErrors.WithStatusCode
			if assert.True(t, errors.As(err, &errWithStatus)) {
				assert.Equal(t, http.StatusNotFound, errWithStatus.StatusCode())
			}
		}
	}

	// Create - parent Source doesn't exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := r.Create("Create description", &sink1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source "123/456/my-source-1" not found in the branch`, err.Error())
			var errWithStatus serviceErrors.WithStatusCode
			if assert.True(t, errors.As(err, &errWithStatus)) {
				assert.Equal(t, http.StatusNotFound, errWithStatus.StatusCode())
			}
		}
	}

	// Create parent branch and sources
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := branchTemplate()
		branch.BranchKey = sink1.BranchKey
		require.NoError(t, r.all.Branch().Create(&branch).Do(ctx).Err())
		source1 := sourceTemplate()
		source1.SourceKey = sink1.SourceKey
		require.NoError(t, r.all.Source().Create("Create source", &source1).Do(ctx).Err())
		source2 := sourceTemplate()
		source2.SourceKey = sink2.SourceKey
		require.NoError(t, r.all.Source().Create("Create source", &source2).Do(ctx).Err())
	}

	// Create
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, r.Create("Create description", &sink1).Do(ctx).Err())
		assert.Equal(t, definition.VersionNumber(1), sink1.VersionNumber())
		assert.NotEmpty(t, sink1.VersionHash())
		require.NoError(t, r.Create("Create description", &sink2).Do(ctx).Err())
		assert.Equal(t, definition.VersionNumber(1), sink2.VersionNumber())
		assert.NotEmpty(t, sink2.VersionHash())
	}
	{
		// List
		sinks, err := r.List(projectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sinks, 2)
		sinks, err = r.List(sink1.BranchKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sinks, 2)
		sinks, err = r.List(sink1.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sinks, 1)
		sinks, err = r.List(sink2.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sinks, 1)
	}
	{
		// ExistsOrErr
		assert.NoError(t, r.ExistsOrErr(sink1.SinkKey).Do(ctx).Err())
	}
	{
		// Get
		result1, err := r.Get(sink1.SinkKey).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Sink 1", result1.Value.Name)
		assert.Equal(t, definition.VersionNumber(1), result1.Value.VersionNumber())
		result2, err := r.Get(sink2.SinkKey).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Sink 2", result2.Value.Name)
		assert.Equal(t, definition.VersionNumber(1), result2.Value.VersionNumber())
	}
	{
		// GetDeleted - not found
		if err := r.GetDeleted(sink1.SinkKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted sink "123/456/my-source-1/my-sink-1" not found in the source`, err.Error())
			var errWithStatus serviceErrors.WithStatusCode
			if assert.True(t, errors.As(err, &errWithStatus)) {
				assert.Equal(t, http.StatusNotFound, errWithStatus.StatusCode())
			}
		}
	}
	{
		// Versions
		versions, err := r.Versions(sink1.SinkKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, versions, 1)
	}

	// Create - already exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := r.Create("Create description", &sink1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "123/456/my-source-1/my-sink-1" already exists in the source`, err.Error())
			var errWithStatus serviceErrors.WithStatusCode
			if assert.True(t, errors.As(err, &errWithStatus)) {
				assert.Equal(t, http.StatusConflict, errWithStatus.StatusCode())
			}
		}
	}

	// Update
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Modify name
		assert.NoError(t, r.Update(sink1.SinkKey, "Update description", func(v definition.Sink) definition.Sink {
			v.Name = "Modified Name"
			return v
		}).Do(ctx).Err())
		kv, err := r.Get(sink1.SinkKey).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "Modified Name", kv.Value.Name)
		assert.Equal(t, definition.VersionNumber(2), kv.Value.VersionNumber())
	}
	{
		// Modify description
		assert.NoError(t, r.Update(sink1.SinkKey, "Update description", func(v definition.Sink) definition.Sink {
			v.Description = "Modified Description"
			return v
		}).Do(ctx).Err())
		kv, err := r.Get(sink1.SinkKey).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "Modified Description", kv.Value.Description)
		assert.Equal(t, definition.VersionNumber(3), kv.Value.VersionNumber())
		sink1 = kv.Value
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
		}
	}

	// Version - found
	// -----------------------------------------------------------------------------------------------------------------
	{
		sink, err := r.Version(sink1.SinkKey, 1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Sink 1", sink.Value.Name)
		sink, err = r.Version(sink1.SinkKey, 2).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "Modified Name", sink.Value.Name)
	}

	// Version - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := r.Version(sink1.SinkKey, 10).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink version "123/456/my-source-1/my-sink-1/0000000010" not found in the source`, err.Error())
			var errWithStatus serviceErrors.WithStatusCode
			if assert.True(t, errors.As(err, &errWithStatus)) {
				assert.Equal(t, http.StatusNotFound, errWithStatus.StatusCode())
			}
		}
	}

	// SoftDelete
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, r.SoftDelete(sink1.SinkKey).Do(ctx).Err())
	}
	{
		// ExistsOrErr - not found
		if err := r.ExistsOrErr(sink1.SinkKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "123/456/my-source-1/my-sink-1" not found in the source`, err.Error())
			var errWithStatus serviceErrors.WithStatusCode
			if assert.True(t, errors.As(err, &errWithStatus)) {
				assert.Equal(t, http.StatusNotFound, errWithStatus.StatusCode())
			}
		}
	}
	{
		// Get - not found
		if err := r.Get(sink1.SinkKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "123/456/my-source-1/my-sink-1" not found in the source`, err.Error())
			var errWithStatus serviceErrors.WithStatusCode
			if assert.True(t, errors.As(err, &errWithStatus)) {
				assert.Equal(t, http.StatusNotFound, errWithStatus.StatusCode())
			}
		}
	}
	{
		// GetDeleted - found
		result, err := r.GetDeleted(sink1.SinkKey).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "Modified Name", result.Value.Name)
		assert.Equal(t, definition.VersionNumber(3), result.Value.VersionNumber())
	}
	{
		// Version - found
		result, err := r.Version(sink1.SinkKey, 1).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Sink 1", result.Value.Name)
		assert.Equal(t, definition.VersionNumber(1), result.Value.VersionNumber())
	}
	{
		// List - empty
		sinks, err := r.List(sink1.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, sinks)
	}
	{
		// ListDeleted
		sinks, err := r.ListDeleted(sink1.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sinks, 1)
	}

	// SoftDelete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.SoftDelete(sink1.SinkKey).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `sink "123/456/my-source-1/my-sink-1" not found in the source`, err.Error())
	}

	// Undelete
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Undelete
		assert.NoError(t, r.Undelete(sink1.SinkKey).Do(ctx).Err())
	}
	{
		// ExistsOrErr
		assert.NoError(t, r.ExistsOrErr(sink1.SinkKey).Do(ctx).Err())
	}
	{
		// Get
		kv, err := r.Get(sink1.SinkKey).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			sink1 = kv.Value
			assert.Equal(t, "Modified Name", sink1.Name)
			assert.Equal(t, definition.VersionNumber(3), sink1.VersionNumber())
		}
	}
	{
		// GetDeleted - not found
		if err := r.GetDeleted(sink1.SinkKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted sink "123/456/my-source-1/my-sink-1" not found in the source`, err.Error())
			var errWithStatus serviceErrors.WithStatusCode
			if assert.True(t, errors.As(err, &errWithStatus)) {
				assert.Equal(t, http.StatusNotFound, errWithStatus.StatusCode())
			}
		}
	}
	{
		// List
		sinks, err := r.List(sink1.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sinks, 1)

	}
	{
		// ListDeleted - empty
		sinks, err := r.ListDeleted(sink1.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, sinks)
	}

	// Undelete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.Undelete(sink1.SinkKey).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `deleted sink "123/456/my-source-1/my-sink-1" not found in the source`, err.Error())
	}

	// Re-create causes Undelete + new version record
	// -----------------------------------------------------------------------------------------------------------------
	{
		// SoftDelete
		assert.Equal(t, definition.VersionNumber(3), sink1.VersionNumber())
		assert.NoError(t, r.SoftDelete(sink1.SinkKey).Do(ctx).Err())
	}
	{
		//  Re-create
		k := sink1.SinkKey
		sink1 = sinkTemplate()
		sink1.SinkKey = k
		assert.NoError(t, r.Create("Re-create", &sink1).Do(ctx).Err())
		assert.Equal(t, definition.VersionNumber(4), sink1.VersionNumber())
		assert.Equal(t, "My Sink", sink1.Name)
		assert.Equal(t, "My Description", sink1.Description)
	}
	{
		// ExistsOrErr
		assert.NoError(t, r.ExistsOrErr(sink1.SinkKey).Do(ctx).Err())
	}
	{
		// Get
		kv, err := r.Get(sink1.SinkKey).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			sink1 = kv.Value
			assert.Equal(t, definition.VersionNumber(4), sink1.VersionNumber())
			assert.Equal(t, "My Sink", sink1.Name)
			assert.Equal(t, "My Description", sink1.Description)
		}
	}
	{
		// Versions
		versions, err := r.Versions(sink1.SinkKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, versions, 4)
	}

	// Rollback version
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Rollback
		assert.NoError(t, r.Rollback(sink1.SinkKey, 2).Do(ctx).Err())
	}
	{
		// State after rollback
		result1, err := r.Get(sink1.SinkKey).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.Equal(t, "My Description", result1.Value.Description)
		assert.Equal(t, definition.VersionNumber(5), result1.Value.VersionNumber())
		assert.Equal(t, "Rollback to version 2", result1.Value.VersionDescription())
	}

	// Rollback version - object not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.Rollback(nonExistentSinkKey, 1).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `sink "123/456/my-source/non-existent" not found in the source`, err.Error())
	}

	// Rollback version - version not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.Rollback(sink1.SinkKey, 10).Do(ctx).Err(); assert.Error(t, err) {
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
    "hash": "fa72528723955f20",
    "modifiedAt": "2006-01-02T15:04:05.123Z",
    "description": "Rollback to version 2"
  },
  "type": "table",
  "name": "Modified Name",
  "description": "My Description",
  "table": {
    "uploadConditions": null,
    "importConditions": {
      "count": 10000,
      "size": "5MB",
      "time": 300000000000
    },
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
			id := key.SinkID(fmt.Sprintf("my-sink-%d", i))
			v := sinkTemplate()
			v.SinkKey = key.SinkKey{SourceKey: sink1.SourceKey, SinkID: id}
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
		sinks, err := r.List(sink1.SourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sinks, MaxSinksPerSource)
	}
	{
		// Exceed the limit
		sink := sinkTemplate()
		sink.SinkKey = key.SinkKey{
			SourceKey: sink1.SourceKey,
			SinkID:    "over-maximum-count",
		}
		if err := r.Create("Create description", &sink).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, "sink count limit reached in the source, the maximum is 100", err.Error())
			var errWithStatus serviceErrors.WithStatusCode
			if assert.True(t, errors.As(err, &errWithStatus)) {
				assert.Equal(t, http.StatusConflict, errWithStatus.StatusCode())
			}
		}
	}

	// Test limit: versions per sink limit
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Create versions up to maximum count
		// Note: multiple puts are merged to a transaction to improve test speed
		txn := op.NewTxnOp(client)
		ops := 0
		for i := sink1.VersionNumber() + 1; i <= MaxSourceVersionsPerSource; i++ {
			sink1.Description = fmt.Sprintf("Description %04d", i)
			sink1.IncrementVersion(sink1, clk.Now(), "Some Update")
			txn.Then(r.schema.Versions().Of(sink1.SinkKey).Version(sink1.VersionNumber()).Put(client, sink1))

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
		sinks, err := r.Versions(sink1.SinkKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, sinks, MaxSourceVersionsPerSource)
	}
	{
		// Exceed the limit
		err := r.Update(sink1.SinkKey, "Some update", func(v definition.Sink) definition.Sink {
			v.Description = "foo"
			return v
		}).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, "version count limit reached in the sink, the maximum is 1000", err.Error())
			var errWithStatus serviceErrors.WithStatusCode
			if assert.True(t, errors.As(err, &errWithStatus)) {
				assert.Equal(t, http.StatusConflict, errWithStatus.StatusCode())
			}
		}
	}
}
