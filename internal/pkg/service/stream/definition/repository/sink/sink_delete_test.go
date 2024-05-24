package sink_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	serviceErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestSinkRepository_SoftDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, mocked := dependencies.NewMockedServiceScope(t)
	client := mocked.TestEtcdClient()
	repo := d.DefinitionRepository().Sink()
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(definition/branch)|(definition/source)")

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}

	// SoftDelete - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.SoftDelete(sinkKey, now, by).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "my-sink" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, by, "Create source").Do(ctx).Err())

		sink := test.NewSink(sinkKey)
		require.NoError(t, repo.Create(&sink, now, by, "Create sink").Do(ctx).Err())
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.Get(sinkKey).Do(ctx).Err())
	}

	// SoftDelete - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, repo.SoftDelete(sinkKey, now, by).Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/sink_delete_snapshot_001.txt", ignoredEtcdKeys)
	}

	// Get - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.Get(sinkKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "my-sink" not found in the source`, err.Error())
		}
	}

	// GetDeleted - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, repo.GetDeleted(sinkKey).Do(ctx).Err())
	}
}

func TestSinkRepository_DeleteSinksOnSourceDelete_DeleteSource(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, mocked := dependencies.NewMockedServiceScope(t)
	client := mocked.TestEtcdClient()
	repo := d.DefinitionRepository().Sink()
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(definition/source/version)|(definition/sink/version)")

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey1 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-2"}
	sinkKey3 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-3"}

	// Create Branch and Source
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, by, "Create source").Do(ctx).Err())
	}

	// Create sinks
	// -----------------------------------------------------------------------------------------------------------------
	var sink1, sink2, sink3 definition.Sink
	{
		sink1 = test.NewSink(sinkKey1)
		require.NoError(t, repo.Create(&sink1, now, by, "Create sink").Do(ctx).Err())
		sink2 = test.NewSink(sinkKey2)
		require.NoError(t, repo.Create(&sink2, now, by, "Create sink").Do(ctx).Err())
		sink3 = test.NewSink(sinkKey3)
		require.NoError(t, repo.Create(&sink3, now, by, "Create sink").Do(ctx).Err())
	}

	// Delete Sink1
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		var err error
		sink1, err = repo.SoftDelete(sinkKey1, now, by).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink1.IsDeleted())
		assert.Equal(t, now, sink1.DeletedAt().Time())
	}

	// Delete Source
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		require.NoError(t, d.DefinitionRepository().Source().SoftDelete(sourceKey, now, by).Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/sink_delete_snapshot_002.txt", ignoredEtcdKeys)
	}
	{
		var err error
		sink1, err = repo.GetDeleted(sinkKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink1.IsDeleted())
		assert.True(t, sink1.IsDeletedDirectly()) // Sink1 has been deleted directly, before the Source deletion.
		sink2, err = repo.GetDeleted(sinkKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink2.IsDeleted())
		assert.False(t, sink2.IsDeletedDirectly())
		sink3, err = repo.GetDeleted(sinkKey3).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink3.IsDeleted())
		assert.False(t, sink3.IsDeletedDirectly())
	}
}

func TestSinkRepository_DeleteSinksOnSourceDelete_DeleteBranch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, mocked := dependencies.NewMockedServiceScope(t)
	client := mocked.TestEtcdClient()
	repo := d.DefinitionRepository().Sink()
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(definition/source/version)|(definition/sink/version)")

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey1 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-2"}
	sinkKey3 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-3"}

	// Create Branch and Source
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, by, "Create source").Do(ctx).Err())
	}

	// Create sinks
	// -----------------------------------------------------------------------------------------------------------------
	var sink1, sink2, sink3 definition.Sink
	{
		sink1 = test.NewSink(sinkKey1)
		require.NoError(t, repo.Create(&sink1, now, by, "Create sink").Do(ctx).Err())
		sink2 = test.NewSink(sinkKey2)
		require.NoError(t, repo.Create(&sink2, now, by, "Create sink").Do(ctx).Err())
		sink3 = test.NewSink(sinkKey3)
		require.NoError(t, repo.Create(&sink3, now, by, "Create sink").Do(ctx).Err())
	}

	// Delete Sink1
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		var err error
		sink1, err = repo.SoftDelete(sinkKey1, now, by).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink1.IsDeleted())
		assert.Equal(t, now, sink1.DeletedAt().Time())
	}

	// Delete Branch
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		require.NoError(t, d.DefinitionRepository().Branch().SoftDelete(branchKey, now, by).Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/sink_delete_snapshot_003.txt", ignoredEtcdKeys)
	}
	{
		var err error
		sink1, err = repo.GetDeleted(sinkKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink1.IsDeleted())
		assert.True(t, sink1.IsDeletedDirectly()) // Sink1 has been deleted directly, before the Source deletion.
		sink2, err = repo.GetDeleted(sinkKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink2.IsDeleted())
		assert.False(t, sink2.IsDeletedDirectly())
		sink3, err = repo.GetDeleted(sinkKey3).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink3.IsDeleted())
		assert.False(t, sink3.IsDeletedDirectly())
	}
}
