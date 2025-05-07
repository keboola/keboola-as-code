package sink_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	serviceErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestSinkRepository_Undelete(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, mocked := dependencies.NewMockedServiceScope(t, ctx)
	client := mocked.TestEtcdClient()
	repo := d.DefinitionRepository().Sink()
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(definition/branch)|(definition/source)")

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}

	// Create branch - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())
	}

	// Create source - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, by, "Create source").Do(ctx).Err())
	}

	// Undelete - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.Undelete(sinkKey, now, by).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted sink "my-sink" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		sink := dummy.NewSink(sinkKey)
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
		now = now.Add(time.Hour)
		require.NoError(t, repo.SoftDelete(sinkKey, now, by).Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/sink_undelete_snapshot_001.txt", ignoredEtcdKeys)
	}

	// Get - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := repo.Get(sinkKey).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `sink "my-sink" not found in the source`, err.Error())
		}
	}

	// Undelete - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		require.NoError(t, repo.Undelete(sinkKey, now.Add(time.Hour), by).Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/sink_undelete_snapshot_002.txt", ignoredEtcdKeys)
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.Get(sinkKey).Do(ctx).Err())
	}

	// GetDeleted - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := repo.GetDeleted(sinkKey).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `deleted sink "my-sink" not found in the source`, err.Error())
		}
	}
}

func TestSinkRepository_UndeleteSinksOnSourceUndelete_UndeleteSource(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, mocked := dependencies.NewMockedServiceScope(t, ctx)
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

	// Create sinks
	// -----------------------------------------------------------------------------------------------------------------
	var sink1, sink2, sink3 definition.Sink
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, by, "Create source").Do(ctx).Err())

		sink1 = dummy.NewSink(sinkKey1)
		require.NoError(t, repo.Create(&sink1, now, by, "Create sink").Do(ctx).Err())
		sink2 = dummy.NewSink(sinkKey2)
		require.NoError(t, repo.Create(&sink2, now, by, "Create sink").Do(ctx).Err())
		sink3 = dummy.NewSink(sinkKey3)
		require.NoError(t, repo.Create(&sink3, now, by, "Create sink").Do(ctx).Err())
	}

	// Delete Sink1
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		require.NoError(t, repo.SoftDelete(sinkKey1, now, by).Do(ctx).Err())
	}

	// Delete Source
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		require.NoError(t, d.DefinitionRepository().Source().SoftDelete(sourceKey, now, by).Do(ctx).Err())
	}

	// Undelete Source
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		require.NoError(t, d.DefinitionRepository().Source().Undelete(sourceKey, now, by).Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/sink_undelete_snapshot_003.txt", ignoredEtcdKeys)
	}
	{
		var err error

		// Sink1 has been deleted before the Branch deletion, so it remains deleted.
		sink1, err = repo.GetDeleted(sinkKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink1.IsDeleted())

		// Sink2 and Sink2
		sink2, err = repo.Get(sinkKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.False(t, sink2.IsDeleted())
		sink3, err = repo.Get(sinkKey3).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.False(t, sink3.IsDeleted())
	}
}

func TestSinkRepository_UndeleteSinksOnSourceUndelete_UndeleteBranch(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, mocked := dependencies.NewMockedServiceScope(t, ctx)
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

	// Create sinks
	// -----------------------------------------------------------------------------------------------------------------
	var sink1, sink2, sink3 definition.Sink
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, by, "Create source").Do(ctx).Err())

		sink1 = dummy.NewSink(sinkKey1)
		require.NoError(t, repo.Create(&sink1, now, by, "Create sink").Do(ctx).Err())
		sink2 = dummy.NewSink(sinkKey2)
		require.NoError(t, repo.Create(&sink2, now, by, "Create sink").Do(ctx).Err())
		sink3 = dummy.NewSink(sinkKey3)
		require.NoError(t, repo.Create(&sink3, now, by, "Create sink").Do(ctx).Err())
	}

	// Delete Sink1
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		require.NoError(t, repo.SoftDelete(sinkKey1, now, by).Do(ctx).Err())
	}

	// Delete Branch
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		require.NoError(t, d.DefinitionRepository().Branch().SoftDelete(branchKey, now, by).Do(ctx).Err())
	}

	// Undelete Branch
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		require.NoError(t, d.DefinitionRepository().Branch().Undelete(branchKey, now, by).Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/sink_undelete_snapshot_003.txt", ignoredEtcdKeys)
	}
	{
		var err error

		// Sink1 has been deleted before the Branch deletion, so it remains deleted.
		sink1, err = repo.GetDeleted(sinkKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink1.IsDeleted())

		// Sink2 and Sink2
		sink2, err = repo.Get(sinkKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.False(t, sink2.IsDeleted())
		sink3, err = repo.Get(sinkKey3).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.False(t, sink3.IsDeleted())
	}
}
