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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestSinkRepository_Disable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, mocked := dependencies.NewMockedServiceScope(t, ctx)
	client := mocked.TestEtcdClient()
	repo := d.DefinitionRepository().Sink()
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(definition/branch|definition/source)")

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}

	// Disable - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.Disable(sinkKey, now, by, "some reason").Do(ctx).Err(); assert.Error(t, err) {
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

		sink := dummy.NewSink(sinkKey)
		require.NoError(t, repo.Create(&sink, now, by, "Create sink").Do(ctx).Err())
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.Get(sinkKey).Do(ctx).Err())
	}

	// Disable - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, repo.Disable(sinkKey, now, by, "some reason").Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/sink_disable_snapshot_001.txt", ignoredEtcdKeys)
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch, err := repo.Get(sinkKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, branch.IsDisabled())
		assert.True(t, branch.IsDisabledAt(now))
		assert.True(t, branch.IsDisabledDirectly())
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, repo.Get(sinkKey).Do(ctx).Err())
	}
}

func TestSinkRepository_DisabledSinksOnBranchDisabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, mocked := dependencies.NewMockedServiceScope(t, ctx)
	client := mocked.TestEtcdClient()
	repo := d.DefinitionRepository().Sink()
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(definition/branch|definition/source|definition/sink/version)")

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
		sink1 = dummy.NewSink(sinkKey1)
		require.NoError(t, repo.Create(&sink1, now, by, "Create sink").Do(ctx).Err())
		sink2 = dummy.NewSink(sinkKey2)
		require.NoError(t, repo.Create(&sink2, now, by, "Create sink").Do(ctx).Err())
		sink3 = dummy.NewSink(sinkKey3)
		require.NoError(t, repo.Create(&sink3, now, by, "Create sink").Do(ctx).Err())
	}

	// Disable Sink1
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		var err error
		sink1, err = repo.Disable(sinkKey1, now, by, "some reason").Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink1.IsDisabled())
		assert.True(t, sink1.IsDisabledAt(now))
	}

	// Disable Branch
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		require.NoError(t, d.DefinitionRepository().Branch().Disable(branchKey, now, by, "some reason").Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/sink_disable_snapshot_002.txt", ignoredEtcdKeys)
	}
	{
		var err error
		sink1, err = repo.Get(sinkKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink1.IsDisabled())
		assert.True(t, sink1.IsDisabledDirectly()) // Sink1 has been disabled directly, before the Branch has been disabled.
		sink2, err = repo.Get(sinkKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink2.IsDisabled())
		assert.False(t, sink2.IsDisabledDirectly())
		sink3, err = repo.Get(sinkKey3).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink3.IsDisabled())
		assert.False(t, sink3.IsDisabledDirectly())
	}
}

func TestSinkRepository_DisabledSinksOnSourceDisabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, mocked := dependencies.NewMockedServiceScope(t, ctx)
	client := mocked.TestEtcdClient()
	repo := d.DefinitionRepository().Sink()
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(definition/branch|definition/source|definition/sink/version)")

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
		sink1 = dummy.NewSink(sinkKey1)
		require.NoError(t, repo.Create(&sink1, now, by, "Create sink").Do(ctx).Err())
		sink2 = dummy.NewSink(sinkKey2)
		require.NoError(t, repo.Create(&sink2, now, by, "Create sink").Do(ctx).Err())
		sink3 = dummy.NewSink(sinkKey3)
		require.NoError(t, repo.Create(&sink3, now, by, "Create sink").Do(ctx).Err())
	}

	// Disable Sink1
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		var err error
		sink1, err = repo.Disable(sinkKey1, now, by, "some reason").Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink1.IsDisabled())
		assert.True(t, sink1.IsDisabledAt(now))
	}

	// Disable Source
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		require.NoError(t, d.DefinitionRepository().Source().Disable(sourceKey, now, by, "some reason").Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/sink_disable_snapshot_002.txt", ignoredEtcdKeys)
	}
	{
		var err error
		sink1, err = repo.Get(sinkKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink1.IsDisabled())
		assert.True(t, sink1.IsDisabledDirectly()) // Sink1 has been disabled directly, before the Source has been disabled.
		sink2, err = repo.Get(sinkKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink2.IsDisabled())
		assert.False(t, sink2.IsDisabledDirectly())
		sink3, err = repo.Get(sinkKey3).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink3.IsDisabled())
		assert.False(t, sink3.IsDisabledDirectly())
	}
}
