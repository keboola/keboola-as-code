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

func TestSinkRepository_Enable(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
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

	// Create branch - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())
	}

	// Enable - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.Enable(sinkKey, now, by).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "my-sink" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, by, "Create source").Do(ctx).Err())

		sink := dummy.NewSink(sinkKey)
		require.NoError(t, repo.Create(&sink, now, by, "Create sink").Do(ctx).Err())
		assert.Equal(t, definition.VersionNumber(1), sink.VersionNumber())
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.Get(sinkKey).Do(ctx).Err())
	}

	// Disable - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		sink, err := repo.Disable(sinkKey, now, by, "some reason").Do(ctx).ResultOrErr()
		require.NoError(t, err)
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/sink_enable_snapshot_001.txt", ignoredEtcdKeys)
		assert.Equal(t, definition.VersionNumber(2), sink.VersionNumber())
	}

	// Enable - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		sink, err := repo.Enable(sinkKey, now, by).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/sink_enable_snapshot_002.txt", ignoredEtcdKeys)
		assert.Equal(t, definition.VersionNumber(3), sink.VersionNumber())
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		sink, err := repo.Get(sinkKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink.IsEnabled())
		assert.True(t, sink.IsEnabledAt(now))
	}

	// Rollback to the disabled state
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		sink, err := repo.RollbackVersion(sinkKey, now, by, 2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.False(t, sink.IsDisabled()) // Rollback intentionally doesn't change disabled/enabled state
		assert.Equal(t, definition.VersionNumber(4), sink.VersionNumber())
	}

	// Rollback to the enabled state
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		sink, err := repo.RollbackVersion(sinkKey, now, by, 3).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink.IsEnabled())
		assert.Equal(t, definition.VersionNumber(5), sink.VersionNumber())
	}
}

func TestSinkRepository_EnableSinksOnBranchEnable(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
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

	// Create branch and source
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
		require.NoError(t, repo.Disable(sinkKey1, now, by, "some reason").Do(ctx).Err())
	}

	// Disable Branch
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		require.NoError(t, d.DefinitionRepository().Branch().Disable(branchKey, now, by, "Reason").Do(ctx).Err())
	}

	// Enable Branch
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		require.NoError(t, d.DefinitionRepository().Branch().Enable(branchKey, now, by).Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/sink_enable_snapshot_003.txt", ignoredEtcdKeys)
	}
	{
		var err error

		// Sink1 has been disabled before the Branch deletion, so it remains disabled.
		sink1, err = repo.Get(sinkKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink1.IsDisabled())

		// Sink2 and Sink2 are enabled
		sink2, err = repo.Get(sinkKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.False(t, sink2.IsDisabled())
		sink3, err = repo.Get(sinkKey3).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.False(t, sink3.IsDisabled())
	}
}

func TestSinkRepository_EnableSinksOnSourceEnable(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
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

	// Create branch and source
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
		require.NoError(t, repo.Disable(sinkKey1, now, by, "some reason").Do(ctx).Err())
	}

	// Disable Source
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		source, err := d.DefinitionRepository().Source().Disable(sourceKey, now, by, "Reason").Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, definition.VersionNumber(2), source.Version.Number)
	}

	// Enable Source
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		source, err := d.DefinitionRepository().Source().Enable(sourceKey, now, by).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, definition.VersionNumber(3), source.Version.Number)
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/sink_enable_snapshot_004.txt", ignoredEtcdKeys)
	}
	{
		var err error

		// Sink1 has been disabled before the Source has been disabled, so it remains disabled.
		sink1, err = repo.Get(sinkKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink1.IsDisabled())

		// Sink2 and Sink2 are enabled
		sink2, err = repo.Get(sinkKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink2.IsEnabled())
		sink3, err = repo.Get(sinkKey3).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink3.IsEnabled())
	}

	// Rollback Source to the disabled state
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		source, err := d.DefinitionRepository().Source().RollbackVersion(sourceKey, now, by, 2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, definition.VersionNumber(4), source.Version.Number)
		assert.False(t, source.IsDisabled()) // Rollback intentionally doesn't change disabled/enabled state
	}
	{
		// Disable the Source
		now = now.Add(time.Hour)
		source, err := d.DefinitionRepository().Source().Disable(sourceKey, now, by, "test").Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, source.IsDisabled())
	}
	{
		var err error

		// Sink1 has been disabled before the Source has been disabled, so it remains disabled.
		sink1, err = repo.Get(sinkKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink1.IsDisabled())

		// Sink2 and Sink2 are disabled
		sink2, err = repo.Get(sinkKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink2.IsDisabled())
		sink3, err = repo.Get(sinkKey3).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink3.IsDisabled())
	}
}
