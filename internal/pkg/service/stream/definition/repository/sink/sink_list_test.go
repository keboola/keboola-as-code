package sink_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
)

func TestSinkRepository_List(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, _ := dependencies.NewMockedServiceScope(t, ctx)
	repo := d.DefinitionRepository().Sink()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey1 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-2"}

	// List - empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.List(projectID).Do(ctx).All()
		require.NoError(t, err)
		assert.Empty(t, result)
	}

	// Create two sinks
	// -----------------------------------------------------------------------------------------------------------------
	var sink1, sink2 definition.Sink
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, by, "Create source").Do(ctx).Err())

		sink1 = dummy.NewSink(sinkKey1)
		sink2 = dummy.NewSink(sinkKey2)
		require.NoError(t, repo.Create(&sink1, now, by, "Create sink").Do(ctx).Err())
		require.NoError(t, repo.Create(&sink2, now, by, "Create sink").Do(ctx).Err())
	}

	// List - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.List(projectID).Do(ctx).All()
		require.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, []definition.Sink{sink1, sink2}, result)
	}
}

func TestSinkRepository_ListDeleted(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, _ := dependencies.NewMockedServiceScope(t, ctx)
	repo := d.DefinitionRepository().Sink()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey1 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-2"}

	// ListDeleted - empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.ListDeleted(projectID).Do(ctx).All()
		require.NoError(t, err)
		assert.Empty(t, result)
	}

	// Create two sinks
	// -----------------------------------------------------------------------------------------------------------------
	var sink1, sink2 definition.Sink
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, by, "Create source").Do(ctx).Err())

		sink1 = dummy.NewSink(sinkKey1)
		require.NoError(t, repo.Create(&sink1, now, by, "Create sink").Do(ctx).Err())

		sink2 = dummy.NewSink(sinkKey2)
		require.NoError(t, repo.Create(&sink2, now, by, "Create sink").Do(ctx).Err())
	}

	// ListDeleted - empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.ListDeleted(projectID).Do(ctx).All()
		require.NoError(t, err)
		assert.Empty(t, result)
	}

	// SoftDelete two sinks
	// -----------------------------------------------------------------------------------------------------------------
	{
		var err error

		sink1, err = repo.SoftDelete(sinkKey1, now, by).Do(ctx).ResultOrErr()
		require.NoError(t, err)

		sink2, err = repo.SoftDelete(sinkKey2, now, by).Do(ctx).ResultOrErr()
		require.NoError(t, err)
	}

	// ListDeleted - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.ListDeleted(projectID).Do(ctx).All()
		require.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, []definition.Sink{sink1, sink2}, result)
	}
}
