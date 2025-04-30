package source_test

import (
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestSourceRepository_List(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, _ := dependencies.NewMockedServiceScope(t, ctx)
	repo := d.DefinitionRepository().Source()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey1 := key.SourceKey{BranchKey: branchKey, SourceID: "my-source-1"}
	sourceKey2 := key.SourceKey{BranchKey: branchKey, SourceID: "my-source-2"}

	// List - empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.List(projectID).Do(ctx).All()
		require.NoError(t, err)
		assert.Empty(t, result)
	}

	// Create two sources
	// -----------------------------------------------------------------------------------------------------------------
	var source1, source2 definition.Source
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source1 = test.NewSource(sourceKey1)
		source2 = test.NewSource(sourceKey2)
		require.NoError(t, repo.Create(&source1, now, by, "Create source").Do(ctx).Err())
		require.NoError(t, repo.Create(&source2, now, by, "Create source").Do(ctx).Err())
	}

	// List - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.List(projectID).Do(ctx).All()
		require.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, []definition.Source{source1, source2}, result)
	}
}

func TestSourceRepository_ListDeleted(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, _ := dependencies.NewMockedServiceScope(t, ctx)
	repo := d.DefinitionRepository().Source()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey1 := key.SourceKey{BranchKey: branchKey, SourceID: "my-source-1"}
	sourceKey2 := key.SourceKey{BranchKey: branchKey, SourceID: "my-source-2"}

	// ListDeleted - empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.ListDeleted(projectID).Do(ctx).All()
		require.NoError(t, err)
		assert.Empty(t, result)
	}

	// Create two sources
	// -----------------------------------------------------------------------------------------------------------------
	var source1, source2 definition.Source
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source1 = test.NewSource(sourceKey1)
		source2 = test.NewSource(sourceKey2)
		require.NoError(t, repo.Create(&source1, now, by, "Create source").Do(ctx).Err())
		require.NoError(t, repo.Create(&source2, now, by, "Create source").Do(ctx).Err())
	}

	// ListDeleted - empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.ListDeleted(projectID).Do(ctx).All()
		require.NoError(t, err)
		assert.Empty(t, result)
	}

	// SoftDelete two sources
	// -----------------------------------------------------------------------------------------------------------------
	{
		var err error
		source1, err = repo.SoftDelete(sourceKey1, now, by).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		source2, err = repo.SoftDelete(sourceKey2, now, by).Do(ctx).ResultOrErr()
		require.NoError(t, err)
	}

	// ListDeleted - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.ListDeleted(projectID).Do(ctx).All()
		require.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, []definition.Source{source1, source2}, result)
	}
}
