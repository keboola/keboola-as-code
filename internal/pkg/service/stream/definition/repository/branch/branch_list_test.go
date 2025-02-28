package branch_test

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
)

func TestBranchRepository_List(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, _ := dependencies.NewMockedServiceScope(t, ctx)
	repo := d.DefinitionRepository().Branch()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey1 := key.BranchKey{ProjectID: projectID, BranchID: 567}
	branchKey2 := key.BranchKey{ProjectID: projectID, BranchID: 789}

	// List - empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.List(projectID).Do(ctx).All()
		require.NoError(t, err)
		assert.Empty(t, result)
	}

	// Create two branches
	// -----------------------------------------------------------------------------------------------------------------
	var branch1, branch2 definition.Branch
	{
		branch1 = test.NewBranch(branchKey1)
		branch2 = test.NewBranch(branchKey2)
		require.NoError(t, repo.Create(&branch1, now, by).Do(ctx).Err())
		require.NoError(t, repo.Create(&branch2, now, by).Do(ctx).Err())
	}

	// List - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.List(projectID).Do(ctx).All()
		require.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, []definition.Branch{
			branch1,
			branch2,
		}, result)
	}
}

func TestBranchRepository_ListDeleted(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, _ := dependencies.NewMockedServiceScope(t, ctx)
	repo := d.DefinitionRepository().Branch()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey1 := key.BranchKey{ProjectID: projectID, BranchID: 567}
	branchKey2 := key.BranchKey{ProjectID: projectID, BranchID: 789}

	// ListDeleted - empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.ListDeleted(projectID).Do(ctx).All()
		require.NoError(t, err)
		assert.Empty(t, result)
	}

	// Create two branches
	// -----------------------------------------------------------------------------------------------------------------
	var branch1, branch2 definition.Branch
	{
		branch1 = test.NewBranch(branchKey1)
		branch2 = test.NewBranch(branchKey2)
		require.NoError(t, repo.Create(&branch1, now, by).Do(ctx).Err())
		require.NoError(t, repo.Create(&branch2, now, by).Do(ctx).Err())
	}

	// ListDeleted - empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.ListDeleted(projectID).Do(ctx).All()
		require.NoError(t, err)
		assert.Empty(t, result)
	}

	// SoftDelete two branches
	// -----------------------------------------------------------------------------------------------------------------
	{
		var err error
		branch1, err = repo.SoftDelete(branchKey1, now, by).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		branch2, err = repo.SoftDelete(branchKey2, now, by).Do(ctx).ResultOrErr()
		require.NoError(t, err)
	}

	// ListDeleted - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.ListDeleted(projectID).Do(ctx).All()
		require.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, []definition.Branch{
			branch1,
			branch2,
		}, result)
	}
}
