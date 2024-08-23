package branch_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	serviceErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestBranchRepository_Get(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, _ := dependencies.NewMockedServiceScope(t, ctx)
	repo := d.DefinitionRepository().Branch()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}

	// Get - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.Get(branchKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `branch "567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - ok
	// -----------------------------------------------------------------------------------------------------------------
	var branch definition.Branch
	{
		branch = test.NewBranch(branchKey)
		require.NoError(t, repo.Create(&branch, now, by).Do(ctx).Err())
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.Get(branchKey).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			assert.Equal(t, branch, result)
		}
	}
}

func TestBranchRepository_GetDeleted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, _ := dependencies.NewMockedServiceScope(t, ctx)
	repo := d.DefinitionRepository().Branch()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}

	// GetDeleted - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.GetDeleted(branchKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted branch "567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - ok
	// -----------------------------------------------------------------------------------------------------------------
	var branch definition.Branch
	{
		branch = test.NewBranch(branchKey)
		require.NoError(t, repo.Create(&branch, now, by).Do(ctx).Err())
	}

	// GetDeleted - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.GetDeleted(branchKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted branch "567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// SoftDelete - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.SoftDelete(branchKey, now, by).Do(ctx).Err())
	}

	// GetDeleted - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch.Deleted = &definition.Deleted{Directly: true, At: utctime.From(now), By: by}
		result, err := repo.GetDeleted(branchKey).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			assert.Equal(t, branch, result)
		}
	}
}

func TestBranchRepository_GetDefault(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, _ := dependencies.NewMockedServiceScope(t, ctx)
	repo := d.DefinitionRepository().Branch()

	// Fixtures
	projectID := keboola.ProjectID(123)
	defaultBranchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	anotherBranchKey := key.BranchKey{ProjectID: projectID, BranchID: 789}

	// GetDefault - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.GetDefault(projectID).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `branch "default" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create default branch
	// -----------------------------------------------------------------------------------------------------------------
	var defaultBranch definition.Branch
	{
		defaultBranch = test.NewBranch(defaultBranchKey)
		defaultBranch.IsDefault = true
		require.NoError(t, repo.Create(&defaultBranch, now, by).Do(ctx).Err())
	}

	// Create another branch
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(anotherBranchKey)
		require.NoError(t, repo.Create(&branch, now, by).Do(ctx).Err())
	}

	// GetDefault - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.GetDefault(projectID).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			assert.Equal(t, defaultBranch, result)
		}
	}
}
