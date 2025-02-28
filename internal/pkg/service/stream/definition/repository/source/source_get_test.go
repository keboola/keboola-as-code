package source_test

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

func TestSourceRepository_Get(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, _ := dependencies.NewMockedServiceScope(t, ctx)
	repo := d.DefinitionRepository().Source()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}

	// Get - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.Get(sourceKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source "my-source" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - ok
	// -----------------------------------------------------------------------------------------------------------------
	var source definition.Source
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source = test.NewSource(sourceKey)
		require.NoError(t, repo.Create(&source, now, by, "Create source").Do(ctx).Err())
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.Get(sourceKey).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			assert.Equal(t, source, result)
		}
	}
}

func TestSourceRepository_GetDeleted(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, _ := dependencies.NewMockedServiceScope(t, ctx)
	repo := d.DefinitionRepository().Source()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}

	// GetDeleted - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.GetDeleted(sourceKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted source "my-source" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - ok
	// -----------------------------------------------------------------------------------------------------------------
	var source definition.Source
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source = test.NewSource(sourceKey)
		require.NoError(t, repo.Create(&source, now, by, "Create source").Do(ctx).Err())
	}

	// GetDeleted - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.GetDeleted(sourceKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted source "my-source" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// SoftDelete - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.SoftDelete(sourceKey, now, by).Do(ctx).Err())
	}

	// GetDeleted - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		source.Deleted = &definition.Deleted{Directly: true, At: utctime.From(now), By: by}
		result, err := repo.GetDeleted(sourceKey).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			assert.Equal(t, source, result)
		}
	}
}
