package source_test

import (
	"net/http"
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	serviceErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestSourceRepository_ExistsOrErr(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, _ := dependencies.NewMockedServiceScope(t, ctx)
	branchRepo := d.DefinitionRepository().Branch()
	sourceRepo := d.DefinitionRepository().Source()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}

	// ExistsOrErr - branch not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := sourceRepo.ExistsOrErr(sourceKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `branch "567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create branch
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, branchRepo.Create(&branch, now, by).Do(ctx).Err())
	}

	// ExistsOrErr - source not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := sourceRepo.ExistsOrErr(sourceKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source "my-source" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create source
	// -----------------------------------------------------------------------------------------------------------------
	{
		source := test.NewSource(sourceKey)
		require.NoError(t, sourceRepo.Create(&source, now, by, "Create source").Do(ctx).Err())
	}

	// ExistsOrErr - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, sourceRepo.ExistsOrErr(sourceKey).Do(ctx).Err())
	}
}

func TestSourceRepository_MustNotExist(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, _ := dependencies.NewMockedServiceScope(t, ctx)
	branchRepo := d.DefinitionRepository().Branch()
	sourceRepo := d.DefinitionRepository().Source()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}

	// MustNotExist - branch not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := sourceRepo.MustNotExist(sourceKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `branch "567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create branch
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, branchRepo.Create(&branch, now, by).Do(ctx).Err())
	}

	// MustNotExist - source not found - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, sourceRepo.MustNotExist(sourceKey).Do(ctx).Err())
	}

	// Create source
	// -----------------------------------------------------------------------------------------------------------------
	{
		source := test.NewSource(sourceKey)
		require.NoError(t, sourceRepo.Create(&source, now, by, "Create source").Do(ctx).Err())
	}

	// MustNotExist - source found - error
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := sourceRepo.MustNotExist(sourceKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source "my-source" already exists in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}
}
