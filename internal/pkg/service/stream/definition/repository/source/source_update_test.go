package source_test

import (
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
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestSourceRepository_Update(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, mocked := dependencies.NewMockedServiceScope(t, ctx)
	client := mocked.TestEtcdClient()
	repo := d.DefinitionRepository().Source()
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(definition/branch)")

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}

	// Update - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		updateFn := func(source definition.Source) (definition.Source, error) {
			return source, nil
		}
		if err := repo.Update(sourceKey, now, by, "Update source", updateFn).Do(ctx).Err(); assert.Error(t, err) {
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

	// Update - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)

		updateFn := func(source definition.Source) (definition.Source, error) {
			source.Description = "Updated description"
			return source, nil
		}

		var err error
		source, err = repo.Update(sourceKey, now, by, "Update source", updateFn).Do(ctx).ResultOrErr()
		require.NoError(t, err)

		etcdhelper.AssertKVsFromFile(t, client, "fixtures/source_update_snapshot_001.txt", ignoredEtcdKeys)
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.Get(sourceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, source, result)
	}

	// Update - error from the update function
	// -----------------------------------------------------------------------------------------------------------------
	{
		updateFn := func(source definition.Source) (definition.Source, error) {
			return definition.Source{}, errors.New("some error")
		}

		err := repo.Update(sourceKey, now, by, "Update source", updateFn).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, "some error", err.Error())
		}
	}

	// Update - "Disabled" field cannot be modified by the Update operation
	// -----------------------------------------------------------------------------------------------------------------
	{
		updateFn := func(source definition.Source) (definition.Source, error) {
			source.Disable(now, test.ByUser(), "some reason", true)
			return source, nil
		}
		err := repo.Update(sourceKey, now, by, "Update source", updateFn).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `"Disabled" field cannot be modified by the Update operation`, err.Error())
		}
	}

	// Update - "Deleted" field cannot be modified by the Update operation
	// -----------------------------------------------------------------------------------------------------------------
	{
		updateFn := func(source definition.Source) (definition.Source, error) {
			source.Delete(now, test.ByUser(), true)
			return source, nil
		}
		err := repo.Update(sourceKey, now, by, "Update source", updateFn).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `"Deleted" field cannot be modified by the Update operation`, err.Error())
		}
	}
}
