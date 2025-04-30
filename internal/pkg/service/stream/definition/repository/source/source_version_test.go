package source_test

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
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestSourceRepository_Versions(t *testing.T) {
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

	var err error

	// ListVersions - empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.ListVersions(sourceKey).Do(ctx).All()
		require.NoError(t, err)
		assert.Empty(t, result)
	}

	// Version - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.Version(sourceKey, definition.VersionNumber(1)).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source version "my-source/0000000001" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// RollbackVersion - entity not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.RollbackVersion(sourceKey, now, by, definition.VersionNumber(1)).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source "my-source" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - ok
	// -----------------------------------------------------------------------------------------------------------------
	var version1 definition.Source
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		version1 = test.NewSource(sourceKey)
		require.NoError(t, repo.Create(&version1, now, by, "Create source").Do(ctx).Err())
		assert.Equal(t, definition.VersionNumber(1), version1.Version.Number)
	}

	// Update - ok
	// -----------------------------------------------------------------------------------------------------------------
	var version2, version3 definition.Source
	{
		updateFn1 := func(source definition.Source) (definition.Source, error) {
			source.Description = "Updated description 1"
			return source, nil
		}
		updateFn2 := func(source definition.Source) (definition.Source, error) {
			source.Description = "Updated description 2"
			return source, nil
		}

		now = now.Add(time.Hour)
		version2, err = repo.Update(sourceKey, now, by, "Update source 1", updateFn1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, definition.VersionNumber(2), version2.Version.Number)

		now = now.Add(time.Hour)
		version3, err = repo.Update(sourceKey, now, by, "Update source 2", updateFn2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, definition.VersionNumber(3), version3.Version.Number)
	}

	// ListVersions - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.ListVersions(sourceKey).Do(ctx).All()
		require.NoError(t, err)
		assert.Len(t, result, 3)
		assert.Equal(t, []definition.Source{version1, version2, version3}, result)
	}

	// Version - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.Version(sourceKey, version2.VersionNumber()).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, version2, result)
	}

	// RollbackVersion - ok
	// -----------------------------------------------------------------------------------------------------------------
	var version4 definition.Source
	{
		now = now.Add(time.Hour)
		version4, err = repo.RollbackVersion(sourceKey, now, by, version2.VersionNumber()).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, definition.VersionNumber(4), version4.Version.Number)
	}

	// RollbackVersion - version not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.RollbackVersion(sourceKey, now, by, definition.VersionNumber(123)).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source version "my-source/0000000123" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Versions list - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.ListVersions(sourceKey).Do(ctx).All()
		require.NoError(t, err)
		assert.Len(t, result, 4)
		assert.Equal(t, []definition.Source{version1, version2, version3, version4}, result)
	}

	// Check database state
	// -----------------------------------------------------------------------------------------------------------------
	{
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/source_version_snapshot_001.txt", ignoredEtcdKeys)
	}
}
