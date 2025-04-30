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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestSourceRepository_Create(t *testing.T) {
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

	// Create - parent branch not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		source := test.NewSource(sourceKey)
		if err := repo.Create(&source, now, by, "Create source").Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `branch "567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		result, err := repo.Create(&source, now, by, "Create source").Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, source, result)
		assert.Equal(t, now, source.VersionModifiedAt().Time())

		etcdhelper.AssertKVsFromFile(t, client, "fixtures/source_create_snapshot_001.txt", ignoredEtcdKeys)
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.Get(sourceKey).Do(ctx).Err())
	}

	// Create - already exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		source := test.NewSource(sourceKey)
		if err := repo.Create(&source, now, by, "Create source").Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source "my-source" already exists in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}

	// SoftDelete - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.SoftDelete(sourceKey, now, by).Do(ctx).Err())
	}

	// Create - ok, undeleted
	// -----------------------------------------------------------------------------------------------------------------
	{
		source := test.NewSource(sourceKey)
		result, err := repo.Create(&source, now.Add(time.Hour), by, "Create source").Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, source, result)
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/source_create_snapshot_002.txt", ignoredEtcdKeys)
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.Get(sourceKey).Do(ctx).Err())
	}
}
