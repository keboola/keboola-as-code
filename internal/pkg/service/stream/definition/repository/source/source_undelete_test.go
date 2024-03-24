package source_test

import (
	"context"
	"github.com/keboola/go-client/pkg/keboola"
	serviceErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
	"time"
)

func TestSourceRepository_Undelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()

	d, mocked := dependencies.NewMockedServiceScope(t)
	client := mocked.TestEtcdClient()
	repo := d.DefinitionRepository().Source()
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(definition/branch)")

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}

	// Create branch - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now).Do(ctx).Err())
	}

	// Undelete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := repo.Undelete(sourceKey, now).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `deleted source "my-source" not found in the branch`, err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Create - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		source := test.NewSource(sourceKey)
		require.NoError(t, repo.Create(&source, now, "Create source").Do(ctx).Err())
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.Get(sourceKey).Do(ctx).Err())
	}

	// SoftDelete - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, repo.SoftDelete(sourceKey, now).Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/source_undelete_test_snapshot_001.txt", ignoredEtcdKeys)
	}

	// Get - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := repo.Get(sourceKey).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `source "my-source" not found in the branch`, err.Error())
		}
	}

	// Undelete - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.Undelete(sourceKey, now.Add(time.Hour)).Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/source_undelete_test_snapshot_002.txt", ignoredEtcdKeys)
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.Get(sourceKey).Do(ctx).Err())
	}

	// GetDeleted - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := repo.GetDeleted(sourceKey).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `deleted source "my-source" not found in the branch`, err.Error())
		}
	}
}
