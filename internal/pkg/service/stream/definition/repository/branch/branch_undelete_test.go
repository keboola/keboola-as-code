package branch_test

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

func TestBranchRepository_Undelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, mocked := dependencies.NewMockedServiceScope(t)
	client := mocked.TestEtcdClient()
	repo := d.DefinitionRepository().Branch()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}

	// Undelete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := repo.Undelete(branchKey, now, by).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `deleted branch "567" not found in the project`, err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Create - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, repo.Create(&branch, now, by).Do(ctx).Err())
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.Get(branchKey).Do(ctx).Err())
	}

	// SoftDelete - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, repo.SoftDelete(branchKey, now, by).Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/branch_undelete_test_snapshot_001.txt")
	}

	// Get - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := repo.Get(branchKey).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `branch "567" not found in the project`, err.Error())
		}
	}

	// Undelete - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.Undelete(branchKey, now.Add(time.Hour), by).Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/branch_undelete_test_snapshot_002.txt")
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.Get(branchKey).Do(ctx).Err())
	}

	// GetDeleted - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := repo.GetDeleted(branchKey).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `deleted branch "567" not found in the project`, err.Error())
		}
	}
}
