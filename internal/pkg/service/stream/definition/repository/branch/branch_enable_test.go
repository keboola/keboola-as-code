package branch_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	serviceErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestBranchRepository_Enable(t *testing.T) {
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

	// Enable - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := repo.Enable(branchKey, now, by).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `branch "567" not found in the project`, err.Error())
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

	// Disable - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, repo.Disable(branchKey, now, by, "some reason").Do(ctx).Err())
	}

	// Enable - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		require.NoError(t, repo.Enable(branchKey, now, by).Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/branch_enable_snapshot_001.txt")
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch, err := repo.Get(branchKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, branch.IsEnabled())
		assert.True(t, branch.IsEnabledAt(now))
	}
}
