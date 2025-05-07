package branch_test

import (
	"net/http"
	"testing"

	"github.com/jonboulle/clockwork"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	serviceErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	branchrepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/branch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/branch/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestBranchRepository_Limits_BranchesPerProject(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	by := test.ByUser()

	clk := clockwork.NewFakeClockAt(utctime.MustParse("2006-01-02T15:04:05.123Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}

	// Get services
	d, mock := dependencies.NewMockedServiceScope(t, ctx, commonDeps.WithClock(clk))
	client := mock.TestEtcdClient()
	branchRepo := repository.New(d).Branch()
	branchSchema := schema.New(d.EtcdSerde())

	// Create branches up to maximum count
	// Note: multiple puts are merged to a transaction to improve test speed
	txn := op.Txn(client)
	ops := 0
	for i := 1; i <= branchrepo.MaxBranchesPerProject; i++ {
		branch := test.NewBranch(key.BranchKey{ProjectID: branchKey.ProjectID, BranchID: keboola.BranchID(1000 + i)})
		branch.SetCreation(clk.Now(), by)
		txn.Then(branchSchema.Active().ByKey(branch.BranchKey).Put(client, branch))

		// Send the txn if it is full, or after the last item
		ops++
		if ops == 100 || i == branchrepo.MaxBranchesPerProject {
			// Send
			require.NoError(t, txn.Do(ctx).Err())
			// Reset
			ops = 0
			txn = op.Txn(client)
		}
	}
	branches, err := branchRepo.List(branchKey.ProjectID).Do(ctx).AllKVs()
	require.NoError(t, err)
	assert.Len(t, branches, branchrepo.MaxBranchesPerProject)

	// Exceed the limit
	branch := test.NewBranch(key.BranchKey{ProjectID: 123, BranchID: 111111})
	if err := branchRepo.Create(&branch, clk.Now(), by).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, "branch count limit reached in the project, the maximum is 100", err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
	}
}
