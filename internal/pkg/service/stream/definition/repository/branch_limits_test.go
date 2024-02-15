package repository

import (
	"context"
	"net/http"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	serviceErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestBranchLimits_BranchesPerProject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2006-01-02T15:04:05.123Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}

	// Get services
	d := commonDeps.NewMocked(t, commonDeps.WithEnabledEtcdClient(), commonDeps.WithClock(clk))
	client := d.TestEtcdClient()
	branchRepo := New(d).Branch()

	// Create branches up to maximum count
	// Note: multiple puts are merged to a transaction to improve test speed
	txn := op.Txn(client)
	ops := 0
	for i := 1; i <= MaxBranchesPerProject; i++ {
		branch := test.NewBranch(key.BranchKey{ProjectID: branchKey.ProjectID, BranchID: keboola.BranchID(1000 + i)})
		txn.Then(branchRepo.schema.Active().ByKey(branch.BranchKey).Put(client, branch))

		// Send the txn it is full, or after the last item
		ops++
		if ops == 100 || i == MaxBranchesPerProject {
			// Send
			assert.NoError(t, txn.Do(ctx).Err())
			// Reset
			ops = 0
			txn = op.Txn(client)
		}
	}
	branches, err := branchRepo.List(branchKey.ProjectID).Do(ctx).AllKVs()
	assert.NoError(t, err)
	assert.Len(t, branches, MaxBranchesPerProject)

	// Exceed the limit
	branch := test.NewBranch(key.BranchKey{ProjectID: 123, BranchID: 111111})
	if err := branchRepo.Create(&branch).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, "branch count limit reached in the project, the maximum is 100", err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
	}
}
