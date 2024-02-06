package repository

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	serviceErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/test"
)

func TestSourceLimits_SourcesPerBranch(t *testing.T) {
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
	repo := New(d)
	sourceRepo := repo.Source()

	// Create branch
	branch := test.NewBranch(branchKey)
	require.NoError(t, repo.Branch().Create(&branch).Do(ctx).Err())

	// Create sources up to maximum count
	// Note: multiple puts are merged to a transaction to improve test speed
	txn := op.Txn(client)
	ops := 0
	for i := 1; i <= MaxSourcesPerBranch; i++ {
		source := test.NewSource(key.SourceKey{BranchKey: branchKey, SourceID: key.SourceID(fmt.Sprintf("my-source-%d", i))})
		source.IncrementVersion(source, clk.Now(), "Create")
		txn.Then(sourceRepo.schema.Active().ByKey(source.SourceKey).Put(client, source))

		// Send the txn it is full, or after the last item
		ops++
		if ops == 100 || i == MaxSourcesPerBranch {
			// Send
			assert.NoError(t, txn.Do(ctx).Err())
			// Reset
			ops = 0
			txn = op.Txn(client)
		}
	}
	sources, err := sourceRepo.List(branchKey).Do(ctx).AllKVs()
	assert.NoError(t, err)
	assert.Len(t, sources, MaxSourcesPerBranch)

	// Exceed the limit
	source := test.NewSource(key.SourceKey{BranchKey: key.BranchKey{ProjectID: projectID, BranchID: 456}, SourceID: "over-maximum-count"})
	if err := sourceRepo.Create("Create description", &source).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, "source count limit reached in the branch, the maximum is 100", err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
	}
}

func TestSourceLimits_VersionsPerSource(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2006-01-02T15:04:05.123Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source-1"}

	// Get services
	d := commonDeps.NewMocked(t, commonDeps.WithEnabledEtcdClient(), commonDeps.WithClock(clk))
	client := d.TestEtcdClient()
	repo := New(d)
	sourceRepo := repo.Source()

	// Create branch
	branch := test.NewBranch(branchKey)
	require.NoError(t, repo.Branch().Create(&branch).Do(ctx).Err())

	// Create source
	source := test.NewSource(sourceKey)
	require.NoError(t, sourceRepo.Create("Create", &source).Do(ctx).Err())

	// Create versions up to maximum count
	// Note: multiple puts are merged to a transaction to improve test speed
	txn := op.Txn(client)
	ops := 0
	for i := source.VersionNumber() + 1; i <= MaxSourceVersionsPerSource; i++ {
		source.Description = fmt.Sprintf("Description %04d", i)
		source.IncrementVersion(source, clk.Now(), "Some Update")
		txn.Then(sourceRepo.schema.Versions().Of(sourceKey).Version(source.VersionNumber()).Put(client, source))

		// Send the txn it is full, or after the last item
		ops++
		if ops == 100 || i == MaxSourceVersionsPerSource {
			// Send
			assert.NoError(t, txn.Do(ctx).Err())
			// Reset
			ops = 0
			txn = op.Txn(client)
		}
	}
	// Check that the maximum count is reached
	sources, err := sourceRepo.Versions(sourceKey).Do(ctx).AllKVs()
	assert.NoError(t, err)
	assert.Len(t, sources, MaxSourceVersionsPerSource)

	// Exceed the limit
	err = sourceRepo.Update(sourceKey, "Some update", func(v definition.Source) definition.Source {
		v.Description = "foo"
		return v
	}).Do(ctx).Err()
	if assert.Error(t, err) {
		assert.Equal(t, "version count limit reached in the source, the maximum is 1000", err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
	}
}
