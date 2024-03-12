package repository_test

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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
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
	d, mock := dependencies.NewMockedServiceScope(t, commonDeps.WithClock(clk))
	client := mock.TestEtcdClient()
	repo := repository.New(d)
	sourceRepo := repo.Source()
	sourceSchema := schema.ForSource(d.EtcdSerde())

	// Create branch
	branch := test.NewBranch(branchKey)
	require.NoError(t, repo.Branch().Create(clk.Now(), &branch).Do(ctx).Err())

	// Create sources up to maximum count
	// Note: multiple puts are merged to a transaction to improve test speed
	txn := op.Txn(client)
	ops := 0
	for i := 1; i <= repository.MaxSourcesPerBranch; i++ {
		source := test.NewSource(key.SourceKey{BranchKey: branchKey, SourceID: key.SourceID(fmt.Sprintf("my-source-%d", i))})
		source.IncrementVersion(source, clk.Now(), "Create")
		txn.Then(sourceSchema.Active().ByKey(source.SourceKey).Put(client, source))

		// Send the txn it is full, or after the last item
		ops++
		if ops == 100 || i == repository.MaxSourcesPerBranch {
			// Send
			assert.NoError(t, txn.Do(ctx).Err())
			// Reset
			ops = 0
			txn = op.Txn(client)
		}
	}
	sources, err := sourceRepo.List(branchKey).Do(ctx).AllKVs()
	assert.NoError(t, err)
	assert.Len(t, sources, repository.MaxSourcesPerBranch)

	// Exceed the limit
	source := test.NewSource(key.SourceKey{BranchKey: key.BranchKey{ProjectID: projectID, BranchID: 456}, SourceID: "over-maximum-count"})
	if err := sourceRepo.Create(clk.Now(), "Create description", &source).Do(ctx).Err(); assert.Error(t, err) {
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
	d, mock := dependencies.NewMockedServiceScope(t, commonDeps.WithClock(clk))
	client := mock.TestEtcdClient()
	repo := repository.New(d)
	sourceRepo := repo.Source()
	sourceSchema := schema.ForSource(d.EtcdSerde())

	// Create branch
	branch := test.NewBranch(branchKey)
	require.NoError(t, repo.Branch().Create(clk.Now(), &branch).Do(ctx).Err())

	// Create source
	source := test.NewSource(sourceKey)
	require.NoError(t, sourceRepo.Create(clk.Now(), "Create", &source).Do(ctx).Err())

	// Create versions up to maximum count
	// Note: multiple puts are merged to a transaction to improve test speed
	txn := op.Txn(client)
	ops := 0
	for i := source.VersionNumber() + 1; i <= repository.MaxSourceVersionsPerSource; i++ {
		source.Description = fmt.Sprintf("Description %04d", i)
		source.IncrementVersion(source, clk.Now(), "Some Update")
		txn.Then(sourceSchema.Versions().Of(sourceKey).Version(source.VersionNumber()).Put(client, source))

		// Send the txn it is full, or after the last item
		ops++
		if ops == 100 || i == repository.MaxSourceVersionsPerSource {
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
	assert.Len(t, sources, repository.MaxSourceVersionsPerSource)

	// Exceed the limit
	err = sourceRepo.Update(clk.Now(), sourceKey, "Some update", func(v definition.Source) (definition.Source, error) {
		v.Description = "foo"
		return v, nil
	}).Do(ctx).Err()
	if assert.Error(t, err) {
		assert.Equal(t, "version count limit reached in the source, the maximum is 1000", err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
	}
}
