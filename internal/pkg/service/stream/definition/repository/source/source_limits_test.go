package source_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/jonboulle/clockwork"
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
	sourcerepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/source"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/source/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestSourceRepository_Limits_SourcesPerBranch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	by := test.ByUser()

	clk := clockwork.NewFakeClockAt(utctime.MustParse("2006-01-02T15:04:05.123Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}

	// Get services
	d, mock := dependencies.NewMockedServiceScope(t, ctx, commonDeps.WithClock(clk))
	client := mock.TestEtcdClient()
	repo := repository.New(d)
	sourceRepo := repo.Source()
	sourceSchema := schema.New(d.EtcdSerde())

	// Create branch
	branch := test.NewBranch(branchKey)
	require.NoError(t, repo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())

	// Create sources up to maximum count
	// Note: multiple puts are merged to a transaction to improve test speed
	txn := op.Txn(client)
	ops := 0
	for i := 1; i <= sourcerepo.MaxSourcesPerBranch; i++ {
		source := test.NewSource(key.SourceKey{BranchKey: branchKey, SourceID: key.SourceID(fmt.Sprintf("my-sourcerepo-%d", i))})
		source.SetCreation(clk.Now(), by)
		source.IncrementVersion(source, clk.Now(), by, "Create")
		txn.Then(sourceSchema.Active().ByKey(source.SourceKey).Put(client, source))

		// Send the txn it is full, or after the last item
		ops++
		if ops == 100 || i == sourcerepo.MaxSourcesPerBranch {
			// Send
			require.NoError(t, txn.Do(ctx).Err())
			// Reset
			ops = 0
			txn = op.Txn(client)
		}
	}
	sources, err := sourceRepo.List(branchKey).Do(ctx).AllKVs()
	require.NoError(t, err)
	assert.Len(t, sources, sourcerepo.MaxSourcesPerBranch)

	// Exceed the limit
	source := test.NewSource(key.SourceKey{BranchKey: key.BranchKey{ProjectID: projectID, BranchID: 456}, SourceID: "over-maximum-count"})
	if err := sourceRepo.Create(&source, clk.Now(), by, "Create description").Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, "source count limit reached in the branch, the maximum is 100", err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
	}
}

func TestSourceLimits_VersionsPerSource(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	by := test.ByUser()

	clk := clockwork.NewFakeClockAt(utctime.MustParse("2006-01-02T15:04:05.123Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-sourcerepo-1"}

	// Get services
	d, mock := dependencies.NewMockedServiceScope(t, ctx, commonDeps.WithClock(clk))
	client := mock.TestEtcdClient()
	repo := repository.New(d)
	sourceRepo := repo.Source()
	sourceSchema := schema.New(d.EtcdSerde())

	// Create branch
	branch := test.NewBranch(branchKey)
	require.NoError(t, repo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())

	// Create sourcerepo
	source := test.NewSource(sourceKey)
	require.NoError(t, sourceRepo.Create(&source, clk.Now(), by, "Create").Do(ctx).Err())

	// Create versions up to maximum count
	// Note: multiple puts are merged to a transaction to improve test speed
	txn := op.Txn(client)
	ops := 0
	for i := source.VersionNumber() + 1; i <= sourcerepo.MaxSourceVersionsPerSource; i++ {
		source.Description = fmt.Sprintf("Description %04d", i)
		source.IncrementVersion(source, clk.Now(), by, "Some Update")
		txn.Then(sourceSchema.Versions().Of(sourceKey).Version(source.VersionNumber()).Put(client, source))

		// Send the txn it is full, or after the last item
		ops++
		if ops == 100 || i == sourcerepo.MaxSourceVersionsPerSource {
			// Send
			require.NoError(t, txn.Do(ctx).Err())
			// Reset
			ops = 0
			txn = op.Txn(client)
		}
	}
	// Check that the maximum count is reached
	sources, err := sourceRepo.ListVersions(sourceKey).Do(ctx).AllKVs()
	require.NoError(t, err)
	assert.Len(t, sources, sourcerepo.MaxSourceVersionsPerSource)

	// Exceed the limit
	err = sourceRepo.Update(sourceKey, clk.Now(), by, "Some update", func(v definition.Source) (definition.Source, error) {
		v.Description = "foo"
		return v, nil
	}).Do(ctx).Err()
	if assert.Error(t, err) {
		assert.Equal(t, "version count limit reached in the source, the maximum is 1000", err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
	}
}
