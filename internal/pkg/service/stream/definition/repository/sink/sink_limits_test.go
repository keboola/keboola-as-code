package sink_test

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
	sinkrepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/sink"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/sink/schema"
	sourcerepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/source"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
)

func TestSinkLimits_SinksPerBranch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	by := test.ByUser()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2006-01-02T15:04:05.123Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source-1"}

	// Get services
	d, mock := dependencies.NewMockedServiceScope(t, ctx, commonDeps.WithClock(clk))
	client := mock.TestEtcdClient()
	repo := repository.New(d)
	sinkRepo := repo.Sink()
	sinkSchema := schema.New(d.EtcdSerde())

	// Create parents
	branch := test.NewBranch(branchKey)
	require.NoError(t, repo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
	source := test.NewSource(sourceKey)
	require.NoError(t, repo.Source().Create(&source, clk.Now(), by, "Create").Do(ctx).Err())

	// Create sinks up to maximum count
	// Note: multiple puts are merged to a transaction to improve test speed
	txn := op.Txn(client)
	ops := 0
	for i := 1; i <= sinkrepo.MaxSinksPerSource; i++ {
		sink := dummy.NewSink(key.SinkKey{SourceKey: sourceKey, SinkID: key.SinkID(fmt.Sprintf("my-sink-%d", i))})
		sink.SetCreation(clk.Now(), by)
		sink.IncrementVersion(sink, clk.Now(), by, "Create")
		txn.Then(sinkSchema.Active().ByKey(sink.SinkKey).Put(client, sink))

		// Send the txn it is full, or after the last item
		ops++
		if ops == 100 || i == sinkrepo.MaxSinksPerSource {
			// Send
			assert.NoError(t, txn.Do(ctx).Err())
			// Reset
			ops = 0
			txn = op.Txn(client)
		}
	}
	sinks, err := sinkRepo.List(sourceKey).Do(ctx).AllKVs()
	assert.NoError(t, err)
	assert.Len(t, sinks, sinkrepo.MaxSinksPerSource)

	// Exceed the limit
	sink := dummy.NewSink(key.SinkKey{SourceKey: sourceKey, SinkID: "over-maximum-count"})
	if err := sinkRepo.Create(&sink, clk.Now(), by, "Create description").Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, "sink count limit reached in the source, the maximum is 100", err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
	}
}

func TestSinkLimits_VersionsPerSink(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	by := test.ByUser()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2006-01-02T15:04:05.123Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source-1"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}

	// Get services
	d, mock := dependencies.NewMockedServiceScope(t, ctx, commonDeps.WithClock(clk))
	client := mock.TestEtcdClient()
	repo := repository.New(d)
	sinkRepo := repo.Sink()
	sinkSchema := schema.New(d.EtcdSerde())

	// Create parents
	branch := test.NewBranch(branchKey)
	require.NoError(t, repo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
	source := test.NewSource(sourceKey)
	require.NoError(t, repo.Source().Create(&source, clk.Now(), by, "Create").Do(ctx).Err())

	// Create sink
	sink := dummy.NewSink(sinkKey)
	require.NoError(t, sinkRepo.Create(&sink, clk.Now(), by, "create").Do(ctx).Err())

	// Create versions up to maximum count
	// Note: multiple puts are merged to a transaction to improve test speed
	txn := op.Txn(client)
	ops := 0
	for i := sink.VersionNumber() + 1; i <= sourcerepo.MaxSourceVersionsPerSource; i++ {
		sink.Description = fmt.Sprintf("Description %04d", i)
		sink.IncrementVersion(sink, clk.Now(), by, "Some Update")
		txn.Then(sinkSchema.Versions().Of(sinkKey).Version(sink.VersionNumber()).Put(client, sink))

		// Send the txn it is full, or after the last item
		ops++
		if ops == 100 || i == sourcerepo.MaxSourceVersionsPerSource {
			// Send
			assert.NoError(t, txn.Do(ctx).Err())
			// Reset
			ops = 0
			txn = op.Txn(client)
		}
	}
	// Check that the maximum count is reached
	sinks, err := sinkRepo.ListVersions(sinkKey).Do(ctx).AllKVs()
	assert.NoError(t, err)
	assert.Len(t, sinks, sourcerepo.MaxSourceVersionsPerSource)

	// Exceed the limit
	err = sinkRepo.Update(sinkKey, clk.Now(), by, "Some update", func(v definition.Sink) (definition.Sink, error) {
		v.Description = "foo"
		return v, nil
	}).Do(ctx).Err()
	if assert.Error(t, err) {
		assert.Equal(t, "version count limit reached in the sink, the maximum is 1000", err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
	}
}
