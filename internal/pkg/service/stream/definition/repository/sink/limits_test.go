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
	test2 "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/keboola/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestSinkLimits_SinksPerBranch(t *testing.T) {
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
	// rb := rollback.NewRepository(d.Logger())
	repo := repository.New(d)
	sinkRepo := repo.Sink()
	sinkSchema := schema.ForSink(d.EtcdSerde())

	// Simulate that the operation is running in an API request authorized by a token
	api := d.KeboolaPublicAPI().WithToken(mock.StorageAPIToken().Token)
	ctx = context.WithValue(ctx, dependencies.KeboolaProjectAPICtxKey, api)

	// Create parents
	branch := test.NewBranch(branchKey)
	require.NoError(t, repo.Branch().Create(&branch, clk.Now()).Do(ctx).Err())
	source := test.NewSource(sourceKey)
	require.NoError(t, repo.Source().Create(&source, clk.Now(), "Create").Do(ctx).Err())

	// Create sinks up to maximum count
	// Note: multiple puts are merged to a transaction to improve test speed
	txn := op.Txn(client)
	ops := 0
	for i := 1; i <= sinkrepo.MaxSinksPerSource; i++ {
		sink := test.NewSink(key.SinkKey{SourceKey: sourceKey, SinkID: key.SinkID(fmt.Sprintf("my-sink-%d", i))})
		sink.IncrementVersion(sink, clk.Now(), "Create")
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
	sink := test.NewSink(key.SinkKey{SourceKey: sourceKey, SinkID: "over-maximum-count"})
	if err := sinkRepo.Create(&sink, clk.Now(), "Create description").Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, "sink count limit reached in the source, the maximum is 100", err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
	}
}

func TestSinkLimits_VersionsPerSink(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2006-01-02T15:04:05.123Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source-1"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}

	// Get services
	d, mock := dependencies.NewMockedServiceScope(t, commonDeps.WithClock(clk))
	client := mock.TestEtcdClient()
	// rb := rollback.NewRepository(d.Logger())
	repo := repository.New(d)
	sinkRepo := repo.Sink()
	sinkSchema := schema.ForSink(d.EtcdSerde())

	// Simulate that the operation is running in an API request authorized by a token
	api := d.KeboolaPublicAPI().WithToken(mock.StorageAPIToken().Token)
	ctx = context.WithValue(ctx, dependencies.KeboolaProjectAPICtxKey, api)

	// Mock file API calls
	transport := mock.MockedHTTPTransport()
	test2.MockBucketStorageAPICalls(t, transport)
	test2.MockTableStorageAPICalls(t, transport)
	test2.MockTokenStorageAPICalls(t, transport)

	// Create parents
	branch := test.NewBranch(branchKey)
	require.NoError(t, repo.Branch().Create(&branch, clk.Now()).Do(ctx).Err())
	source := test.NewSource(sourceKey)
	require.NoError(t, repo.Source().Create(&source, clk.Now(), "Create").Do(ctx).Err())

	// Create sink
	sink := test.NewSink(sinkKey)
	require.NoError(t, sinkRepo.Create(&sink, clk.Now(), "create").Do(ctx).Err())

	// Create versions up to maximum count
	// Note: multiple puts are merged to a transaction to improve test speed
	txn := op.Txn(client)
	ops := 0
	for i := sink.VersionNumber() + 1; i <= sourcerepo.MaxSourceVersionsPerSource; i++ {
		sink.Description = fmt.Sprintf("Description %04d", i)
		sink.IncrementVersion(sink, clk.Now(), "Some Update")
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
	sinks, err := sinkRepo.Versions(sinkKey).Do(ctx).AllKVs()
	assert.NoError(t, err)
	assert.Len(t, sinks, sourcerepo.MaxSourceVersionsPerSource)

	// Exceed the limit
	err = sinkRepo.Update(sinkKey, clk.Now(), "Some update", func(v definition.Sink) (definition.Sink, error) {
		v.Description = "foo"
		return v, nil
	}).Do(ctx).Err()
	if assert.Error(t, err) {
		assert.Equal(t, "version count limit reached in the sink, the maximum is 1000", err.Error())
		serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
	}
}
