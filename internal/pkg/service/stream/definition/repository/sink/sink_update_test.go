package sink_test

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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestSinkRepository_Update(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, mocked := dependencies.NewMockedServiceScope(t)
	client := mocked.TestEtcdClient()
	repo := d.DefinitionRepository().Sink()
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(definition/branch)|(definition/source)")

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}

	// Update - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		updateFn := func(sink definition.Sink) (definition.Sink, error) {
			return sink, nil
		}
		if err := repo.Update(sinkKey, now, by, "Update sink", updateFn).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "my-sink" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - ok
	// -----------------------------------------------------------------------------------------------------------------
	var sink definition.Sink
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, by, "Create source").Do(ctx).Err())

		sink = dummy.NewSink(sinkKey)
		require.NoError(t, repo.Create(&sink, now, by, "Create sink").Do(ctx).Err())
	}

	// Update - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)

		updateFn := func(sink definition.Sink) (definition.Sink, error) {
			sink.Description = "Updated description"
			return sink, nil
		}

		var err error
		sink, err = repo.Update(sinkKey, now, by, "Update sink", updateFn).Do(ctx).ResultOrErr()
		require.NoError(t, err)

		etcdhelper.AssertKVsFromFile(t, client, "fixtures/sink_update_snapshot_001.txt", ignoredEtcdKeys)
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.Get(sinkKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, sink, result)
	}

	// Update - error from the update function
	// -----------------------------------------------------------------------------------------------------------------
	{
		updateFn := func(sink definition.Sink) (definition.Sink, error) {
			return definition.Sink{}, errors.New("some error")
		}

		err := repo.Update(sinkKey, now, by, "Update sink", updateFn).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, "some error", err.Error())
		}
	}

	// Update - "Disabled" field cannot be modified by the Update operation
	// -----------------------------------------------------------------------------------------------------------------
	{
		updateFn := func(sink definition.Sink) (definition.Sink, error) {
			sink.Disable(now, test.ByUser(), "some reason", true)
			return sink, nil
		}
		err := repo.Update(sinkKey, now, by, "Update sink", updateFn).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `"Disabled" field cannot be modified by the Update operation`, err.Error())
		}
	}

	// Update - "Deleted" field cannot be modified by the Update operation
	// -----------------------------------------------------------------------------------------------------------------
	{
		updateFn := func(sink definition.Sink) (definition.Sink, error) {
			sink.Delete(now, test.ByUser(), true)
			return sink, nil
		}
		err := repo.Update(sinkKey, now, by, "Update sink", updateFn).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `"Deleted" field cannot be modified by the Update operation`, err.Error())
		}
	}
}
