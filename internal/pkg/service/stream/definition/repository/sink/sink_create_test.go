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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestSinkRepository_Create(t *testing.T) {
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

	// Create - parent not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		sink := test.NewSink(sinkKey)
		if err := repo.Create(&sink, now, by, "Create sink").Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `branch "567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, by, "Create source").Do(ctx).Err())

		sink := test.NewSink(sinkKey)
		result, err := repo.Create(&sink, now, by, "Create sink").Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, sink, result)
		assert.Equal(t, now, sink.VersionModifiedAt().Time())

		etcdhelper.AssertKVsFromFile(t, client, "fixtures/sink_create_snapshot_001.txt", ignoredEtcdKeys)
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.Get(sinkKey).Do(ctx).Err())
	}

	// Create - already exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		sink := test.NewSink(sinkKey)
		if err := repo.Create(&sink, now, by, "Create sink").Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "my-sink" already exists in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}

	// SoftDelete - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, repo.SoftDelete(sinkKey, now, by).Do(ctx).Err())
	}

	// Create - ok, undeleted
	// -----------------------------------------------------------------------------------------------------------------
	{
		sink := test.NewSink(sinkKey)
		result, err := repo.Create(&sink, now.Add(time.Hour), by, "Create sink").Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, sink, result)
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/sink_create_snapshot_002.txt", ignoredEtcdKeys)
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.Get(sinkKey).Do(ctx).Err())
	}
}
