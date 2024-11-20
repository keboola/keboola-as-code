package job_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	serviceErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestJobRepository_Exists(t *testing.T) {
	t.Parallel()

	by := test.ByUser()
	ctx := context.Background()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()

	d, mocked := dependencies.NewMockedServiceScope(t, ctx)
	client := mocked.TestEtcdClient()
	repo := d.KeboolaBridgeRepository().Job()
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(definition/branch)|(definition/source)|(definition/sink)")

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	jobKey := key.JobKey{SinkKey: sinkKey, JobID: "321"}

	// Exists and MustNotExists - the job does not exists, MustNotExists returns error
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.ExistsOrErr(jobKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `branch "567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}

		require.Error(t, repo.MustNotExist(jobKey).Do(ctx).Err())
	}

	// Create - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, by, "Create source").Do(ctx).Err())

		sink := dummy.NewSink(sinkKey)
		require.NoError(t, d.DefinitionRepository().Sink().Create(&sink, now, by, "Create sink").Do(ctx).Err())

		job := model.Job{JobKey: jobKey}
		result, err := repo.Create(&job).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, job, result)
		assert.Equal(t, now, sink.VersionModifiedAt().Time())

		etcdhelper.AssertKVsFromFile(t, client, "fixtures/job_exists_snapshot_001.txt", ignoredEtcdKeys)
	}

	// ExistsOrErr - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.ExistsOrErr(jobKey).Do(ctx).Err())
	}

	// Create - already exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		job := model.Job{JobKey: jobKey}
		if err := repo.Create(&job).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `job "321" already exists in the sink`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}

	// Purge - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		job := model.Job{JobKey: jobKey}
		assert.NoError(t, repo.Purge(&job).Do(ctx).Err())
	}

	// MustNotExists - not ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.MustNotExist(jobKey).Do(ctx).Err())
	}
}
