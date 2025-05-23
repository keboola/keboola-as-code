package job_test

import (
	"net/http"
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
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

func TestJobRepository_Get(t *testing.T) {
	t.Parallel()

	by := test.ByUser()
	ctx := t.Context()
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
	jobKey := model.JobKey{SinkKey: sinkKey, JobID: "321"}

	// Get - job does not exist
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.Get(jobKey).Do(ctx).ResultOrErr()
		if assert.Error(t, err) {
			assert.Equal(t, `job "123/567/my-source/my-sink/321" not found in the sink`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
		assert.Equal(t, model.Job{}, result)
	}

	// Create prerequisites and job
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

		etcdhelper.AssertKVsFromFile(t, client, "fixtures/job_get_snapshot_001.txt", ignoredEtcdKeys)
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.Get(jobKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.Job{JobKey: jobKey}, result)
	}
}
