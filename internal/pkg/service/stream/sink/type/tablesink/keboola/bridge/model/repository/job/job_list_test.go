package job_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestJobRepository_List(t *testing.T) {
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
	jobKey1 := key.JobKey{SinkKey: sinkKey, JobID: "321"}
	jobKey2 := key.JobKey{SinkKey: sinkKey, JobID: "322"}

	// List - empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.List(projectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, result)
	}

	// Create two jobs - ok
	// -----------------------------------------------------------------------------------------------------------------
	var job1, job2 model.Job
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, by, "Create source").Do(ctx).Err())

		sink := dummy.NewSink(sinkKey)
		require.NoError(t, d.DefinitionRepository().Sink().Create(&sink, now, by, "Create sink").Do(ctx).Err())

		job1 = model.Job{JobKey: jobKey1}
		result, err := repo.Create(&job1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, job1, result)

		job2 = model.Job{JobKey: jobKey2}
		result, err = repo.Create(&job2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, job2, result)

		etcdhelper.AssertKVsFromFile(t, client, "fixtures/job_list_snapshot_001.txt", ignoredEtcdKeys)
	}

	// List - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.List(projectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, []model.Job{job1, job2}, result)
	}

	// List all - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.ListAll().Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, []model.Job{job1, job2}, result)
	}
}

func TestJobRepository_ListDeleted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, mocked := dependencies.NewMockedServiceScope(t, ctx)
	client := mocked.TestEtcdClient()
	repo := d.KeboolaBridgeRepository().Job()
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(definition/branch)|(definition/source)|(definition/sink)")

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey1 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-2"}
	jobKey1 := key.JobKey{SinkKey: sinkKey1, JobID: "321"}
	jobKey2 := key.JobKey{SinkKey: sinkKey2, JobID: "322"}

	// Create two sinks
	// -----------------------------------------------------------------------------------------------------------------
	var sink1, sink2 definition.Sink
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, by, "Create source").Do(ctx).Err())

		sink1 = dummy.NewSink(sinkKey1)
		require.NoError(t, d.DefinitionRepository().Sink().Create(&sink1, now, by, "Create sink").Do(ctx).Err())

		sink2 = dummy.NewSink(sinkKey2)
		require.NoError(t, d.DefinitionRepository().Sink().Create(&sink2, now, by, "Create sink").Do(ctx).Err())
	}

	// Create two jobs - ok
	// -----------------------------------------------------------------------------------------------------------------
	var job1, job2 model.Job
	{
		job1 = model.Job{JobKey: jobKey1}
		result, err := repo.Create(&job1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, job1, result)

		job2 = model.Job{JobKey: jobKey2}
		result, err = repo.Create(&job2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, job2, result)

		etcdhelper.AssertKVsFromFile(t, client, "fixtures/job_list_snapshot_002.txt", ignoredEtcdKeys)
	}

	// SoftDelete two sinks - ok, removes jobs
	// -----------------------------------------------------------------------------------------------------------------
	{
		var err error

		sink1, err = d.DefinitionRepository().Sink().SoftDelete(sinkKey1, now, by).Do(ctx).ResultOrErr()
		assert.NoError(t, err)

		sink2, err = d.DefinitionRepository().Sink().SoftDelete(sinkKey2, now, by).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
	}

	// List - empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.List(sourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, result, 0)
	}
}
