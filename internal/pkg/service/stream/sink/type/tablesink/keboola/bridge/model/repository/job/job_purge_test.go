package job_test

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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestJobRepository_Purge(t *testing.T) {
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
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	jobKey := key.JobKey{SinkKey: sinkKey, JobID: "321"}

	// Purge - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		job := model.Job{JobKey: jobKey}
		if err := repo.Purge(&job).Do(ctx).Err(); assert.Error(t, err) {
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

		sink := dummy.NewSink(sinkKey)
		require.NoError(t, d.DefinitionRepository().Sink().Create(&sink, now, by, "Create sink").Do(ctx).Err())

		job := model.Job{JobKey: jobKey, Token: "secret"}
		require.NoError(t, repo.Create(&job).Do(ctx).Err())
	}

	// ExistsOrErr - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.ExistsOrErr(jobKey).Do(ctx).Err())
	}

	// Purge - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		job := model.Job{JobKey: jobKey}
		assert.NoError(t, repo.Purge(&job).Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/job_purge_snapshot_001.txt", ignoredEtcdKeys)
	}

	// ExistsOrErr - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.ExistsOrErr(jobKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `job "321" not found in the sink`, err.Error())
		}
	}
}

func TestJobRepository_PurgeJobsOnSinkDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, mocked := dependencies.NewMockedServiceScope(t, ctx)
	client := mocked.TestEtcdClient()
	repo := d.KeboolaBridgeRepository().Job()
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(definition/source/version)|(definition/sink/version)")

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	jobKey1 := key.JobKey{SinkKey: sinkKey, JobID: "321"}
	jobKey2 := key.JobKey{SinkKey: sinkKey, JobID: "322"}
	jobKey3 := key.JobKey{SinkKey: sinkKey, JobID: "323"}

	// Create Branch, Source and Sink
	// -----------------------------------------------------------------------------------------------------------------
	var sink definition.Sink
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, by, "Create source").Do(ctx).Err())

		sink = dummy.NewSink(sinkKey)
		require.NoError(t, d.DefinitionRepository().Sink().Create(&sink, now, by, "Create sink").Do(ctx).Err())
	}

	// Create three Jobs
	// -----------------------------------------------------------------------------------------------------------------
	{
		job := model.Job{JobKey: jobKey1, Token: "secret1"}
		require.NoError(t, repo.Create(&job).Do(ctx).Err())

		job = model.Job{JobKey: jobKey2, Token: "secret2"}
		require.NoError(t, repo.Create(&job).Do(ctx).Err())

		job = model.Job{JobKey: jobKey3, Token: "secret3"}
		require.NoError(t, repo.Create(&job).Do(ctx).Err())
	}

	// Delete Sink
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		var err error
		sink, err = d.DefinitionRepository().Sink().SoftDelete(sinkKey, now, by).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink.IsDeleted())
		assert.Equal(t, now, sink.DeletedAt().Time())
	}

	// Check keys
	// -----------------------------------------------------------------------------------------------------------------
	{
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/job_purge_snapshot_002.txt", ignoredEtcdKeys)
	}
}

func TestJobRepository_PurgeJobsOnSourceDelete_DeleteSource(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, mocked := dependencies.NewMockedServiceScope(t, ctx)
	client := mocked.TestEtcdClient()
	repo := d.KeboolaBridgeRepository().Job()
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(definition/source/version)|(definition/sink/version)")

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey1 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-2"}
	sinkKey3 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-3"}
	jobKey0 := key.JobKey{SinkKey: sinkKey1, JobID: "320"}
	jobKey1 := key.JobKey{SinkKey: sinkKey2, JobID: "321"}
	jobKey2 := key.JobKey{SinkKey: sinkKey2, JobID: "322"}
	jobKey3 := key.JobKey{SinkKey: sinkKey3, JobID: "323"}

	// Create Branch and Source
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, by, "Create source").Do(ctx).Err())
	}

	// Create sinks
	// -----------------------------------------------------------------------------------------------------------------
	var sink1, sink2, sink3 definition.Sink
	{
		sink1 = dummy.NewSink(sinkKey1)
		require.NoError(t, d.DefinitionRepository().Sink().Create(&sink1, now, by, "Create sink").Do(ctx).Err())
		sink2 = dummy.NewSink(sinkKey2)
		require.NoError(t, d.DefinitionRepository().Sink().Create(&sink2, now, by, "Create sink").Do(ctx).Err())
		sink3 = dummy.NewSink(sinkKey3)
		require.NoError(t, d.DefinitionRepository().Sink().Create(&sink3, now, by, "Create sink").Do(ctx).Err())
	}

	// Delete Sink1
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		var err error
		sink1, err = d.DefinitionRepository().Sink().SoftDelete(sinkKey1, now, by).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink1.IsDeleted())
		assert.Equal(t, now, sink1.DeletedAt().Time())
	}

	// Create three out of four Jobs
	// -----------------------------------------------------------------------------------------------------------------
	var job0, job1, job2, job3 model.Job
	{
		// This should not be able to create as sink1 is deleted
		job0 = model.Job{JobKey: jobKey0}
		if err := repo.Create(&job0).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "my-sink-1" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}

		job1 = model.Job{JobKey: jobKey1, Token: "secret1"}
		require.NoError(t, repo.Create(&job1).Do(ctx).Err())

		job2 = model.Job{JobKey: jobKey2, Token: "secret2"}
		require.NoError(t, repo.Create(&job2).Do(ctx).Err())

		job3 = model.Job{JobKey: jobKey3, Token: "secret3"}
		require.NoError(t, repo.Create(&job3).Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/job_purge_snapshot_003.txt", ignoredEtcdKeys)
	}

	// Delete Source
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		require.NoError(t, d.DefinitionRepository().Source().SoftDelete(sourceKey, now, by).Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/job_purge_snapshot_004.txt", ignoredEtcdKeys)
	}
	{
		if _, err := repo.MustNotExist(jobKey0).Do(ctx).ResultOrErr(); assert.Error(t, err) {
			assert.Equal(t, `source "my-source" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}

		var err error
		_, err = repo.MustNotExist(jobKey1).Do(ctx).ResultOrErr()
		require.Error(t, err)
		_, err = repo.MustNotExist(jobKey2).Do(ctx).ResultOrErr()
		require.Error(t, err)
		_, err = repo.MustNotExist(jobKey3).Do(ctx).ResultOrErr()
		require.Error(t, err)
	}
}

func TestSinkRepository_DeleteSinksOnSourceDelete_DeleteBranch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, mocked := dependencies.NewMockedServiceScope(t, ctx)
	client := mocked.TestEtcdClient()
	repo := d.KeboolaBridgeRepository().Job()
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(definition/source/version)|(definition/sink/version)")

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey1 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-2"}
	sinkKey3 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-3"}
	jobKey0 := key.JobKey{SinkKey: sinkKey1, JobID: "320"}
	jobKey1 := key.JobKey{SinkKey: sinkKey2, JobID: "321"}
	jobKey2 := key.JobKey{SinkKey: sinkKey2, JobID: "322"}
	jobKey3 := key.JobKey{SinkKey: sinkKey3, JobID: "323"}

	// Create Branch and Source
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, by, "Create source").Do(ctx).Err())
	}

	// Create sinks
	// -----------------------------------------------------------------------------------------------------------------
	var sink1, sink2, sink3 definition.Sink
	{
		sink1 = dummy.NewSink(sinkKey1)
		require.NoError(t, d.DefinitionRepository().Sink().Create(&sink1, now, by, "Create sink").Do(ctx).Err())
		sink2 = dummy.NewSink(sinkKey2)
		require.NoError(t, d.DefinitionRepository().Sink().Create(&sink2, now, by, "Create sink").Do(ctx).Err())
		sink3 = dummy.NewSink(sinkKey3)
		require.NoError(t, d.DefinitionRepository().Sink().Create(&sink3, now, by, "Create sink").Do(ctx).Err())
	}

	// Delete Sink1
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		var err error
		sink1, err = d.DefinitionRepository().Sink().SoftDelete(sinkKey1, now, by).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, sink1.IsDeleted())
		assert.Equal(t, now, sink1.DeletedAt().Time())
	}

	// Create three out of four Jobs
	// -----------------------------------------------------------------------------------------------------------------
	var job0, job1, job2, job3 model.Job
	{
		// This should not be able to create as sink1 is deleted
		job0 = model.Job{JobKey: jobKey0}
		if err := repo.Create(&job0).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "my-sink-1" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}

		job1 = model.Job{JobKey: jobKey1, Token: "secret1"}
		require.NoError(t, repo.Create(&job1).Do(ctx).Err())

		job2 = model.Job{JobKey: jobKey2, Token: "secret2"}
		require.NoError(t, repo.Create(&job2).Do(ctx).Err())

		job3 = model.Job{JobKey: jobKey3, Token: "secret3"}
		require.NoError(t, repo.Create(&job3).Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/job_purge_snapshot_005.txt", ignoredEtcdKeys)
	}

	// Delete Branch
	// -----------------------------------------------------------------------------------------------------------------
	{
		now = now.Add(time.Hour)
		require.NoError(t, d.DefinitionRepository().Branch().SoftDelete(branchKey, now, by).Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/job_purge_snapshot_006.txt", ignoredEtcdKeys)
	}
	{
		if _, err := repo.MustNotExist(jobKey0).Do(ctx).ResultOrErr(); assert.Error(t, err) {
			assert.Equal(t, `branch "567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}

		var err error
		_, err = repo.MustNotExist(jobKey1).Do(ctx).ResultOrErr()
		require.Error(t, err)
		_, err = repo.MustNotExist(jobKey2).Do(ctx).ResultOrErr()
		require.Error(t, err)
		_, err = repo.MustNotExist(jobKey3).Do(ctx).ResultOrErr()
		require.Error(t, err)
	}
}
