package sink_test

import (
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
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestSinkRepository_Versions(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, mocked := dependencies.NewMockedServiceScope(t, ctx)
	client := mocked.TestEtcdClient()
	repo := d.DefinitionRepository().Sink()
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(definition/branch)|(definition/source)")

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}

	var err error

	// ListVersions - empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.ListVersions(sinkKey).Do(ctx).All()
		require.NoError(t, err)
		assert.Empty(t, result)
	}

	// Version - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.Version(sinkKey, definition.VersionNumber(1)).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink version "my-sink/0000000001" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// RollbackVersion - entity not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.RollbackVersion(sinkKey, now, by, definition.VersionNumber(1)).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "my-sink" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - ok
	// -----------------------------------------------------------------------------------------------------------------
	var version1 definition.Sink
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, by, "Create source").Do(ctx).Err())

		version1 = dummy.NewSink(sinkKey)
		require.NoError(t, repo.Create(&version1, now, by, "Create sink").Do(ctx).Err())
		assert.Equal(t, definition.VersionNumber(1), version1.Version.Number)
	}

	// Update - ok
	// -----------------------------------------------------------------------------------------------------------------
	var version2, version3 definition.Sink
	{
		updateFn1 := func(sink definition.Sink) (definition.Sink, error) {
			sink.Description = "Updated description 1"
			return sink, nil
		}
		updateFn2 := func(sink definition.Sink) (definition.Sink, error) {
			sink.Description = "Updated description 2"
			return sink, nil
		}

		now = now.Add(time.Hour)
		version2, err = repo.Update(sinkKey, now, by, "Update sink 1", updateFn1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, definition.VersionNumber(2), version2.Version.Number)

		now = now.Add(time.Hour)
		version3, err = repo.Update(sinkKey, now, by, "Update sink 2", updateFn2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, definition.VersionNumber(3), version3.Version.Number)
	}

	// ListVersions - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.ListVersions(sinkKey).Do(ctx).All()
		require.NoError(t, err)
		assert.Len(t, result, 3)
		assert.Equal(t, []definition.Sink{version1, version2, version3}, result)
	}

	// Version - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.Version(sinkKey, version2.VersionNumber()).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, version2, result)
	}

	// RollbackVersion - ok
	// -----------------------------------------------------------------------------------------------------------------
	var version4 definition.Sink
	{
		now = now.Add(time.Hour)
		version4, err = repo.RollbackVersion(sinkKey, now, by, version2.VersionNumber()).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, definition.VersionNumber(4), version4.Version.Number)
	}

	// RollbackVersion - version not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.RollbackVersion(sinkKey, now, by, definition.VersionNumber(123)).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink version "my-sink/0000000123" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Versions list - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.ListVersions(sinkKey).Do(ctx).All()
		require.NoError(t, err)
		assert.Len(t, result, 4)
		assert.Equal(t, []definition.Sink{version1, version2, version3, version4}, result)
	}

	// Check database state
	// -----------------------------------------------------------------------------------------------------------------
	{
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/sink_version_snapshot_001.txt", ignoredEtcdKeys)
	}
}
