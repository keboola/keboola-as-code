package sink_test

import (
	"testing"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestSinkRepository_PurgeAllFrom(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, mocked := dependencies.NewMockedServiceScope(t, ctx)
	client := mocked.TestEtcdClient()
	repo := d.DefinitionRepository().Sink()
	// Only branch and source keys may remain after the sinks are purged.
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(definition/branch)|(definition/source)")

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey1 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-2"}

	// Create branch, source and two sinks.
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, by, "Create source").Do(ctx).Err())

		sink1 := dummy.NewSink(sinkKey1)
		require.NoError(t, repo.Create(&sink1, now, by, "Create sink").Do(ctx).Err())
		sink2 := dummy.NewSink(sinkKey2)
		require.NoError(t, repo.Create(&sink2, now, by, "Create sink").Do(ctx).Err())
	}

	// SoftDelete the source - cascades the sinks to the deleted prefix.
	{
		now = now.Add(time.Hour)
		require.NoError(t, d.DefinitionRepository().Source().SoftDelete(sourceKey, now, by).Do(ctx).Err())
		require.NoError(t, repo.GetDeleted(sinkKey1).Do(ctx).Err())
		require.NoError(t, repo.GetDeleted(sinkKey2).Do(ctx).Err())
	}

	// PurgeAllFrom - removes all sinks of the source (active, deleted and version history).
	{
		purged, err := repo.PurgeAllFrom(sourceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Len(t, purged, 2)

		// No sink-related keys remain in etcd.
		etcdhelper.AssertKVsString(t, client, "", ignoredEtcdKeys)

		require.Error(t, repo.Get(sinkKey1).Do(ctx).Err())
		require.Error(t, repo.GetDeleted(sinkKey1).Do(ctx).Err())
		require.Error(t, repo.Get(sinkKey2).Do(ctx).Err())
		require.Error(t, repo.GetDeleted(sinkKey2).Do(ctx).Err())
	}
}
