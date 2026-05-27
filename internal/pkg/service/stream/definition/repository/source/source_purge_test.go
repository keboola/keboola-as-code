package source_test

import (
	"testing"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestSourceRepository_Purge(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, mocked := dependencies.NewMockedServiceScope(t, ctx)
	client := mocked.TestEtcdClient()
	repo := d.DefinitionRepository().Source()
	// Only branch keys may remain after the source is purged.
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(definition/branch)")

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}

	// Purge - not found (source exists in neither the active nor the deleted prefix).
	{
		err := repo.Purge(sourceKey).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `source "my-source" not found in the branch`, err.Error())
		}
	}

	// Create branch and a source with two versions (so version history is non-trivial).
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now, by).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, repo.Create(&source, now, by, "Create source").Do(ctx).Err())

		require.NoError(t, repo.Update(sourceKey, now, by, "Rename", func(s definition.Source) (definition.Source, error) {
			s.Name = "Renamed"
			return s, nil
		}).Do(ctx).Err())
	}

	// SoftDelete moves the source to the deleted prefix (Purge runs after a soft delete in production).
	{
		now = now.Add(time.Hour)
		require.NoError(t, repo.SoftDelete(sourceKey, now, by).Do(ctx).Err())
		require.NoError(t, repo.GetDeleted(sourceKey).Do(ctx).Err())
	}

	// Purge - removes active, deleted and version history entirely.
	{
		now = now.Add(time.Hour)
		purged, err := repo.Purge(sourceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, sourceKey, purged.SourceKey)

		// Nothing related to the source remains in etcd.
		etcdhelper.AssertKVsString(t, client, "", ignoredEtcdKeys)

		// The source is gone from both the active and deleted prefixes.
		require.Error(t, repo.Get(sourceKey).Do(ctx).Err())
		require.Error(t, repo.GetDeleted(sourceKey).Do(ctx).Err())
	}

	// Recreate with the same key yields a fresh source (version reset to 1), not a revived one.
	{
		now = now.Add(time.Hour)
		source := test.NewSource(sourceKey)
		require.NoError(t, repo.Create(&source, now, by, "Recreate source").Do(ctx).Err())

		recreated, err := repo.Get(sourceKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, definition.VersionNumber(1), recreated.VersionNumber())
	}
}
