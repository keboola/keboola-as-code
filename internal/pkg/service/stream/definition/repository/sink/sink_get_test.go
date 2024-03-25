package sink_test

import (
	"context"
	"github.com/keboola/go-client/pkg/keboola"
	serviceErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

func TestSinkRepository_Get(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()

	d, _ := dependencies.NewMockedServiceScope(t)
	repo := d.DefinitionRepository().Sink()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}

	// Get - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.Get(sinkKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "my-sink" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - ok
	// -----------------------------------------------------------------------------------------------------------------
	var sink definition.Sink
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, "Create source").Do(ctx).Err())

		sink = test.NewSink(sinkKey)
		require.NoError(t, repo.Create(&sink, now, "Create sink").Do(ctx).Err())
	}

	// Get - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := repo.Get(sinkKey).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			assert.Equal(t, sink, result)
		}
	}
}

func TestSinkRepository_GetDeleted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()

	d, _ := dependencies.NewMockedServiceScope(t)
	repo := d.DefinitionRepository().Sink()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}

	// GetDeleted - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.GetDeleted(sinkKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted sink "my-sink" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - ok
	// -----------------------------------------------------------------------------------------------------------------
	var sink definition.Sink
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, now).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, d.DefinitionRepository().Source().Create(&source, now, "Create source").Do(ctx).Err())

		sink = test.NewSink(sinkKey)
		require.NoError(t, repo.Create(&sink, now, "Create sink").Do(ctx).Err())
	}

	// GetDeleted - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := repo.GetDeleted(sinkKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `deleted sink "my-sink" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// SoftDelete - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, repo.SoftDelete(sinkKey, now).Do(ctx).Err())
	}

	// GetDeleted - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		deletedAt := utctime.From(now)
		sink.Deleted = true
		sink.DeletedAt = &deletedAt
		result, err := repo.GetDeleted(sinkKey).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			assert.Equal(t, sink, result)
		}
	}
}
