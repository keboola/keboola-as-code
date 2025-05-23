package sink_test

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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
)

func TestSinkRepository_ExistsOrErr(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, _ := dependencies.NewMockedServiceScope(t, ctx)
	branchRepo := d.DefinitionRepository().Branch()
	sourceRepo := d.DefinitionRepository().Source()
	sinkRepo := d.DefinitionRepository().Sink()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}

	// ExistsOrErr - branch not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := sinkRepo.ExistsOrErr(sinkKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `branch "567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create branch
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, branchRepo.Create(&branch, now, by).Do(ctx).Err())
	}

	// ExistsOrErr - source not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := sinkRepo.ExistsOrErr(sinkKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source "my-source" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create source
	// -----------------------------------------------------------------------------------------------------------------
	{
		source := test.NewSource(sourceKey)
		require.NoError(t, sourceRepo.Create(&source, now, by, "Create source").Do(ctx).Err())
	}

	// ExistsOrErr - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := sinkRepo.ExistsOrErr(sinkKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "my-sink" not found in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create sink
	// -----------------------------------------------------------------------------------------------------------------
	{
		sink := dummy.NewSink(sinkKey)
		require.NoError(t, sinkRepo.Create(&sink, now, by, "Create sink").Do(ctx).Err())
	}

	// ExistsOrErr - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, sinkRepo.ExistsOrErr(sinkKey).Do(ctx).Err())
	}
}

func TestSinkRepository_MustNotExist(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()

	d, _ := dependencies.NewMockedServiceScope(t, ctx)
	branchRepo := d.DefinitionRepository().Branch()
	sourceRepo := d.DefinitionRepository().Source()
	sinkRepo := d.DefinitionRepository().Sink()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 567}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}

	// MustNotExist - branch not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := sinkRepo.MustNotExist(sinkKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `branch "567" not found in the project`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create branch
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, branchRepo.Create(&branch, now, by).Do(ctx).Err())
	}

	// MustNotExist - source not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := sinkRepo.MustNotExist(sinkKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `source "my-source" not found in the branch`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create source
	// -----------------------------------------------------------------------------------------------------------------
	{
		source := test.NewSource(sourceKey)
		require.NoError(t, sourceRepo.Create(&source, now, by, "Create source").Do(ctx).Err())
	}

	// MustNotExist - sink not found - ok
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, sinkRepo.MustNotExist(sinkKey).Do(ctx).Err())
	}

	// Create sink
	// -----------------------------------------------------------------------------------------------------------------
	{
		sink := dummy.NewSink(sinkKey)
		require.NoError(t, sinkRepo.Create(&sink, now, by, "Create sink").Do(ctx).Err())
	}

	// MustNotExist - error
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := sinkRepo.MustNotExist(sinkKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "my-sink" already exists in the source`, err.Error())
			serviceErrors.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}
}
