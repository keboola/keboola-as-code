package branchmetadata_test

import (
	"context"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/branchmetadata"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestConfigMetadataMapper_MapBeforeLocalSave(t *testing.T) {
	t.Parallel()
	d, _ := dependencies.NewMocked(t, context.Background())
	logger := d.DebugLogger()
	mockedState := d.MockedState()
	mockedState.Mapper().AddMapper(branchmetadata.NewMapper(mockedState, d))

	branchKey := model.BranchKey{ID: 123}
	state := &model.BranchState{
		BranchManifest: &model.BranchManifest{
			BranchKey: branchKey,
		},
		Local: &model.Branch{
			BranchKey: branchKey,
			Name:      "My Branch",
			Metadata:  map[string]string{"KBC.KaC.Meta1": "val1", "KBC.KaC.Meta2": "val2"},
		},
	}

	recipe := model.NewLocalSaveRecipe(state.Manifest(), state.Local, model.NewChangedFields())
	assert.NoError(t, mockedState.Mapper().MapBeforeLocalSave(context.Background(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	branchManifest := recipe.ObjectManifest.(*model.BranchManifest)
	assert.Equal(t, orderedmap.FromPairs([]orderedmap.Pair{
		{Key: "KBC.KaC.Meta1", Value: "val1"},
		{Key: "KBC.KaC.Meta2", Value: "val2"},
	}), branchManifest.Metadata)
}
