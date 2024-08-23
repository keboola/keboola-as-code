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

func TestConfigMetadataMapper_MapAfterLocalLoad(t *testing.T) {
	t.Parallel()
	d := dependencies.NewMocked(t, context.Background())
	logger := d.DebugLogger()
	mockedState := d.MockedState()
	mockedState.Mapper().AddMapper(branchmetadata.NewMapper(mockedState, d))

	branchKey := model.BranchKey{ID: 123}
	state := &model.BranchState{
		BranchManifest: &model.BranchManifest{
			BranchKey: branchKey,
			Metadata: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "KBC.KaC.Meta1", Value: "val1"},
				{Key: "KBC.KaC.Meta2", Value: "val2"},
			}),
		},
		Local: &model.Branch{
			BranchKey: branchKey,
			Name:      "My Branch",
		},
	}

	recipe := model.NewLocalLoadRecipe(mockedState.FileLoader(), state.Manifest(), state.Local)
	assert.NoError(t, mockedState.Mapper().MapAfterLocalLoad(context.Background(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	branch := recipe.Object.(*model.Branch)
	assert.NotEmpty(t, branch.Metadata)
	assert.Equal(t, "val1", branch.Metadata["KBC.KaC.Meta1"])
	assert.Equal(t, "val2", branch.Metadata["KBC.KaC.Meta2"])
}
