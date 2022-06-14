package configmetadata_test

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/configmetadata"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestConfigMetadataMapper_MapBeforeLocalSave(t *testing.T) {
	t.Parallel()
	d := dependencies.NewTestContainer()
	logger := d.DebugLogger()
	mockedState := d.EmptyState()
	mockedState.Mapper().AddMapper(configmetadata.NewMapper(mockedState, d))

	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.ComponentId("keboola.snowflake-transformation"),
		Id:          `456`,
	}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
		},
		Local: &model.Config{
			ConfigKey: configKey,
			Name:      "My Config",
			Content:   orderedmap.New(),
			Metadata:  map[string]string{"KBC.KaC.Meta1": "val1", "KBC.KaC.Meta2": "val2"},
		},
	}

	recipe := model.NewLocalSaveRecipe(configState.Manifest(), configState.Local, model.NewChangedFields())
	assert.NoError(t, mockedState.Mapper().MapBeforeLocalSave(recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	configManifest := recipe.ObjectManifest.(*model.ConfigManifest)
	assert.Equal(t, orderedmap.FromPairs([]orderedmap.Pair{
		{Key: "KBC.KaC.Meta1", Value: "val1"},
		{Key: "KBC.KaC.Meta2", Value: "val2"},
	}), configManifest.Metadata)
}
