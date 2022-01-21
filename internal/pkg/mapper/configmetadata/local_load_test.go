package configmetadata_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/configmetadata"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testdeps"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestConfigMetadataMapper_MapAfterLocalLoad(t *testing.T) {
	t.Parallel()
	d := testdeps.New()
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
			Metadata: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "KBC.KaC.Meta1", Value: "val1"},
				{Key: "KBC.KaC.Meta2", Value: "val2"},
			}),
		},
		Local: &model.Config{
			ConfigKey: configKey,
			Name:      "My Config",
			Content:   orderedmap.New(),
		},
	}

	recipe := model.NewLocalLoadRecipe(configState.Manifest(), configState.Local)
	assert.NoError(t, mockedState.Mapper().MapAfterLocalLoad(recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	config := recipe.Object.(*model.Config)
	assert.NotEmpty(t, config.Metadata)
	assert.Equal(t, "val1", config.Metadata["KBC.KaC.Meta1"])
	assert.Equal(t, "val2", config.Metadata["KBC.KaC.Meta2"])
}
