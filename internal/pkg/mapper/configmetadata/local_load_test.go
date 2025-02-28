package configmetadata_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/configmetadata"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestConfigMetadataMapper_MapAfterLocalLoad(t *testing.T) {
	t.Parallel()
	d := dependencies.NewMocked(t, t.Context())
	logger := d.DebugLogger()
	mockedState := d.MockedState()
	mockedState.Mapper().AddMapper(configmetadata.NewMapper(mockedState, d))

	configKey := model.ConfigKey{
		BranchID:    123,
		ComponentID: keboola.ComponentID("keboola.snowflake-transformation"),
		ID:          `456`,
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

	recipe := model.NewLocalLoadRecipe(mockedState.FileLoader(), configState.Manifest(), configState.Local)
	require.NoError(t, mockedState.Mapper().MapAfterLocalLoad(t.Context(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	config := recipe.Object.(*model.Config)
	assert.NotEmpty(t, config.Metadata)
	assert.Equal(t, "val1", config.Metadata["KBC.KaC.Meta1"])
	assert.Equal(t, "val2", config.Metadata["KBC.KaC.Meta2"])
}
