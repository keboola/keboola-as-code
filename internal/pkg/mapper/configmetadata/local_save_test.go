package configmetadata_test

import (
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/configmetadata"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestConfigMetadataMapper_MapBeforeLocalSave(t *testing.T) {
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
		},
		Local: &model.Config{
			ConfigKey: configKey,
			Name:      "My Config",
			Content:   orderedmap.New(),
			Metadata:  map[string]string{"KBC.KaC.Meta1": "val1", "KBC.KaC.Meta2": "val2"},
		},
	}

	recipe := model.NewLocalSaveRecipe(configState.Manifest(), configState.Local, model.NewChangedFields())
	require.NoError(t, mockedState.Mapper().MapBeforeLocalSave(t.Context(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	configManifest := recipe.ObjectManifest.(*model.ConfigManifest)
	assert.Equal(t, orderedmap.FromPairs([]orderedmap.Pair{
		{Key: "KBC.KaC.Meta1", Value: "val1"},
		{Key: "KBC.KaC.Meta2", Value: "val2"},
	}), configManifest.Metadata)
}
