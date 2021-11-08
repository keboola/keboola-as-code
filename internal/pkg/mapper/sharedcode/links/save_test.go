package links_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/links"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestSharedCodeLinksMapBeforeLocalSave(t *testing.T) {
	t.Parallel()
	context, logs := createMapperContext(t)

	// Shared code
	sharedCodeKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.SharedCodeComponentId,
		Id:          `456`,
	}
	sharedCodeStateRaw, err := context.State.GetOrCreateFrom(&model.ConfigManifest{
		ConfigKey: sharedCodeKey,
		Paths: model.Paths{
			PathInProject: model.NewPathInProject(
				`branch`,
				`_shared/keboola.python-transformation-v2`,
			),
		},
	})
	assert.NoError(t, err)
	sharedCodeState := sharedCodeStateRaw.(*model.ConfigState)
	sharedCodeState.SetLocalState(&model.Config{
		ConfigKey: sharedCodeKey,
		Content: utils.PairsToOrderedMap([]utils.Pair{
			{Key: model.SharedCodeComponentIdContentKey, Value: `keboola.python-transformation-v2`},
		}),
	})

	// Config using shared code
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: `keboola.python-transformation-v2`,
		Id:          `789`,
	}
	config := &model.Config{
		ConfigKey: configKey,
	}
	configManifest := &model.ConfigManifest{
		ConfigKey: configKey,
	}
	recipe := createLocalSaveRecipe(config, configManifest)
	recipe.Configuration.Content.Set(model.SharedCodeIdContentKey, `456`)

	// Invoke
	assert.NoError(t, NewMapper(context).MapBeforeLocalSave(recipe))
	assert.Empty(t, logs.String())

	// Path is replaced by ID
	_, found := recipe.Configuration.Content.Get(model.SharedCodeIdContentKey)
	assert.False(t, found)
	sharedCodeId, found := recipe.Configuration.Content.Get(model.SharedCodePathContentKey)
	assert.True(t, found)
	assert.Equal(t, sharedCodeId, `_shared/keboola.python-transformation-v2`)
}
