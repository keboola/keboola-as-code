package links_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestSharedCodeLinksMapAfterLocalLoad(t *testing.T) {
	t.Parallel()
	mapperInst, context, logs := createMapper(t)

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
		Content:   utils.NewOrderedMap(),
	}
	config.Content.Set(model.SharedCodePathContentKey, `_shared/keboola.python-transformation-v2`)
	configStateRaw, err := context.State.GetOrCreateFrom(&model.ConfigManifest{
		ConfigKey: configKey,
	})
	assert.NoError(t, err)
	configState := configStateRaw.(*model.ConfigState)
	configState.SetLocalState(config)

	// Invoke

	assert.NoError(t, mapperInst.OnObjectsLoad(model.StateTypeLocal, []model.Object{config}))
	assert.Empty(t, logs.String())

	// Path is replaced by ID
	_, found := config.Content.Get(model.SharedCodePathContentKey)
	assert.False(t, found)
	sharedCodeId, found := config.Content.Get(model.SharedCodeIdContentKey)
	assert.True(t, found)
	assert.Equal(t, sharedCodeId, `456`)
}
