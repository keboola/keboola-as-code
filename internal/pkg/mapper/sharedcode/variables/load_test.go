package variables_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/variables"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestSharedCodeMapAfterRemoteLoad(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)

	variablesConfigId := `123456`
	content := utils.NewOrderedMap()
	content.Set(model.SharedCodeVariablesIdContentKey, variablesConfigId)
	apiObject := &model.ConfigRow{
		ConfigRowKey: model.ConfigRowKey{ComponentId: model.SharedCodeComponentId},
		Content:      content,
	}
	internalObject := apiObject.Clone().(*model.ConfigRow)
	recipe := &model.RemoteLoadRecipe{ApiObject: apiObject, InternalObject: internalObject}

	// Invoke
	assert.Empty(t, apiObject.Relations)
	assert.Empty(t, internalObject.Relations)
	assert.NoError(t, NewMapper(context).MapAfterRemoteLoad(recipe))

	// Api object is not changed
	assert.Empty(t, apiObject.Relations)
	v, found := apiObject.Content.Get(model.SharedCodeVariablesIdContentKey)
	assert.True(t, found)
	assert.Equal(t, variablesConfigId, v)

	// Internal object has new relation + content without variables ID
	assert.Equal(t, model.Relations{
		&model.SharedCodeVariablesFromRelation{
			VariablesId: variablesConfigId,
		},
	}, internalObject.Relations)
	_, found = internalObject.Content.Get(model.SharedCodeVariablesIdContentKey)
	assert.False(t, found)
}
