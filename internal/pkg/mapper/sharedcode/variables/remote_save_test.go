package variables_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/variables"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestSharedCodeMapBeforeRemoteSave(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)

	variablesConfigId := `123456`
	apiObject := &model.ConfigRow{
		ConfigRowKey: model.ConfigRowKey{ComponentId: model.SharedCodeComponentId},
		Content:      orderedmap.New(),
	}
	apiObject.AddRelation(&model.SharedCodeVariablesFromRelation{
		VariablesId: model.ConfigId(variablesConfigId),
	})
	internalObject := apiObject.Clone().(*model.ConfigRow)
	recipe := &model.RemoteSaveRecipe{
		ApiObject:      apiObject,
		InternalObject: internalObject,
		Manifest:       &model.ConfigManifest{},
	}

	// Invoke
	assert.NotEmpty(t, apiObject.Relations)
	assert.NotEmpty(t, internalObject.Relations)
	assert.NoError(t, NewMapper(context).MapBeforeRemoteSave(recipe))

	// Internal object is not changed
	assert.Equal(t, model.Relations{
		&model.SharedCodeVariablesFromRelation{
			VariablesId: model.ConfigId(variablesConfigId),
		},
	}, internalObject.Relations)
	_, found := internalObject.Content.Get(model.SharedCodeVariablesIdContentKey)
	assert.False(t, found)

	// All relations have been mapped
	assert.Empty(t, apiObject.Relations)

	// Api object contains variables ID in content
	v, found := apiObject.Content.Get(model.SharedCodeVariablesIdContentKey)
	assert.True(t, found)
	assert.Equal(t, variablesConfigId, v)
}
