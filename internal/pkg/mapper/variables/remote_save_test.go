package variables_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/variables"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestVariablesMapBeforeRemoteSave(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)

	variablesConfigId := `123456`
	valuesConfigRowId := `456789`
	apiObject := &model.Config{Content: utils.NewOrderedMap()}
	apiObject.AddRelation(&model.VariablesFromRelation{
		VariablesId: variablesConfigId,
	})
	apiObject.AddRelation(&model.VariablesValuesFromRelation{
		VariablesValuesId: valuesConfigRowId,
	})
	internalObject := apiObject.Clone().(*model.Config)
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
		&model.VariablesFromRelation{
			VariablesId: variablesConfigId,
		},
		&model.VariablesValuesFromRelation{
			VariablesValuesId: valuesConfigRowId,
		},
	}, internalObject.Relations)
	_, found := internalObject.Content.Get(model.VariablesIdContentKey)
	assert.False(t, found)

	// All relations have been mapped
	assert.Empty(t, apiObject.Relations)

	// Api object contains variables ID in content
	v, found := apiObject.Content.Get(model.VariablesIdContentKey)
	assert.True(t, found)
	assert.Equal(t, variablesConfigId, v)

	// Api object contains variables values ID in content
	v, found = apiObject.Content.Get(model.VariablesValuesIdContentKey)
	assert.True(t, found)
	assert.Equal(t, valuesConfigRowId, v)
}
