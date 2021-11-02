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
	apiObject := &model.Config{Content: utils.NewOrderedMap()}
	internalObject := apiObject.Clone().(*model.Config)
	internalObject.AddRelation(&model.VariablesFromRelation{
		Source: model.ConfigKeySameBranch{
			ComponentId: model.VariablesComponentId,
			Id:          variablesConfigId,
		},
	})
	recipe := &model.RemoteSaveRecipe{ApiObject: apiObject, InternalObject: internalObject}

	// Invoke
	assert.Empty(t, apiObject.Relations)
	assert.NotEmpty(t, internalObject.Relations)
	assert.NoError(t, NewMapper(context).MapBeforeRemoteSave(recipe))

	// Internal object is not changed
	assert.Equal(t, model.Relations{
		&model.VariablesFromRelation{
			Source: model.ConfigKeySameBranch{
				ComponentId: model.VariablesComponentId,
				Id:          variablesConfigId,
			},
		},
	}, internalObject.Relations)
	_, found := internalObject.Content.Get(model.VariablesIdContentKey)
	assert.False(t, found)

	// Api object contains variables ID in content
	assert.Empty(t, apiObject.Relations)
	v, found := apiObject.Content.Get(model.VariablesIdContentKey)
	assert.True(t, found)
	assert.Equal(t, variablesConfigId, v)
}
