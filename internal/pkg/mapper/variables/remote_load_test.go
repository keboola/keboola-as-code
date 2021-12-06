package variables_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/variables"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestVariablesMapAfterRemoteLoad(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)

	variablesConfigId := `123456`
	valuesConfigRowId := `456789`
	content := orderedmap.New()
	content.Set(model.VariablesIdContentKey, variablesConfigId)
	content.Set(model.VariablesValuesIdContentKey, valuesConfigRowId)
	apiObject := &model.Config{Content: content}
	internalObject := apiObject.Clone().(*model.Config)
	recipe := &model.RemoteLoadRecipe{ApiObject: apiObject, InternalObject: internalObject}

	// Invoke
	assert.Empty(t, apiObject.Relations)
	assert.Empty(t, internalObject.Relations)
	assert.NoError(t, NewMapper(context).MapAfterRemoteLoad(recipe))

	// Api object is not changed
	assert.Empty(t, apiObject.Relations)
	v, found := apiObject.Content.Get(model.VariablesIdContentKey)
	assert.True(t, found)
	assert.Equal(t, variablesConfigId, v)

	// Internal object has new relation + content without variables ID
	assert.Equal(t, model.Relations{
		&model.VariablesFromRelation{
			VariablesId: variablesConfigId,
		},
		&model.VariablesValuesFromRelation{
			VariablesValuesId: valuesConfigRowId,
		},
	}, internalObject.Relations)
	_, found = internalObject.Content.Get(model.VariablesIdContentKey)
	assert.False(t, found)
	_, found = internalObject.Content.Get(model.VariablesValuesIdContentKey)
	assert.False(t, found)
}
