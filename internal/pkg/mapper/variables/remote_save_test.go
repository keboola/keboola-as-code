package variables_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/variables"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestVariablesMapBeforeRemoteSave(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)

	variablesConfigId := `123456`
	valuesConfigRowId := `456789`
	object := &model.Config{Content: orderedmap.New()}
	object.AddRelation(&model.VariablesFromRelation{
		VariablesId: model.ConfigId(variablesConfigId),
	})
	object.AddRelation(&model.VariablesValuesFromRelation{
		VariablesValuesId: model.RowId(valuesConfigRowId),
	})
	recipe := &model.RemoteSaveRecipe{
		Object:         object,
		ObjectManifest: &model.ConfigManifest{},
	}

	// Invoke
	assert.NotEmpty(t, object.Relations)
	assert.NotEmpty(t, object.Relations)
	assert.NoError(t, NewMapper(context).MapBeforeRemoteSave(recipe))

	// All relations have been mapped
	assert.Empty(t, object.Relations)

	// Object contains variables ID in content
	v, found := object.Content.Get(model.VariablesIdContentKey)
	assert.True(t, found)
	assert.Equal(t, variablesConfigId, v)

	// Object contains variables values ID in content
	v, found = object.Content.Get(model.VariablesValuesIdContentKey)
	assert.True(t, found)
	assert.Equal(t, valuesConfigRowId, v)
}
