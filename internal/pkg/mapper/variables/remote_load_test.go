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
	object := &model.Config{Content: content}
	recipe := &model.RemoteLoadRecipe{Object: object}

	// Invoke
	assert.Empty(t, object.Relations)
	assert.NoError(t, NewMapper(context).MapAfterRemoteLoad(recipe))

	// Internal object has new relation + content without variables ID
	assert.Equal(t, model.Relations{
		&model.VariablesFromRelation{
			VariablesId: model.ConfigId(variablesConfigId),
		},
		&model.VariablesValuesFromRelation{
			VariablesValuesId: model.RowId(valuesConfigRowId),
		},
	}, object.Relations)
	_, found := object.Content.Get(model.VariablesIdContentKey)
	assert.False(t, found)
	_, found = object.Content.Get(model.VariablesValuesIdContentKey)
	assert.False(t, found)
}
