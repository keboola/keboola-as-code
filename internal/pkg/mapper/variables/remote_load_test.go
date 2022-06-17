package variables_test

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestVariablesMapAfterRemoteLoad(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	variablesConfigId := `123456`
	valuesConfigRowId := `456789`
	content := orderedmap.New()
	content.Set(model.VariablesIdContentKey, variablesConfigId)
	content.Set(model.VariablesValuesIdContentKey, valuesConfigRowId)
	object := &model.Config{Content: content}
	recipe := model.NewRemoteLoadRecipe(&model.ConfigManifest{}, object)

	// Invoke
	assert.Empty(t, object.Relations)
	assert.NoError(t, state.Mapper().MapAfterRemoteLoad(recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Internal object has new relation + content without variables ID
	assert.Equal(t, model.Relations{
		&model.VariablesFromRelation{
			VariablesId: storageapi.ConfigID(variablesConfigId),
		},
		&model.VariablesValuesFromRelation{
			VariablesValuesId: storageapi.RowID(valuesConfigRowId),
		},
	}, object.Relations)
	_, found := object.Content.Get(model.VariablesIdContentKey)
	assert.False(t, found)
	_, found = object.Content.Get(model.VariablesValuesIdContentKey)
	assert.False(t, found)
}
