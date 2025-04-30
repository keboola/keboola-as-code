package variables_test

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestVariablesMapBeforeRemoteSave(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	variablesConfigID := `123456`
	valuesConfigRowID := `456789`
	object := &model.Config{Content: orderedmap.New()}
	object.AddRelation(&model.VariablesFromRelation{
		VariablesID: keboola.ConfigID(variablesConfigID),
	})
	object.AddRelation(&model.VariablesValuesFromRelation{
		VariablesValuesID: keboola.RowID(valuesConfigRowID),
	})
	recipe := model.NewRemoteSaveRecipe(&model.ConfigManifest{}, object, model.NewChangedFields())

	// Invoke
	assert.NotEmpty(t, object.Relations)
	assert.NotEmpty(t, object.Relations)
	require.NoError(t, state.Mapper().MapBeforeRemoteSave(t.Context(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// All relations have been mapped
	assert.Empty(t, object.Relations)

	// Object contains variables ID in content
	v, found := object.Content.Get(model.VariablesIDContentKey)
	assert.True(t, found)
	assert.Equal(t, variablesConfigID, v)

	// Object contains variables values ID in content
	v, found = object.Content.Get(model.VariablesValuesIDContentKey)
	assert.True(t, found)
	assert.Equal(t, valuesConfigRowID, v)
}
