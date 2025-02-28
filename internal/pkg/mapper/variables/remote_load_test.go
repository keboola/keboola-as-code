package variables_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestVariablesMapAfterRemoteLoad(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	variablesConfigID := `123456`
	valuesConfigRowID := `456789`
	content := orderedmap.New()
	content.Set(model.VariablesIDContentKey, variablesConfigID)
	content.Set(model.VariablesValuesIDContentKey, valuesConfigRowID)
	object := &model.Config{Content: content}
	recipe := model.NewRemoteLoadRecipe(&model.ConfigManifest{}, object)

	// Invoke
	assert.Empty(t, object.Relations)
	require.NoError(t, state.Mapper().MapAfterRemoteLoad(t.Context(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Internal object has new relation + content without variables ID
	assert.Equal(t, model.Relations{
		&model.VariablesFromRelation{
			VariablesID: keboola.ConfigID(variablesConfigID),
		},
		&model.VariablesValuesFromRelation{
			VariablesValuesID: keboola.RowID(valuesConfigRowID),
		},
	}, object.Relations)
	_, found := object.Content.Get(model.VariablesIDContentKey)
	assert.False(t, found)
	_, found = object.Content.Get(model.VariablesValuesIDContentKey)
	assert.False(t, found)
}
