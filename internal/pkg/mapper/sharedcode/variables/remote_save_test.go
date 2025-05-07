package variables_test

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeMapBeforeRemoteSave(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	variablesConfigID := `123456`
	object := &model.ConfigRow{
		ConfigRowKey: model.ConfigRowKey{ComponentID: keboola.SharedCodeComponentID},
		Content:      orderedmap.New(),
	}
	object.AddRelation(&model.SharedCodeVariablesFromRelation{
		VariablesID: keboola.ConfigID(variablesConfigID),
	})
	recipe := model.NewRemoteSaveRecipe(&model.ConfigManifest{}, object, model.NewChangedFields())

	// Invoke
	assert.NotEmpty(t, object.Relations)
	assert.NotEmpty(t, object.Relations)
	require.NoError(t, state.Mapper().MapBeforeRemoteSave(t.Context(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// All relations have been mapped
	assert.Empty(t, object.Relations)

	// Api object contains variables ID in content
	v, found := object.Content.Get(model.SharedCodeVariablesIDContentKey)
	assert.True(t, found)
	assert.Equal(t, variablesConfigID, v)
}
