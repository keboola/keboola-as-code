package variables_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeMapBeforeRemoteSave(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	variablesConfigId := `123456`
	object := &model.ConfigRow{
		ConfigRowKey: model.ConfigRowKey{ComponentId: storageapi.SharedCodeComponentID},
		Content:      orderedmap.New(),
	}
	object.AddRelation(&model.SharedCodeVariablesFromRelation{
		VariablesId: storageapi.ConfigID(variablesConfigId),
	})
	recipe := model.NewRemoteSaveRecipe(&model.ConfigManifest{}, object, model.NewChangedFields())

	// Invoke
	assert.NotEmpty(t, object.Relations)
	assert.NotEmpty(t, object.Relations)
	assert.NoError(t, state.Mapper().MapBeforeRemoteSave(context.Background(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// All relations have been mapped
	assert.Empty(t, object.Relations)

	// Api object contains variables ID in content
	v, found := object.Content.Get(model.SharedCodeVariablesIdContentKey)
	assert.True(t, found)
	assert.Equal(t, variablesConfigId, v)
}
