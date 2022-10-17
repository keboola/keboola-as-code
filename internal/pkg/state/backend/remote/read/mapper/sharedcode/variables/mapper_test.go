package variables_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeMapAfterRemoteLoad(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	variablesConfigId := `123456`
	content := orderedmap.New()
	content.Set(model.SharedCodeVariablesIdContentKey, variablesConfigId)
	object := &model.ConfigRow{
		ConfigRowKey: model.ConfigRowKey{ComponentId: storageapi.SharedCodeComponentID},
		Content:      content,
	}
	recipe := model.NewRemoteLoadRecipe(&model.ConfigRowManifest{}, object)

	// Invoke
	assert.Empty(t, object.Relations)
	assert.NoError(t, state.Mapper().MapAfterRemoteLoad(context.Background(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Object has new relation + content without variables ID
	assert.Equal(t, model.Relations{
		&model.SharedCodeVariablesFromRelation{
			VariablesId: storageapi.ConfigID(variablesConfigId),
		},
	}, object.Relations)
	_, found := object.Content.Get(model.SharedCodeVariablesIdContentKey)
	assert.False(t, found)
}
