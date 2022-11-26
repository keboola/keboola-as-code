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

	variablesConfigID := `123456`
	content := orderedmap.New()
	content.Set(model.SharedCodeVariablesIDContentKey, variablesConfigID)
	object := &model.ConfigRow{
		ConfigRowKey: model.ConfigRowKey{ComponentID: storageapi.SharedCodeComponentID},
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
			VariablesID: storageapi.ConfigID(variablesConfigID),
		},
	}, object.Relations)
	_, found := object.Content.Get(model.SharedCodeVariablesIDContentKey)
	assert.False(t, found)
}
