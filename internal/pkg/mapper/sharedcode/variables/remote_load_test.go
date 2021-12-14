package variables_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/variables"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestSharedCodeMapAfterRemoteLoad(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)

	variablesConfigId := `123456`
	content := orderedmap.New()
	content.Set(model.SharedCodeVariablesIdContentKey, variablesConfigId)
	object := &model.ConfigRow{
		ConfigRowKey: model.ConfigRowKey{ComponentId: model.SharedCodeComponentId},
		Content:      content,
	}
	recipe := &model.RemoteLoadRecipe{Object: object}

	// Invoke
	assert.Empty(t, object.Relations)
	assert.NoError(t, NewMapper(context).MapAfterRemoteLoad(recipe))

	// Object has new relation + content without variables ID
	assert.Equal(t, model.Relations{
		&model.SharedCodeVariablesFromRelation{
			VariablesId: model.ConfigId(variablesConfigId),
		},
	}, object.Relations)
	_, found := object.Content.Get(model.SharedCodeVariablesIdContentKey)
	assert.False(t, found)
}
