package scheduler_test

import (
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSchedulerMapAfterRemoteLoad(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Create api and internal object
	key := model.ConfigKey{BranchId: 1, ComponentId: storageapi.SchedulerComponentID, Id: `123`}
	object := &model.Config{ConfigKey: key, Content: orderedmap.New()}
	contentStr := `{
  "target": {
    "componentId": "foo.bar",
    "configurationId": "123",
    "mode": "run"
  }
}
`
	json.MustDecodeString(contentStr, object.Content)
	recipe := model.NewRemoteLoadRecipe(&model.ConfigManifest{}, object)

	// Invoke
	assert.Empty(t, object.Relations)
	assert.NoError(t, state.Mapper().MapAfterRemoteLoad(recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Internal object has new relation
	assert.Equal(t, model.Relations{
		&model.SchedulerForRelation{
			ComponentId: `foo.bar`,
			ConfigId:    `123`,
		},
	}, object.Relations)

	// Object target is without component and configuration ID
	exoected := `{
  "target": {
    "mode": "run"
  }
}
`
	assert.Equal(t, exoected, json.MustEncodeString(object.Content, true))
}
