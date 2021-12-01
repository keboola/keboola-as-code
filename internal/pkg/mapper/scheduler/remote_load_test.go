package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestSchedulerMapAfterRemoteLoad(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)

	// Create api and internal object
	key := model.ConfigKey{BranchId: 1, ComponentId: model.SchedulerComponentId, Id: `123`}
	apiObject := &model.Config{ConfigKey: key, Content: utils.NewOrderedMap()}
	apiContentStr := `{
  "target": {
    "componentId": "foo.bar",
    "configurationId": "123",
    "mode": "run"
  }
}
`
	json.MustDecodeString(apiContentStr, apiObject.Content)
	internalObject := apiObject.Clone().(*model.Config)
	recipe := &model.RemoteLoadRecipe{ApiObject: apiObject, InternalObject: internalObject}

	// Invoke
	assert.Empty(t, apiObject.Relations)
	assert.Empty(t, internalObject.Relations)
	assert.NoError(t, NewMapper(context).MapAfterRemoteLoad(recipe))

	// Api object is not changed
	assert.Empty(t, apiObject.Relations)
	assert.Equal(t, apiContentStr, json.MustEncodeString(apiObject.Content, true))

	// Internal object has new relation
	assert.Equal(t, model.Relations{
		&model.SchedulerForRelation{
			ComponentId: `foo.bar`,
			ConfigId:    `123`,
		},
	}, internalObject.Relations)

	// Internal object target is without component and configuration ID
	expectedInternalContent := `{
  "target": {
    "mode": "run"
  }
}
`
	assert.Equal(t, expectedInternalContent, json.MustEncodeString(internalObject.Content, true))
}
