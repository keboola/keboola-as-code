package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestSchedulerMapAfterRemoteLoad(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)
	schedulerApi, _, _ := testapi.NewMockedSchedulerApi()
	mapper := NewMapper(context, schedulerApi)

	// Create api and internal object
	key := model.ConfigKey{BranchId: 1, ComponentId: model.SchedulerComponentId, Id: `123`}
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
	recipe := &model.RemoteLoadRecipe{Object: object}

	// Invoke
	assert.Empty(t, object.Relations)
	assert.NoError(t, mapper.MapAfterRemoteLoad(recipe))

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
