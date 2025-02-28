package scheduler_test

import (
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSchedulerMapAfterRemoteLoad(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Create api and internal object
	key := model.ConfigKey{BranchID: 1, ComponentID: keboola.SchedulerComponentID, ID: `123`}
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
	require.NoError(t, state.Mapper().MapAfterRemoteLoad(t.Context(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Internal object has new relation
	assert.Equal(t, model.Relations{
		&model.SchedulerForRelation{
			ComponentID: `foo.bar`,
			ConfigID:    `123`,
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
