package scheduler_test

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSchedulerMapBeforeRemoteSave(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Scheduler config
	content := orderedmap.New()
	json.MustDecodeString(`{"target": {"mode": "run"}}`, content)
	object := &model.Config{Content: content}
	object.AddRelation(&model.SchedulerForRelation{
		ComponentID: `foo.bar`,
		ConfigID:    `12345`,
	})
	recipe := model.NewRemoteSaveRecipe(&model.ConfigManifest{}, object, model.NewChangedFields())

	// Invoke
	assert.NotEmpty(t, object.Relations)
	require.NoError(t, state.Mapper().MapBeforeRemoteSave(t.Context(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// All relations have been mapped
	assert.Empty(t, object.Relations)

	// Object contains target
	targetRaw, found := object.Content.Get(model.SchedulerTargetKey)
	assert.True(t, found)
	target, ok := targetRaw.(*orderedmap.OrderedMap)
	assert.True(t, ok)

	// Object contains componentID and configurationID
	componentID, found := target.Get(model.SchedulerTargetComponentIDKey)
	assert.True(t, found)
	assert.Equal(t, `foo.bar`, componentID)
	configurationID, found := target.Get(model.SchedulerTargetConfigurationIDKey)
	assert.True(t, found)
	assert.Equal(t, `12345`, configurationID)
}
