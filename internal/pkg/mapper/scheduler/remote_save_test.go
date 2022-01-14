package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
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
		ComponentId: `foo.bar`,
		ConfigId:    `12345`,
	})
	recipe := model.NewRemoteSaveRecipe(&model.ConfigManifest{}, object, model.NewChangedFields())

	// Invoke
	assert.NotEmpty(t, object.Relations)
	assert.NoError(t, state.Mapper().MapBeforeRemoteSave(recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// All relations have been mapped
	assert.Empty(t, object.Relations)

	// Object contains target
	targetRaw, found := object.Content.Get(model.SchedulerTargetKey)
	assert.True(t, found)
	target, ok := targetRaw.(*orderedmap.OrderedMap)
	assert.True(t, ok)

	// Object contains componentId and configurationId
	componentId, found := target.Get(model.SchedulerTargetComponentIdKey)
	assert.True(t, found)
	assert.Equal(t, `foo.bar`, componentId)
	configurationId, found := target.Get(model.SchedulerTargetConfigurationIdKey)
	assert.True(t, found)
	assert.Equal(t, `12345`, configurationId)
}
