package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSchedulerLocalMapper_MapBeforePersist(t *testing.T) {
	t.Parallel()
	state, d := createLocalStateWithMapper(t)
	logger := d.DebugLogger()

	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.SchedulerComponentId,
		ConfigId:    `678`,
	}
	parentKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: `foo.bar`,
		ConfigId:    `345`,
	}
	recipe := &model.PersistRecipe{
		Key:       configKey,
		ParentKey: parentKey,
	}

	// Invoke
	assert.Empty(t, recipe.Relations)
	assert.NoError(t, state.Mapper().MapBeforePersist(recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Relation has been created
	assert.Equal(t, model.Relations{
		&model.SchedulerForRelation{
			ComponentId: `foo.bar`,
			ConfigId:    `345`,
		},
	}, recipe.Relations)
}
