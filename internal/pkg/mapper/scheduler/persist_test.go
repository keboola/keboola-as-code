package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
)

func TestVariablesMapBeforePersist(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)
	schedulerApi, _, _ := testapi.NewMockedSchedulerApi()
	mapper := NewMapper(context, schedulerApi)

	parentKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: `foo.bar`,
		Id:          `345`,
	}
	configManifest := &model.ConfigManifest{
		ConfigKey: model.ConfigKey{
			BranchId:    123,
			ComponentId: model.SchedulerComponentId,
			Id:          `678`,
		},
	}
	recipe := &model.PersistRecipe{
		ParentKey: parentKey,
		Manifest:  configManifest,
	}

	// Invoke
	assert.Empty(t, configManifest.Relations)
	assert.NoError(t, mapper.MapBeforePersist(recipe))

	// Relation has been created
	assert.Equal(t, model.Relations{
		&model.SchedulerForRelation{
			ComponentId: `foo.bar`,
			ConfigId:    `345`,
		},
	}, configManifest.Relations)
}
