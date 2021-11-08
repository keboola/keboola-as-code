package sharedcode_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeMapBeforePersist(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)

	parentKey := model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.SharedCodeComponentId,
		ConfigId:    `345`,
		Id:          `567`,
	}
	configManifest := &model.ConfigManifest{
		ConfigKey: model.ConfigKey{
			BranchId:    123,
			ComponentId: model.VariablesComponentId,
			Id:          `789`,
		},
	}
	recipe := &model.PersistRecipe{
		ParentKey: parentKey,
		Manifest:  configManifest,
	}

	// Invoke
	assert.Empty(t, configManifest.Relations)
	assert.NoError(t, NewMapper(context).MapBeforePersist(recipe))

	// Relation has been created
	assert.Equal(t, model.Relations{
		&model.SharedCodeVariablesForRelation{
			ConfigId: `345`,
			RowId:    `567`,
		},
	}, configManifest.Relations)
}
