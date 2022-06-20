package variables_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeMapBeforePersist(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	parentKey := model.ConfigRowKey{
		BranchId:    123,
		ComponentId: storageapi.SharedCodeComponentID,
		ConfigId:    `345`,
		Id:          `567`,
	}
	configManifest := &model.ConfigManifest{
		ConfigKey: model.ConfigKey{
			BranchId:    123,
			ComponentId: storageapi.VariablesComponentID,
			Id:          `789`,
		},
	}
	recipe := &model.PersistRecipe{
		ParentKey: parentKey,
		Manifest:  configManifest,
	}

	// Invoke
	assert.Empty(t, configManifest.Relations)
	assert.NoError(t, state.Mapper().MapBeforePersist(context.Background(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Relation has been created
	assert.Equal(t, model.Relations{
		&model.SharedCodeVariablesForRelation{
			ConfigId: `345`,
			RowId:    `567`,
		},
	}, configManifest.Relations)
}
