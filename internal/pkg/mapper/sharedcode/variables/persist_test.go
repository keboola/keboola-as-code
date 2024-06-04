package variables_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeMapBeforePersist(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	parentKey := model.ConfigRowKey{
		BranchID:    123,
		ComponentID: keboola.SharedCodeComponentID,
		ConfigID:    `345`,
		ID:          `567`,
	}
	configManifest := &model.ConfigManifest{
		ConfigKey: model.ConfigKey{
			BranchID:    123,
			ComponentID: keboola.VariablesComponentID,
			ID:          `789`,
		},
	}
	recipe := &model.PersistRecipe{
		ParentKey: parentKey,
		Manifest:  configManifest,
	}

	// Invoke
	assert.Empty(t, configManifest.Relations)
	require.NoError(t, state.Mapper().MapBeforePersist(context.Background(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Relation has been created
	assert.Equal(t, model.Relations{
		&model.SharedCodeVariablesForRelation{
			ConfigID: `345`,
			RowID:    `567`,
		},
	}, configManifest.Relations)
}
