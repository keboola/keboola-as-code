package links_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestRemoteSaveTranWithSharedCode(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Shared code config with rows
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, state)

	// Create transformation with shared code
	transformation := createInternalTranWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, state)

	// Invoke
	object := transformation.Local
	recipe := model.NewRemoteSaveRecipe(transformation.Manifest(), object, model.NewChangedFields())
	require.NoError(t, state.Mapper().MapBeforeRemoteSave(t.Context(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Config ID and rows ID are set in Content
	id, found := object.Content.Get(model.SharedCodeIDContentKey)
	assert.True(t, found)
	assert.Equal(t, sharedCodeKey.ID.String(), id)
	rows, found := object.Content.Get(model.SharedCodeRowsIDContentKey)
	assert.True(t, found)
	assert.Equal(t, []any{sharedCodeRowsKeys[0].ObjectID(), sharedCodeRowsKeys[1].ObjectID()}, rows)
}
