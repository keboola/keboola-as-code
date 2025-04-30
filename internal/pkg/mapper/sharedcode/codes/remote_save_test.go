package codes_test

import (
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeRemoteSave(t *testing.T) {
	t.Parallel()
	targetComponentID := keboola.ComponentID(`keboola.python-transformation-v2`)

	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()
	configState, rowState := createInternalSharedCode(t, targetComponentID, state)

	// Map config
	configRecipe := model.NewRemoteSaveRecipe(configState.Manifest(), configState.Remote, model.NewChangedFields(`configuration`))
	err := state.Mapper().MapBeforeRemoteSave(t.Context(), configRecipe)
	require.NoError(t, err)
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Map row
	rowRecipe := model.NewRemoteSaveRecipe(rowState.Manifest(), rowState.Remote, model.NewChangedFields(`configuration`))
	err = state.Mapper().MapBeforeRemoteSave(t.Context(), rowRecipe)
	require.NoError(t, err)
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Assert
	assert.Equal(t,
		`keboola.python-transformation-v2`,
		configRecipe.Object.(*model.Config).Content.GetOrNil(model.ShareCodeTargetComponentKey),
	)
	assert.Equal(t,
		[]any{
			`foo`,
			`bar`,
		},
		rowRecipe.Object.(*model.ConfigRow).Content.GetOrNil(model.SharedCodeContentKey),
	)
}
