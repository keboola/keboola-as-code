package codes_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/codes"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeRemoteSave(t *testing.T) {
	t.Parallel()
	targetComponentId := model.ComponentId(`keboola.python-transformation-v2`)
	context, logs, configState, rowState := createInternalSharedCode(t, targetComponentId)

	// Map config
	configRecipe := &model.RemoteSaveRecipe{
		ObjectManifest: configState.Manifest(),
		Object:         configState.Remote.Clone(),
		ChangedFields:  model.NewChangedFields(`configuration`),
	}
	err := NewMapper(context).MapBeforeRemoteSave(configRecipe)
	assert.NoError(t, err)
	assert.Empty(t, logs.String())

	// Map row
	rowRecipe := &model.RemoteSaveRecipe{
		ObjectManifest: rowState.Manifest(),
		Object:         rowState.Remote.Clone(),
		ChangedFields:  model.NewChangedFields(`configuration`),
	}
	err = NewMapper(context).MapBeforeRemoteSave(rowRecipe)
	assert.NoError(t, err)
	assert.Empty(t, logs.String())

	// Assert
	assert.Equal(t,
		`keboola.python-transformation-v2`,
		configRecipe.Object.(*model.Config).Content.GetOrNil(model.ShareCodeTargetComponentKey),
	)
	assert.Equal(t,
		[]interface{}{
			`foo`,
			`bar`,
		},
		rowRecipe.Object.(*model.ConfigRow).Content.GetOrNil(model.SharedCodeContentKey),
	)
}
