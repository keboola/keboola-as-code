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
		InternalObject: configState.Remote,
		ApiObject:      configState.Remote.Clone().(*model.Config),
		ChangedFields:  model.NewChangedFields(`configuration`),
	}
	err := NewMapper(context).MapBeforeRemoteSave(configRecipe)
	assert.NoError(t, err)
	assert.Empty(t, logs.String())

	// Map row
	rowRecipe := &model.RemoteSaveRecipe{
		ObjectManifest: rowState.Manifest(),
		InternalObject: rowState.Remote,
		ApiObject:      rowState.Remote.Clone().(*model.ConfigRow),
		ChangedFields:  model.NewChangedFields(`configuration`),
	}
	err = NewMapper(context).MapBeforeRemoteSave(rowRecipe)
	assert.NoError(t, err)
	assert.Empty(t, logs.String())

	// Assert
	assert.Equal(t,
		`keboola.python-transformation-v2`,
		configRecipe.ApiObject.(*model.Config).Content.GetOrNil(model.ShareCodeTargetComponentKey),
	)
	assert.Equal(t,
		[]interface{}{
			`foo`,
			`bar`,
		},
		rowRecipe.ApiObject.(*model.ConfigRow).Content.GetOrNil(model.SharedCodeContentKey),
	)
}
