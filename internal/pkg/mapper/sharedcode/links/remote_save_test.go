package links_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestRemoteSaveTranWithSharedCode(t *testing.T) {
	t.Parallel()
	mapperInst, context, logs := createMapper(t)

	// Shared code config with rows
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, context.State, context.NamingRegistry)

	// Create transformation with shared code
	transformation := createInternalTranWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, context)

	// Invoke
	object := transformation.Local
	recipe := &model.RemoteSaveRecipe{
		Object:         object,
		ObjectManifest: transformation.Manifest(),
	}
	assert.NoError(t, mapperInst.MapBeforeRemoteSave(recipe))
	assert.Empty(t, logs.AllMsgs())

	// Config ID and rows ID are set in Content
	id, found := object.Content.Get(model.SharedCodeIdContentKey)
	assert.True(t, found)
	assert.Equal(t, sharedCodeKey.Id.String(), id)
	rows, found := object.Content.Get(model.SharedCodeRowsIdContentKey)
	assert.True(t, found)
	assert.Equal(t, []interface{}{sharedCodeRowsKeys[0].ObjectId(), sharedCodeRowsKeys[1].ObjectId()}, rows)
}
