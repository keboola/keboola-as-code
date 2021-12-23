package links_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestRemoteLoadTranWithSharedCode(t *testing.T) {
	t.Parallel()
	mapperInst, context, logs := createMapper(t)
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, context.State, context.NamingRegistry)

	// Create transformation with shared code
	transformation := createRemoteTranWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, context)

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(transformation)
	assert.NoError(t, mapperInst.OnRemoteChange(changes))
	assert.Empty(t, logs.AllMsgs())

	// Values from content are converted to struct
	assert.Equal(t, &model.LinkToSharedCode{Config: sharedCodeKey, Rows: sharedCodeRowsKeys}, transformation.Remote.Transformation.LinkToSharedCode)

	// Keys from Content are deleted
	_, found := transformation.Remote.Content.Get(model.SharedCodeIdContentKey)
	assert.False(t, found)
	_, found = transformation.Remote.Content.Get(model.SharedCodeRowsIdContentKey)
	assert.False(t, found)
}

func TestRemoteLoadTranWithSharedCode_InvalidSharedCodeId(t *testing.T) {
	t.Parallel()
	mapperInst, context, logs := createMapper(t)
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, context.State, context.NamingRegistry)

	// Create transformation with shared code
	transformation := createRemoteTranWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, context)
	transformation.Remote.Content.Set(model.SharedCodeIdContentKey, `missing`) // <<<<<<<<<<<

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(transformation)
	assert.NoError(t, mapperInst.OnRemoteChange(changes))
	expectedLogs := `
WARN  Warning:
  - missing shared code config "branch:123/component:keboola.shared-code/config:missing":
    - referenced from config "branch:123/component:keboola.python-transformation-v2/config:001"
`
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logs.AllMsgs())

	// Link to shared code is not set
	assert.Nil(t, transformation.Remote.Transformation.LinkToSharedCode)

	// Keys from Content are deleted
	_, found := transformation.Remote.Content.Get(model.SharedCodeIdContentKey)
	assert.False(t, found)
	_, found = transformation.Remote.Content.Get(model.SharedCodeRowsIdContentKey)
	assert.False(t, found)
}

func TestRemoteLoadTranWithSharedCode_InvalidSharedCodeRowId(t *testing.T) {
	t.Parallel()
	mapperInst, context, logs := createMapper(t)
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, context.State, context.NamingRegistry)

	// Create transformation with shared code
	transformation := createRemoteTranWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, context)
	transformation.Remote.Content.Set(model.SharedCodeRowsIdContentKey, []interface{}{`missing`}) // <<<<<<<<<<<

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(transformation)
	assert.NoError(t, mapperInst.OnRemoteChange(changes))
	expectedLogs := `
WARN  Warning:
  - missing shared code config row "branch:123/component:keboola.shared-code/config:456/row:missing":
    - referenced from config "branch:123/component:keboola.python-transformation-v2/config:001"
`
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logs.AllMsgs())

	// Link to shared code is set, but without invalid row
	assert.Equal(t, &model.LinkToSharedCode{Config: sharedCodeKey}, transformation.Remote.Transformation.LinkToSharedCode)

	// Keys from Content are deleted
	_, found := transformation.Remote.Content.Get(model.SharedCodeIdContentKey)
	assert.False(t, found)
	_, found = transformation.Remote.Content.Get(model.SharedCodeRowsIdContentKey)
	assert.False(t, found)
}

func createRemoteTranWithSharedCode(t *testing.T, sharedCodeKey model.ConfigKey, sharedCodeRowsKeys []model.ConfigRowKey, context mapper.Context) *model.ConfigState {
	t.Helper()

	// Rows -> rows IDs
	var rows []interface{}
	for _, row := range sharedCodeRowsKeys {
		rows = append(rows, row.Id.String())
	}

	key := model.ConfigKey{
		BranchId:    sharedCodeKey.BranchId,
		ComponentId: model.ComponentId("keboola.python-transformation-v2"),
		Id:          model.ConfigId("001"),
	}

	transformation := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: key,
		},
		Remote: &model.Config{
			ConfigKey: key,
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: model.SharedCodeIdContentKey, Value: sharedCodeKey.Id.String()},
				{Key: model.SharedCodeRowsIdContentKey, Value: rows},
			}),
			Transformation: &model.Transformation{},
		},
	}

	assert.NoError(t, context.State.Set(transformation))
	return transformation
}
