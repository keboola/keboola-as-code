package helper

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestGetSharedCodePath(t *testing.T) {
	t.Parallel()
	fs := testhelper.NewMemoryFs()
	state := model.NewState(zap.NewNop().Sugar(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	h := New(state, model.DefaultNamingWithIds())

	transformation := &model.Config{
		ConfigKey: model.ConfigKey{
			BranchId:    123,
			ComponentId: `keboola.python-transformation-v2`,
			Id:          `456`,
		},
		Content: utils.PairsToOrderedMap([]utils.Pair{
			{
				Key:   model.SharedCodePathContentKey,
				Value: `_shared/keboola.python-transformation-v2`,
			},
		}),
	}

	// Valid
	_, path, err := h.GetSharedCodePath(transformation)
	assert.NoError(t, err)
	assert.Equal(t, `_shared/keboola.python-transformation-v2`, path)

	// Not config
	row := &model.ConfigRow{}
	_, path, err = h.GetSharedCodePath(row)
	assert.NoError(t, err)
	assert.Empty(t, path)

	// Not transformation
	object := transformation.Clone().(*model.Config)
	object.ComponentId = `foo.bar`
	_, path, err = h.GetSharedCodePath(object)
	assert.NoError(t, err)
	assert.Empty(t, path)

	// No path
	object = transformation.Clone().(*model.Config)
	object.Content = utils.NewOrderedMap()
	_, path, err = h.GetSharedCodePath(object)
	assert.NoError(t, err)
	assert.Empty(t, path)

	// Path is not string
	object = transformation.Clone().(*model.Config)
	object.Content.Set(model.SharedCodePathContentKey, 123)
	_, path, err = h.GetSharedCodePath(object)
	assert.Error(t, err)
	assert.Equal(t, `key "shared_code_path" must be string, found int, in config "branch:123/component:keboola.python-transformation-v2/config:456"`, err.Error())
	assert.Empty(t, path)
}

func TestGetSharedCodeKey(t *testing.T) {
	t.Parallel()
	fs := testhelper.NewMemoryFs()
	state := model.NewState(zap.NewNop().Sugar(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	h := New(state, model.DefaultNamingWithIds())

	transformation := &model.Config{
		ConfigKey: model.ConfigKey{
			BranchId:    123,
			ComponentId: `keboola.python-transformation-v2`,
			Id:          `456`,
		},
		Content: utils.PairsToOrderedMap([]utils.Pair{
			{
				Key:   model.SharedCodeIdContentKey,
				Value: `789`,
			},
		}),
	}

	// Valid
	_, key, err := h.GetSharedCodeKey(transformation)
	assert.NoError(t, err)
	assert.Equal(t, model.ConfigKey{
		BranchId:    123,
		ComponentId: model.SharedCodeComponentId,
		Id:          `789`,
	}, key)

	// Not config
	row := &model.ConfigRow{}
	_, key, err = h.GetSharedCodeKey(row)
	assert.NoError(t, err)
	assert.Nil(t, key)

	// Not transformation
	object := transformation.Clone().(*model.Config)
	object.ComponentId = `foo.bar`
	_, key, err = h.GetSharedCodeKey(object)
	assert.NoError(t, err)
	assert.Nil(t, key)

	// No ID
	object = transformation.Clone().(*model.Config)
	object.Content = utils.NewOrderedMap()
	_, key, err = h.GetSharedCodeKey(object)
	assert.NoError(t, err)
	assert.Empty(t, key)

	// ID is not string
	object = transformation.Clone().(*model.Config)
	object.Content.Set(model.SharedCodeIdContentKey, 123)
	_, key, err = h.GetSharedCodeKey(object)
	assert.Error(t, err)
	assert.Equal(t, `key "shared_code_id" must be string, found int, in config "branch:123/component:keboola.python-transformation-v2/config:456"`, err.Error())
	assert.Empty(t, key)
}

func TestGetSharedCodeByPath(t *testing.T) {
	t.Parallel()
	fs := testhelper.NewMemoryFs()
	state := model.NewState(zap.NewNop().Sugar(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	naming := model.DefaultNamingWithIds()
	h := New(state, naming)
	sharedCodeKey := fixtures.CreateSharedCode(t, state, naming)

	// Found
	result, err := h.GetSharedCodeByPath(model.BranchKey{Id: 123}, `_shared/keboola.python-transformation-v2`)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, sharedCodeKey, result.Key())

	// Different branch
	result, err = h.GetSharedCodeByPath(model.BranchKey{Id: 456}, `_shared/keboola.python-transformation-v2`)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Equal(t, `branch "456" not found`, err.Error())

	// Not found
	result, err = h.GetSharedCodeByPath(model.BranchKey{Id: 123}, `foo/bar`)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Equal(t, `shared code "branch/foo/bar" not found`, err.Error())
}

func TestGetSharedCodeRowByPath(t *testing.T) {
	t.Parallel()
	fs := testhelper.NewMemoryFs()
	state := model.NewState(zap.NewNop().Sugar(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	naming := model.DefaultNamingWithIds()
	h := New(state, naming)
	sharedCodeKey := fixtures.CreateSharedCode(t, state, naming)
	sharedCode := state.MustGet(sharedCodeKey).(*model.ConfigState)

	// Found
	result, err := h.GetSharedCodeRowByPath(sharedCode, `codes/code1`)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.SharedCodeComponentId,
		ConfigId:    `456`,
		Id:          `1234`,
	}, result.Key())

	// Not found
	result, err = h.GetSharedCodeRowByPath(sharedCode, `foo/bar`)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Equal(t, `shared code row "branch/_shared/keboola.python-transformation-v2/foo/bar" not found`, err.Error())
}

func TestGetSharedCodeVariablesId(t *testing.T) {
	t.Parallel()
	fs := testhelper.NewMemoryFs()
	state := model.NewState(zap.NewNop().Sugar(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	naming := model.DefaultNamingWithIds()
	h := New(state, model.DefaultNamingWithIds())

	fixtures.CreateSharedCode(t, state, naming)
	sharedCodeRow1 := state.MustGet(model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.SharedCodeComponentId,
		ConfigId:    `456`,
		Id:          `1234`,
	}).(*model.ConfigRowState)
	sharedCodeRow2 := state.MustGet(model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.SharedCodeComponentId,
		ConfigId:    `456`,
		Id:          `5678`,
	}).(*model.ConfigRowState)

	sharedCodeRow1.Local.Content.Set(model.SharedCodeVariablesIdContentKey, `789`)

	// Found
	variablesId, found := h.GetSharedCodeVariablesId(sharedCodeRow1.Local)
	assert.True(t, found)
	assert.Equal(t, `789`, variablesId)

	// Not found
	_, found = h.GetSharedCodeVariablesId(sharedCodeRow2.Local)
	assert.False(t, found)
}
