package links

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
	m := NewMapper(nil, model.MapperContext{State: state})

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
	_, path, err := m.getSharedCodePath(transformation)
	assert.NoError(t, err)
	assert.Equal(t, `_shared/keboola.python-transformation-v2`, path)

	// Not config
	row := &model.ConfigRow{}
	_, path, err = m.getSharedCodePath(row)
	assert.NoError(t, err)
	assert.Empty(t, path)

	// Not transformation
	object := transformation.Clone().(*model.Config)
	object.ComponentId = `foo.bar`
	_, path, err = m.getSharedCodePath(object)
	assert.NoError(t, err)
	assert.Empty(t, path)

	// No path
	object = transformation.Clone().(*model.Config)
	object.Content = utils.NewOrderedMap()
	_, path, err = m.getSharedCodePath(object)
	assert.NoError(t, err)
	assert.Empty(t, path)

	// Path is not string
	object = transformation.Clone().(*model.Config)
	object.Content.Set(model.SharedCodePathContentKey, 123)
	_, path, err = m.getSharedCodePath(object)
	assert.Error(t, err)
	assert.Equal(t, `key "shared_code_path" must be string, found int, in config "branch:123/component:keboola.python-transformation-v2/config:456"`, err.Error())
	assert.Empty(t, path)
}

func TestGetSharedCodeKey(t *testing.T) {
	t.Parallel()
	fs := testhelper.NewMemoryFs()
	state := model.NewState(zap.NewNop().Sugar(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	m := NewMapper(nil, model.MapperContext{State: state})

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
	_, key, err := m.getSharedCodeKey(transformation)
	assert.NoError(t, err)
	assert.Equal(t, model.ConfigKey{
		BranchId:    123,
		ComponentId: model.SharedCodeComponentId,
		Id:          `789`,
	}, key)

	// Not config
	row := &model.ConfigRow{}
	_, key, err = m.getSharedCodeKey(row)
	assert.NoError(t, err)
	assert.Nil(t, key)

	// Not transformation
	object := transformation.Clone().(*model.Config)
	object.ComponentId = `foo.bar`
	_, key, err = m.getSharedCodeKey(object)
	assert.NoError(t, err)
	assert.Nil(t, key)

	// No ID
	object = transformation.Clone().(*model.Config)
	object.Content = utils.NewOrderedMap()
	_, key, err = m.getSharedCodeKey(object)
	assert.NoError(t, err)
	assert.Empty(t, key)

	// ID is not string
	object = transformation.Clone().(*model.Config)
	object.Content.Set(model.SharedCodeIdContentKey, 123)
	_, key, err = m.getSharedCodeKey(object)
	assert.Error(t, err)
	assert.Equal(t, `key "shared_code_id" must be string, found int, in config "branch:123/component:keboola.python-transformation-v2/config:456"`, err.Error())
	assert.Empty(t, key)
}

func TestGetSharedCodeByPath(t *testing.T) {
	t.Parallel()
	fs := testhelper.NewMemoryFs()
	state := model.NewState(zap.NewNop().Sugar(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	naming := model.DefaultNaming()
	m := NewMapper(nil, model.MapperContext{Naming: naming, State: state})
	sharedCodeKey := fixtures.CreateSharedCode(t, state, naming)

	// Found
	result := m.getSharedCodeByPath(model.BranchKey{Id: 123}, `_shared/keboola.python-transformation-v2`)
	assert.NotNil(t, result)
	assert.Equal(t, sharedCodeKey, result.Key())

	// Different branch
	assert.Nil(t, m.getSharedCodeByPath(model.BranchKey{Id: 456}, `_shared/keboola.python-transformation-v2`))

	// Not found
	assert.Nil(t, m.getSharedCodeByPath(model.BranchKey{Id: 123}, `foo/bar`))
}

func TestGetSharedCodeRowByPath(t *testing.T) {
	t.Parallel()
	fs := testhelper.NewMemoryFs()
	state := model.NewState(zap.NewNop().Sugar(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	naming := model.DefaultNaming()
	m := NewMapper(nil, model.MapperContext{Naming: naming, State: state})
	sharedCodeKey := fixtures.CreateSharedCode(t, state, naming)
	sharedCode := state.MustGet(sharedCodeKey).(*model.ConfigState)

	// Found
	result := m.getSharedCodeRowByPath(sharedCode, `codes/code1`)
	assert.NotNil(t, result)
	assert.Equal(t, model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.SharedCodeComponentId,
		ConfigId:    `456`,
		Id:          `1234`,
	}, result.Key())

	// Not found
	assert.Nil(t, m.getSharedCodeRowByPath(sharedCode, `foo/bar`))
}
