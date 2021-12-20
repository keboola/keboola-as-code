package helper_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/helper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/testfs"
)

func TestGetSharedCodeByPath(t *testing.T) {
	t.Parallel()
	fs := testfs.NewMemoryFs()
	projectState := state.NewRegistry(log.NewNopLogger(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	namingRegistry := naming.NewRegistry()
	h := New(projectState, namingRegistry)
	sharedCodeKey, _ := fixtures.CreateSharedCode(t, projectState, namingRegistry)

	// Found
	result, err := h.GetSharedCodeByPath(model.BranchKey{Id: 123}, `_shared/keboola.python-transformation-v2`)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, sharedCodeKey, result.Key())

	// Different branch
	result, err = h.GetSharedCodeByPath(model.BranchKey{Id: 456}, `_shared/keboola.python-transformation-v2`)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Equal(t, `missing branch "456"`, err.Error())

	// Not found
	result, err = h.GetSharedCodeByPath(model.BranchKey{Id: 123}, `foo/bar`)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Equal(t, `missing shared code "branch/foo/bar"`, err.Error())
}

func TestGetSharedCodeRowByPath(t *testing.T) {
	t.Parallel()
	fs := testfs.NewMemoryFs()
	projectState := state.NewRegistry(log.NewNopLogger(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	namingRegistry := naming.NewRegistry()
	h := New(projectState, namingRegistry)
	sharedCodeKey, _ := fixtures.CreateSharedCode(t, projectState, namingRegistry)
	sharedCode := projectState.MustGet(sharedCodeKey).(*model.ConfigState)

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
	assert.Equal(t, `missing shared code "branch/_shared/keboola.python-transformation-v2/foo/bar"`, err.Error())
}

func TestGetSharedCodeVariablesId(t *testing.T) {
	t.Parallel()
	fs := testfs.NewMemoryFs()
	projectState := state.NewRegistry(log.NewNopLogger(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	namingRegistry := naming.NewRegistry()
	h := New(projectState, namingRegistry)

	fixtures.CreateSharedCode(t, projectState, namingRegistry)
	sharedCodeRow1 := projectState.MustGet(model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.SharedCodeComponentId,
		ConfigId:    `456`,
		Id:          `1234`,
	}).(*model.ConfigRowState)
	sharedCodeRow2 := projectState.MustGet(model.ConfigRowKey{
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
