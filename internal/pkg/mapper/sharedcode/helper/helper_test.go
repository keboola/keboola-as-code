package helper_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/helper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testdeps"
)

func TestGetSharedCodeByPath(t *testing.T) {
	t.Parallel()
	d := testdeps.New()
	mockedState := d.EmptyState()

	sharedCodeKey, _ := fixtures.CreateSharedCode(t, mockedState)
	helper := New(mockedState)

	// Found
	result, err := helper.GetSharedCodeByPath(model.BranchKey{Id: 123}, `_shared/keboola.python-transformation-v2`)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, sharedCodeKey, result.Key())

	// Different branch
	result, err = helper.GetSharedCodeByPath(model.BranchKey{Id: 456}, `_shared/keboola.python-transformation-v2`)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Equal(t, `missing branch "456"`, err.Error())

	// Not found
	result, err = helper.GetSharedCodeByPath(model.BranchKey{Id: 123}, `foo/bar`)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Equal(t, `missing shared code "branch/foo/bar"`, err.Error())
}

func TestGetSharedCodeRowByPath(t *testing.T) {
	t.Parallel()

	d := testdeps.New()
	mockedState := d.EmptyState()

	sharedCodeKey, _ := fixtures.CreateSharedCode(t, mockedState)
	helper := New(mockedState)
	sharedCode := mockedState.MustGet(sharedCodeKey).(*model.ConfigState)

	// Found
	result, err := helper.GetSharedCodeRowByPath(sharedCode, `codes/code1`)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.SharedCodeComponentId,
		ConfigId:    `456`,
		Id:          `1234`,
	}, result.Key())

	// Not found
	result, err = helper.GetSharedCodeRowByPath(sharedCode, `foo/bar`)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Equal(t, `missing shared code "branch/_shared/keboola.python-transformation-v2/foo/bar"`, err.Error())
}

func TestGetSharedCodeVariablesId(t *testing.T) {
	t.Parallel()

	d := testdeps.New()
	mockedState := d.EmptyState()

	fixtures.CreateSharedCode(t, mockedState)
	helper := New(mockedState)

	sharedCodeRow1 := mockedState.MustGet(model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.SharedCodeComponentId,
		ConfigId:    `456`,
		Id:          `1234`,
	}).(*model.ConfigRowState)
	sharedCodeRow2 := mockedState.MustGet(model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.SharedCodeComponentId,
		ConfigId:    `456`,
		Id:          `5678`,
	}).(*model.ConfigRowState)

	sharedCodeRow1.Local.Content.Set(model.SharedCodeVariablesIdContentKey, `789`)

	// Found
	variablesId, found := helper.GetSharedCodeVariablesId(sharedCodeRow1.Local)
	assert.True(t, found)
	assert.Equal(t, `789`, variablesId)

	// Not found
	_, found = helper.GetSharedCodeVariablesId(sharedCodeRow2.Local)
	assert.False(t, found)
}
