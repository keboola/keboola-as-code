package variables_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestVariablesMapBeforePersist(t *testing.T) {
	t.Parallel()
	state, d := createLocalStateWithMapper(t)
	logger := d.DebugLogger()

	key := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.VariablesComponentId,
		Id:          `678`,
	}
	parentKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: `foo.bar`,
		Id:          `345`,
	}

	recipe := &model.PersistRecipe{
		Key:       key,
		ParentKey: parentKey,
	}

	// Invoke
	assert.Empty(t, recipe.Relations)
	assert.NoError(t, state.Mapper().MapBeforePersist(recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Relation has been created
	assert.Equal(t, model.Relations{
		&model.VariablesForRelation{
			ComponentId: `foo.bar`,
			ConfigId:    `345`,
		},
	}, recipe.Relations)
}

func TestVariablesValuesPersistDefaultInName(t *testing.T) {
	t.Parallel()
	state, d := createLocalStateWithMapper(t)
	logger := d.DebugLogger()

	// Fixtures
	row1, row2, row3 := createTestObjectForPersist(t, state)

	// Row 2 contains "default" in name
	row2.Name = `Foo Default Bar`

	// All rows are without relations
	assert.Empty(t, row1.Relations)
	assert.Empty(t, row2.Relations)
	assert.Empty(t, row3.Relations)

	// Invoke
	assert.NoError(t, state.Mapper().AfterLocalPersist([]model.Object{row1, row2, row3}))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Row 2 has relation -> contains default variables values, because it has "default" in the name
	expectedRelation := model.Relations{
		&model.VariablesValuesForRelation{},
	}
	assert.Empty(t, row1.Relations)
	assert.Equal(t, expectedRelation, row2.Relations)
	assert.Empty(t, row3.Relations)
}

func TestVariablesValuesPersistFirstRowIsDefault(t *testing.T) {
	t.Parallel()
	state, d := createLocalStateWithMapper(t)
	logger := d.DebugLogger()

	// Fixtures
	row1, row2, row3 := createTestObjectForPersist(t, state)

	// All rows are without relations
	assert.Empty(t, row1.Relations)
	assert.Empty(t, row2.Relations)
	assert.Empty(t, row3.Relations)

	// Invoke
	assert.NoError(t, state.Mapper().AfterLocalPersist([]model.Object{row1, row2, row3}))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Row1 has relation -> contains default variables values, because it is first
	expectedRelation := model.Relations{
		&model.VariablesValuesForRelation{},
	}
	assert.Equal(t, expectedRelation, row1.Relations)
	assert.Empty(t, row2.Relations)
	assert.Empty(t, row3.Relations)
}
