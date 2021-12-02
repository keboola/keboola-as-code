package variables_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/variables"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestVariablesMapBeforePersist(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)

	parentKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: `foo.bar`,
		Id:          `345`,
	}
	configManifest := &model.ConfigManifest{
		ConfigKey: model.ConfigKey{
			BranchId:    123,
			ComponentId: model.VariablesComponentId,
			Id:          `678`,
		},
	}
	recipe := &model.PersistRecipe{
		ParentKey: parentKey,
		Manifest:  configManifest,
	}

	// Invoke
	assert.Empty(t, configManifest.Relations)
	assert.NoError(t, NewMapper(context).MapBeforePersist(recipe))

	// Relation has been created
	assert.Equal(t, model.Relations{
		&model.VariablesForRelation{
			ComponentId: `foo.bar`,
			ConfigId:    `345`,
		},
	}, configManifest.Relations)
}

func TestVariablesValuesPersistDefaultInName(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)
	createTestObjectForPersist(t, context.State)

	// Get objects
	rowKey := model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.VariablesComponentId,
		ConfigId:    `456`,
	}
	row1Key := rowKey
	row1Key.Id = `1`
	row2Key := rowKey
	row2Key.Id = `2`
	row3Key := rowKey
	row3Key.Id = `3`
	row1 := context.State.MustGet(row1Key).(*model.ConfigRowState)
	row2 := context.State.MustGet(row2Key).(*model.ConfigRowState)
	row3 := context.State.MustGet(row3Key).(*model.ConfigRowState)

	// Row 2 contains "default" in name
	row2.Local.Name = `Foo Default Bar`

	// All rows are without relations
	assert.Empty(t, row1.Local.Relations)
	assert.Empty(t, row1.ConfigRowManifest.Relations)
	assert.Empty(t, row2.Local.Relations)
	assert.Empty(t, row2.ConfigRowManifest.Relations)
	assert.Empty(t, row3.Local.Relations)
	assert.Empty(t, row3.ConfigRowManifest.Relations)

	// Invoke
	changes := model.NewLocalChanges()
	changes.AddPersisted(context.State.All()...)
	assert.NoError(t, NewMapper(context).OnLocalChange(changes))

	// Row 2 has relation -> contains default variables values, because it has "default" in the name
	expectedRelation := model.Relations{
		&model.VariablesValuesForRelation{},
	}
	assert.Empty(t, row1.Local.Relations)
	assert.Empty(t, row1.ConfigRowManifest.Relations)
	assert.Equal(t, expectedRelation, row2.Local.Relations)
	assert.Equal(t, expectedRelation, row2.ConfigRowManifest.Relations)
	assert.Empty(t, row3.Local.Relations)
	assert.Empty(t, row3.ConfigRowManifest.Relations)
}

func TestVariablesValuesPersistFirstRowIsDefault(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)
	createTestObjectForPersist(t, context.State)

	// Get objects
	rowKey := model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.VariablesComponentId,
		ConfigId:    `456`,
	}
	row1Key := rowKey
	row1Key.Id = `1`
	row2Key := rowKey
	row2Key.Id = `2`
	row3Key := rowKey
	row3Key.Id = `3`
	row1 := context.State.MustGet(row1Key).(*model.ConfigRowState)
	row2 := context.State.MustGet(row2Key).(*model.ConfigRowState)
	row3 := context.State.MustGet(row3Key).(*model.ConfigRowState)

	// All rows are without relations
	assert.Empty(t, row1.Local.Relations)
	assert.Empty(t, row1.ConfigRowManifest.Relations)
	assert.Empty(t, row2.Local.Relations)
	assert.Empty(t, row2.ConfigRowManifest.Relations)
	assert.Empty(t, row3.Local.Relations)
	assert.Empty(t, row3.ConfigRowManifest.Relations)

	// Invoke
	changes := model.NewLocalChanges()
	changes.AddPersisted(context.State.All()...)
	assert.NoError(t, NewMapper(context).OnLocalChange(changes))

	// Row1 has relation -> contains default variables values, because it is first
	expectedRelation := model.Relations{
		&model.VariablesValuesForRelation{},
	}
	assert.Equal(t, expectedRelation, row1.Local.Relations)
	assert.Equal(t, expectedRelation, row1.ConfigRowManifest.Relations)
	assert.Empty(t, row2.Local.Relations)
	assert.Empty(t, row2.ConfigRowManifest.Relations)
	assert.Empty(t, row3.Local.Relations)
	assert.Empty(t, row3.ConfigRowManifest.Relations)
}

func createTestObjectForPersist(t *testing.T, state *model.State) {
	t.Helper()

	// Config
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.VariablesComponentId,
		Id:          `456`,
	}
	configRelations := model.Relations{
		&model.VariablesForRelation{
			ComponentId: `foo.bar`,
			ConfigId:    `789`,
		},
	}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
			Relations: configRelations,
		},
		Local: &model.Config{
			ConfigKey: configKey,
			Relations: configRelations,
		},
	}
	assert.NoError(t, state.Set(configState))

	// Row 1
	row1Key := model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.VariablesComponentId,
		ConfigId:    `456`,
		Id:          `1`,
	}
	row1State := &model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{
			ConfigRowKey: row1Key,
		},
		Local: &model.ConfigRow{
			ConfigRowKey: row1Key,
			Name:         `first`,
		},
	}
	assert.NoError(t, state.Set(row1State))

	// Row 2
	row2Key := model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.VariablesComponentId,
		ConfigId:    `456`,
		Id:          `2`,
	}
	row2State := &model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{
			ConfigRowKey: row2Key,
		},
		Local: &model.ConfigRow{
			ConfigRowKey: row2Key,
			Name:         `second`,
		},
	}
	assert.NoError(t, state.Set(row2State))

	// Row 3
	row3Key := model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.VariablesComponentId,
		ConfigId:    `456`,
		Id:          `3`,
	}
	row3State := &model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{
			ConfigRowKey: row3Key,
		},
		Local: &model.ConfigRow{
			ConfigRowKey: row3Key,
			Name:         `third`,
		},
	}
	assert.NoError(t, state.Set(row3State))
}
