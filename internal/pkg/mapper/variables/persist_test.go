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
	event := model.OnObjectsPersistEvent{
		PersistedObjects: context.State.LocalObjects().All(),
		AllObjects:       context.State.LocalObjects(),
	}
	assert.NoError(t, NewMapper(context).OnObjectsPersist(event))

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
	event := model.OnObjectsPersistEvent{
		PersistedObjects: context.State.LocalObjects().All(),
		AllObjects:       context.State.LocalObjects(),
	}
	assert.NoError(t, NewMapper(context).OnObjectsPersist(event))

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

	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.VariablesComponentId,
		Id:          `456`,
	}
	row1Key := model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.VariablesComponentId,
		ConfigId:    `456`,
		Id:          `1`,
	}
	row2Key := model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.VariablesComponentId,
		ConfigId:    `456`,
		Id:          `2`,
	}
	row3Key := model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.VariablesComponentId,
		ConfigId:    `456`,
		Id:          `3`,
	}

	configRelations := model.Relations{
		&model.VariablesForRelation{
			ComponentId: `foo.bar`,
			ConfigId:    `789`,
		},
	}

	config := &model.Config{
		ConfigKey: configKey,
		Relations: configRelations,
	}
	row1 := &model.ConfigRow{
		ConfigRowKey: row1Key,
		Name:         `first`,
	}
	row2 := &model.ConfigRow{
		ConfigRowKey: row2Key,
		Name:         `second`,
	}
	row3 := &model.ConfigRow{
		ConfigRowKey: row3Key,
		Name:         `third`,
	}

	configManifest := &model.ConfigManifest{
		ConfigKey: configKey,
		Relations: configRelations,
	}
	row1Manifest := &model.ConfigRowManifest{
		ConfigRowKey: row1Key,
	}
	row2Manifest := &model.ConfigRowManifest{
		ConfigRowKey: row2Key,
	}
	row3Manifest := &model.ConfigRowManifest{
		ConfigRowKey: row3Key,
	}

	configState, err := state.CreateFrom(configManifest)
	assert.NoError(t, err)
	configState.SetLocalState(config)
	row1State, err := state.CreateFrom(row1Manifest)
	assert.NoError(t, err)
	row1State.SetLocalState(row1)
	row2State, err := state.CreateFrom(row2Manifest)
	assert.NoError(t, err)
	row2State.SetLocalState(row2)
	row3State, err := state.CreateFrom(row3Manifest)
	assert.NoError(t, err)
	row3State.SetLocalState(row3)
}
