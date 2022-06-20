package variables_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestVariablesMapBeforePersist(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	parentKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: `foo.bar`,
		Id:          `345`,
	}
	configManifest := &model.ConfigManifest{
		ConfigKey: model.ConfigKey{
			BranchId:    123,
			ComponentId: storageapi.VariablesComponentID,
			Id:          `678`,
		},
	}
	recipe := &model.PersistRecipe{
		ParentKey: parentKey,
		Manifest:  configManifest,
	}

	// Invoke
	assert.Empty(t, configManifest.Relations)
	assert.NoError(t, state.Mapper().MapBeforePersist(context.Background(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

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
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	createTestObjectForPersist(t, state)

	// Get objects
	rowKey := model.ConfigRowKey{
		BranchId:    123,
		ComponentId: storageapi.VariablesComponentID,
		ConfigId:    `456`,
	}
	row1Key := rowKey
	row1Key.Id = `1`
	row2Key := rowKey
	row2Key.Id = `2`
	row3Key := rowKey
	row3Key.Id = `3`
	row1 := state.MustGet(row1Key).(*model.ConfigRowState)
	row2 := state.MustGet(row2Key).(*model.ConfigRowState)
	row3 := state.MustGet(row3Key).(*model.ConfigRowState)

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
	changes.AddPersisted(state.All()...)
	assert.NoError(t, state.Mapper().AfterLocalOperation(context.Background(), changes))
	assert.Empty(t, logger.WarnAndErrorMessages())

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
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()
	createTestObjectForPersist(t, state)

	// Get objects
	rowKey := model.ConfigRowKey{
		BranchId:    123,
		ComponentId: storageapi.VariablesComponentID,
		ConfigId:    `456`,
	}
	row1Key := rowKey
	row1Key.Id = `1`
	row2Key := rowKey
	row2Key.Id = `2`
	row3Key := rowKey
	row3Key.Id = `3`
	row1 := state.MustGet(row1Key).(*model.ConfigRowState)
	row2 := state.MustGet(row2Key).(*model.ConfigRowState)
	row3 := state.MustGet(row3Key).(*model.ConfigRowState)

	// All rows are without relations
	assert.Empty(t, row1.Local.Relations)
	assert.Empty(t, row1.ConfigRowManifest.Relations)
	assert.Empty(t, row2.Local.Relations)
	assert.Empty(t, row2.ConfigRowManifest.Relations)
	assert.Empty(t, row3.Local.Relations)
	assert.Empty(t, row3.ConfigRowManifest.Relations)

	// Invoke
	changes := model.NewLocalChanges()
	changes.AddPersisted(state.All()...)
	assert.NoError(t, state.Mapper().AfterLocalOperation(context.Background(), changes))
	assert.Empty(t, logger.WarnAndErrorMessages())

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
