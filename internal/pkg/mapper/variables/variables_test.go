package variables_test

import (
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/variables"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func createStateWithMapper(t *testing.T) (*state.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyState()
	mockedState.Mapper().AddMapper(variables.NewMapper(mockedState))
	return mockedState, d
}

func createTestObjectForPersist(t *testing.T, state model.ObjectStates) {
	t.Helper()

	// Config
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: storageapi.VariablesComponentID,
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
		ComponentId: storageapi.VariablesComponentID,
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
		ComponentId: storageapi.VariablesComponentID,
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
		ComponentId: storageapi.VariablesComponentID,
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
