package variables_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/variables"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func createStateWithMapper(t *testing.T) (*state.State, dependencies.Mocked) {
	t.Helper()
	d, _ := dependencies.NewMocked(t, context.Background())
	mockedState := d.MockedState()
	mockedState.Mapper().AddMapper(variables.NewMapper(mockedState))
	return mockedState, d
}

func createTestObjectForPersist(t *testing.T, state model.ObjectStates) {
	t.Helper()

	// Config
	configKey := model.ConfigKey{
		BranchID:    123,
		ComponentID: keboola.VariablesComponentID,
		ID:          `456`,
	}
	configRelations := model.Relations{
		&model.VariablesForRelation{
			ComponentID: `foo.bar`,
			ConfigID:    `789`,
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
		BranchID:    123,
		ComponentID: keboola.VariablesComponentID,
		ConfigID:    `456`,
		ID:          `1`,
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
		BranchID:    123,
		ComponentID: keboola.VariablesComponentID,
		ConfigID:    `456`,
		ID:          `2`,
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
		BranchID:    123,
		ComponentID: keboola.VariablesComponentID,
		ConfigID:    `456`,
		ID:          `3`,
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
