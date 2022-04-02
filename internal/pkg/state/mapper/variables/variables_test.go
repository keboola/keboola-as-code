package variables_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper/variables"
)

func createLocalStateWithMapper(t *testing.T) (*local.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyLocalState()
	mockedState.Mapper().AddMapper(variables.NewLocalMapper(mockedState, d))
	return mockedState, d
}

func createRemoteStateWithMapper(t *testing.T) (*remote.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyRemoteState()
	mockedState.Mapper().AddMapper(variables.NewRemoteMapper(mockedState))
	return mockedState, d
}

func createTestObjectForPersist(t *testing.T, state *local.State) (row1, row2, row3 *model.ConfigRow) {
	t.Helper()

	// Branch
	state.MustAdd(&model.Branch{
		BranchKey: model.BranchKey{BranchId: 123},
	})

	// Config using variables
	state.MustAdd(&model.Config{
		ConfigKey: model.ConfigKey{
			BranchId:    123,
			ComponentId: `foo.bar`,
			ConfigId:    `789`,
		},
		Relations: model.Relations{
			&model.VariablesFromRelation{
				VariablesId: `456`,
			},
		},
	})

	// Variables config
	state.MustAdd(&model.Config{
		ConfigKey: model.ConfigKey{
			BranchId:    123,
			ComponentId: model.VariablesComponentId,
			ConfigId:    `456`,
		},
		Relations: model.Relations{
			&model.VariablesForRelation{
				ComponentId: `foo.bar`,
				ConfigId:    `789`,
			},
		},
	})

	// Row 1
	row1 = &model.ConfigRow{
		ConfigRowKey: model.ConfigRowKey{
			BranchId:    123,
			ComponentId: model.VariablesComponentId,
			ConfigId:    `456`,
			ConfigRowId: `1`,
		},
		Name: `first`,
	}
	state.MustAdd(row1)

	// Row 2
	row2 = &model.ConfigRow{
		ConfigRowKey: model.ConfigRowKey{
			BranchId:    123,
			ComponentId: model.VariablesComponentId,
			ConfigId:    `456`,
			ConfigRowId: `2`,
		},
		Name: `second`,
	}
	state.MustAdd(row2)

	// Row 3
	row3 = &model.ConfigRow{
		ConfigRowKey: model.ConfigRowKey{
			BranchId:    123,
			ComponentId: model.VariablesComponentId,
			ConfigId:    `456`,
			ConfigRowId: `3`,
		},
		Name: `third`,
	}
	state.MustAdd(row3)

	return row1, row2, row3
}
