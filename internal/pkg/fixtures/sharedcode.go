package fixtures

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func CreateSharedCode(t *testing.T, state *model.State, naming *model.Naming) model.ConfigKey {
	t.Helper()

	// Branch
	branchKey := model.BranchKey{Id: 123}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{
			BranchKey: branchKey,
			Paths:     model.Paths{PathInProject: model.NewPathInProject(``, `branch`)},
		},
		Local: &model.Branch{BranchKey: branchKey},
	}
	assert.NoError(t, state.Set(branchState))
	naming.Attach(branchState.Key(), branchState.PathInProject)

	// Shared code
	sharedCodeKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.SharedCodeComponentId,
		Id:          `456`,
	}
	sharedCodeState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: sharedCodeKey,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject(`branch`, `_shared/keboola.python-transformation-v2`),
			},
		},
		Local: &model.Config{
			ConfigKey: sharedCodeKey,
			Content: utils.PairsToOrderedMap([]utils.Pair{
				{Key: model.SharedCodeComponentIdContentKey, Value: `keboola.python-transformation-v2`},
			}),
		},
	}
	assert.NoError(t, state.Set(sharedCodeState))
	naming.Attach(sharedCodeState.Key(), sharedCodeState.PathInProject)

	// Shared code row 1
	row1Key := model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.SharedCodeComponentId,
		ConfigId:    `456`,
		Id:          `1234`,
	}
	row1State := &model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{
			ConfigRowKey: row1Key,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject(`branch/_shared/keboola.python-transformation-v2`, `codes/code1`),
			},
		},
		Local: &model.ConfigRow{ConfigRowKey: row1Key, Content: utils.NewOrderedMap()},
	}
	assert.NoError(t, state.Set(row1State))
	naming.Attach(row1State.Key(), row1State.PathInProject)

	// Shared code row 2
	row2Key := model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.SharedCodeComponentId,
		ConfigId:    `456`,
		Id:          `5678`,
	}
	row2State := &model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{
			ConfigRowKey: row2Key,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject(`branch/_shared/keboola.python-transformation-v2`, `codes/code2`),
			},
		},
		Local: &model.ConfigRow{ConfigRowKey: row2Key, Content: utils.NewOrderedMap()},
	}
	assert.NoError(t, state.Set(row2State))
	naming.Attach(row2State.Key(), row2State.PathInProject)

	return sharedCodeKey
}
