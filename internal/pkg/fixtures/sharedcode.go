package fixtures

import (
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func CreateSharedCode(t *testing.T, state model.ObjectStates) (model.ConfigKey, []model.ConfigRowKey) {
	t.Helper()

	// Branch
	branchKey := model.BranchKey{ID: 123}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{
			BranchKey: branchKey,
			Paths:     model.Paths{AbsPath: model.NewAbsPath(``, `branch`)},
		},
		Local:  &model.Branch{BranchKey: branchKey},
		Remote: &model.Branch{BranchKey: branchKey},
	}
	require.NoError(t, state.Set(branchState))

	// Shared code
	sharedCodeKey := model.ConfigKey{
		BranchID:    123,
		ComponentID: keboola.SharedCodeComponentID,
		ID:          `456`,
	}
	sharedCodeState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: sharedCodeKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(`branch`, `_shared/keboola.python-transformation-v2`),
			},
		},
		Local: &model.Config{
			ConfigKey: sharedCodeKey,
			Content:   orderedmap.New(),
			SharedCode: &model.SharedCodeConfig{
				Target: `keboola.python-transformation-v2`,
			},
		},
		Remote: &model.Config{
			ConfigKey: sharedCodeKey,
			Content:   orderedmap.New(),
			SharedCode: &model.SharedCodeConfig{
				Target: `keboola.python-transformation-v2`,
			},
		},
	}
	require.NoError(t, state.Set(sharedCodeState))

	// Shared code row 1
	row1Key := model.ConfigRowKey{
		BranchID:    123,
		ComponentID: keboola.SharedCodeComponentID,
		ConfigID:    `456`,
		ID:          `1234`,
	}
	row1State := &model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{
			ConfigRowKey: row1Key,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(`branch/_shared/keboola.python-transformation-v2`, `codes/code1`),
			},
		},
		Local:  &model.ConfigRow{ConfigRowKey: row1Key, Content: orderedmap.New()},
		Remote: &model.ConfigRow{ConfigRowKey: row1Key, Content: orderedmap.New()},
	}
	require.NoError(t, state.Set(row1State))

	// Shared code row 2
	row2Key := model.ConfigRowKey{
		BranchID:    123,
		ComponentID: keboola.SharedCodeComponentID,
		ConfigID:    `456`,
		ID:          `5678`,
	}
	row2State := &model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{
			ConfigRowKey: row2Key,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(`branch/_shared/keboola.python-transformation-v2`, `codes/code2`),
			},
		},
		Local:  &model.ConfigRow{ConfigRowKey: row2Key, Content: orderedmap.New()},
		Remote: &model.ConfigRow{ConfigRowKey: row2Key, Content: orderedmap.New()},
	}
	require.NoError(t, state.Set(row2State))
	return sharedCodeKey, []model.ConfigRowKey{row1Key, row2Key}
}
