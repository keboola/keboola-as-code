package links_test

import (
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/links"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func createStateWithMapper(t *testing.T) (*state.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyState()
	mockedState.Mapper().AddMapper(links.NewMapper(mockedState))
	return mockedState, d
}

func createLocalTranWithSharedCode(t *testing.T, state *state.State) *model.ConfigState {
	t.Helper()

	key := model.ConfigKey{
		BranchId:    123,
		ComponentId: `keboola.python-transformation-v2`,
		Id:          `789`,
	}

	transformation := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: key,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(`branch`, `transformation`),
			},
		},
		Local: &model.Config{
			ConfigKey: key,
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key:   model.SharedCodePathContentKey,
					Value: `_shared/keboola.python-transformation-v2`,
				},
			}),
			Transformation: &model.Transformation{
				Blocks: []*model.Block{
					{
						Name:    `Block 1`,
						AbsPath: model.NewAbsPath(`branch/transformation/blocks`, `block-1`),
						Codes: model.Codes{
							{
								CodeKey: model.CodeKey{
									ComponentId: `keboola.python-transformation-v2`,
								},
								Name:    `Code 1`,
								AbsPath: model.NewAbsPath(`branch/transformation/blocks/block-1`, `code-1`),
								Scripts: model.Scripts{
									model.StaticScript{Value: `print(100)`},
									model.StaticScript{Value: "# {{:codes/code1}}\n"},
								},
							},
							{
								CodeKey: model.CodeKey{
									ComponentId: `keboola.python-transformation-v2`,
								},
								Name:    `Code 2`,
								AbsPath: model.NewAbsPath(`branch/transformation/blocks/block-1`, `code-2`),
								Scripts: model.Scripts{
									model.StaticScript{Value: " {{:codes/code2}}\n"},
									model.StaticScript{Value: "#     {{:codes/code1}}"},
								},
							},
						},
					},
				},
			},
		},
	}
	assert.NoError(t, state.Set(transformation))
	return transformation
}

func createInternalTranWithSharedCode(t *testing.T, sharedCodeKey model.ConfigKey, sharedCodeRowsKeys []model.ConfigRowKey, state *state.State) *model.ConfigState {
	t.Helper()

	key := model.ConfigKey{
		BranchId:    123,
		ComponentId: `keboola.python-transformation-v2`,
		Id:          `789`,
	}

	transformation := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: key,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(`branch`, `transformation`),
			},
		},
		Local: &model.Config{
			ConfigKey: key,
			Content:   orderedmap.New(),
			Transformation: &model.Transformation{
				LinkToSharedCode: &model.LinkToSharedCode{
					Config: sharedCodeKey,
					Rows:   sharedCodeRowsKeys,
				},
				Blocks: []*model.Block{
					{
						Name: `Block 1`,
						Codes: model.Codes{
							{
								CodeKey: model.CodeKey{
									ComponentId: `keboola.python-transformation-v2`,
								},
								Name: `Code 1`,
								Scripts: model.Scripts{
									model.StaticScript{Value: `print(100)`},
									model.LinkScript{Target: sharedCodeRowsKeys[0]},
								},
								AbsPath: model.NewAbsPath(`branch/transformation/blocks/block-1`, `code-1`),
							},
							{
								CodeKey: model.CodeKey{
									ComponentId: `keboola.python-transformation-v2`,
								},
								Name: `Code 2`,
								Scripts: model.Scripts{
									model.LinkScript{Target: sharedCodeRowsKeys[1]},
									model.LinkScript{Target: sharedCodeRowsKeys[0]},
								},
								AbsPath: model.NewAbsPath(`branch/transformation/blocks/block-1`, `code-2`),
							},
						},
						AbsPath: model.NewAbsPath(`branch/transformation/blocks`, `block-1`),
					},
				},
			},
		},
	}

	assert.NoError(t, state.Set(transformation))
	return transformation
}

func createRemoteTranWithSharedCode(t *testing.T, sharedCodeKey model.ConfigKey, sharedCodeRowsKeys []model.ConfigRowKey, state *state.State) *model.ConfigState {
	t.Helper()

	// Rows -> rows IDs
	var rows []interface{}
	for _, row := range sharedCodeRowsKeys {
		rows = append(rows, row.Id.String())
	}

	key := model.ConfigKey{
		BranchId:    sharedCodeKey.BranchId,
		ComponentId: storageapi.ComponentID("keboola.python-transformation-v2"),
		Id:          storageapi.ConfigID("001"),
	}

	transformation := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: key,
		},
		Remote: &model.Config{
			ConfigKey: key,
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: model.SharedCodeIdContentKey, Value: sharedCodeKey.Id.String()},
				{Key: model.SharedCodeRowsIdContentKey, Value: rows},
			}),
			Transformation: &model.Transformation{},
		},
	}

	assert.NoError(t, state.Set(transformation))
	return transformation
}
