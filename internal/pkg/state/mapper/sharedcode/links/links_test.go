package links_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper/sharedcode/links"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func createStateWithLocalMapper(t *testing.T) (*local.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyLocalState()
	mockedState.Mapper().AddMapper(links.NewLocalMapper(mockedState, d))
	return mockedState, d
}

func createStateWithRemoteMapper(t *testing.T) (*remote.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyRemoteState()
	mockedState.Mapper().AddMapper(links.NewRemoteMapper(mockedState, d))
	return mockedState, d
}

func createLocalTransformationWithSharedCode(t *testing.T, state *local.State) *model.Config {
	t.Helper()

	// Branch
	state.MustAdd(&model.Branch{
		BranchKey: model.BranchKey{Id: 123},
	})

	// Transformation
	transformationKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: `keboola.python-transformation-v2`,
		Id:          `789`,
	}
	blockKey := model.BlockKey{
		Parent: transformationKey,
		Index:  0,
	}
	transformation := &model.Config{
		ConfigKey: transformationKey,
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{
				Key:   model.SharedCodePathContentKey,
				Value: `_shared/keboola.python-transformation-v2`,
			},
		}),
		Transformation: &model.Transformation{
			Blocks: []*model.Block{
				{
					BlockKey: blockKey,
					Name:     `Block 1`,
					Codes: model.Codes{
						{
							CodeKey: model.CodeKey{
								Parent: blockKey,
								Index:  0,
							},
							Name: `Code 1`,
							Scripts: model.Scripts{
								model.StaticScript{Value: `print(100)`},
								model.StaticScript{Value: "# {{:codes/code1}}\n"},
							},
						},
						{
							CodeKey: model.CodeKey{
								Parent: blockKey,
								Index:  1,
							},
							Name: `Code 2`,
							Scripts: model.Scripts{
								model.StaticScript{Value: " {{:codes/code2}}\n"},
								model.StaticScript{Value: "#     {{:codes/code1}}"},
							},
						},
					},
				},
			},
		},
	}
	state.MustAdd(transformation)
	return transformation
}

func createRemoteTransformationWithSharedCode(t *testing.T, sharedCodeKey model.ConfigKey, sharedCodeRowsKeys []model.ConfigRowKey, state *remote.State) *model.Config {
	t.Helper()

	// Rows -> rows IDs
	var rows []interface{}
	for _, row := range sharedCodeRowsKeys {
		rows = append(rows, row.Id.String())
	}

	// Branch
	state.MustAdd(&model.Branch{
		BranchKey: model.BranchKey{Id: 123},
	})

	// Transformation
	transformationKey := model.ConfigKey{
		BranchId:    sharedCodeKey.BranchId,
		ComponentId: model.ComponentId("keboola.python-transformation-v2"),
		Id:          model.ConfigId("001"),
	}
	transformation := &model.Config{
		ConfigKey: transformationKey,
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{Key: model.SharedCodeIdContentKey, Value: sharedCodeKey.Id.String()},
			{Key: model.SharedCodeRowsIdContentKey, Value: rows},
		}),
		Transformation: &model.Transformation{},
	}
	state.MustAdd(transformation)
	return transformation
}

func createInternalTransformationWithSharedCode(t *testing.T, sharedCodeKey model.ConfigKey, sharedCodeRowsKeys []model.ConfigRowKey, state model.Objects) *model.Config {
	t.Helper()

	// Branch
	state.MustAdd(&model.Branch{
		BranchKey: model.BranchKey{Id: 123},
	})

	// Transformation
	transformationKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: `keboola.python-transformation-v2`,
		Id:          `789`,
	}
	blockKey := model.BlockKey{
		Parent: transformationKey,
		Index:  0,
	}
	transformation := &model.Config{
		ConfigKey: transformationKey,
		Content:   orderedmap.New(),
		Transformation: &model.Transformation{
			LinkToSharedCode: &model.LinkToSharedCode{
				Config: sharedCodeKey,
				Rows:   sharedCodeRowsKeys,
			},
			Blocks: []*model.Block{
				{
					BlockKey: blockKey,
					Name:     `Block 1`,
					Codes: model.Codes{
						{
							CodeKey: model.CodeKey{
								Parent: blockKey,
								Index:  0,
							},
							Name: `Code 1`,
							Scripts: model.Scripts{
								model.StaticScript{Value: `print(100)`},
								model.LinkScript{Target: sharedCodeRowsKeys[0]},
							},
						},
						{
							CodeKey: model.CodeKey{
								Parent: blockKey,
								Index:  1,
							},
							Name: `Code 2`,
							Scripts: model.Scripts{
								model.LinkScript{Target: sharedCodeRowsKeys[1]},
								model.LinkScript{Target: sharedCodeRowsKeys[0]},
							},
						},
					},
				},
			},
		},
	}
	state.MustAdd(transformation)
	return transformation
}
