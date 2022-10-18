package links_test

import (
	"context"
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestLocalLoadTranWithSharedCode(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Create transformation with shared code
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, state)
	transformation := createLocalTranWithSharedCode(t, state)

	// Invoke
	changes := model.NewLocalChanges()
	changes.AddLoaded(transformation)
	assert.NoError(t, state.Mapper().AfterLocalOperation(context.Background(), changes))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Paths in transformation blocks are replaced by IDs
	assert.Equal(t, []*model.Block{
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
						model.LinkScript{
							Target: model.ConfigRowKey{
								BranchId:    123,
								ComponentId: `keboola.shared-code`,
								ConfigId:    `456`,
								Id:          `1234`,
							},
						},
					},
				},
				{
					CodeKey: model.CodeKey{
						ComponentId: `keboola.python-transformation-v2`,
					},
					Name:    `Code 2`,
					AbsPath: model.NewAbsPath(`branch/transformation/blocks/block-1`, `code-2`),
					Scripts: model.Scripts{
						model.LinkScript{
							Target: model.ConfigRowKey{
								BranchId:    123,
								ComponentId: `keboola.shared-code`,
								ConfigId:    `456`,
								Id:          `5678`,
							},
						},
						model.LinkScript{
							Target: model.ConfigRowKey{
								BranchId:    123,
								ComponentId: `keboola.shared-code`,
								ConfigId:    `456`,
								Id:          `1234`,
							},
						},
					},
				},
			},
		},
	}, transformation.Local.Transformation.Blocks)

	// Values from content are converted to struct
	assert.Equal(t, &model.LinkToSharedCode{Config: sharedCodeKey, Rows: sharedCodeRowsKeys}, transformation.Local.Transformation.LinkToSharedCode)

	// Key from Content is deleted
	_, found := transformation.Local.Content.Get(model.SharedCodePathContentKey)
	assert.False(t, found)
}

func TestLocalLoadTranWithSharedCode_InvalidSharedCodePath(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Create transformation with shared code
	fixtures.CreateSharedCode(t, state)
	transformation := createLocalTranWithSharedCode(t, state)
	transformation.Local.Content.Set(model.SharedCodePathContentKey, `missing`) // <<<<<<<<<<<

	// Invoke
	changes := model.NewLocalChanges()
	changes.AddLoaded(transformation)
	expectedErr := `
missing shared code "branch/missing":
  - referenced from config "branch:123/component:keboola.python-transformation-v2/config:789"
`
	err := state.Mapper().AfterLocalOperation(context.Background(), changes)
	assert.Error(t, err)
	assert.Equal(t, strings.TrimSpace(expectedErr), err.Error())
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Link to shared code is not set
	assert.Nil(t, transformation.Local.Transformation.LinkToSharedCode)

	// Key from Content is deleted
	_, found := transformation.Local.Content.Get(model.SharedCodePathContentKey)
	assert.False(t, found)
}

func TestLocalLoadTranWithSharedCode_InvalidSharedCodeRowPath(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Create transformation with shared code
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, state)
	transformation := createLocalTranWithSharedCode(t, state)
	transformation.Local.Transformation.Blocks[0].Codes[1].Scripts[0] = model.StaticScript{Value: "# {{:codes/missing}}\n"} // <<<<<<<<<<<<

	// Invoke
	changes := model.NewLocalChanges()
	changes.AddLoaded(transformation)
	expectedErr := `
missing shared code "branch/_shared/keboola.python-transformation-v2/codes/missing":
  - referenced from "branch/transformation/blocks/block-1/code-2"
`
	err := state.Mapper().AfterLocalOperation(context.Background(), changes)
	assert.Error(t, err)
	assert.Equal(t, strings.TrimSpace(expectedErr), err.Error())
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Link to shared code is set, but without invalid row
	assert.Equal(t, &model.LinkToSharedCode{Config: sharedCodeKey, Rows: []model.ConfigRowKey{sharedCodeRowsKeys[0]}}, transformation.Local.Transformation.LinkToSharedCode)

	// Key from Content is deleted
	_, found := transformation.Local.Content.Get(model.SharedCodePathContentKey)
	assert.False(t, found)
}

func createStateWithMapper(t *testing.T) (*state.State, dependencies.Mocked) {
	t.Helper()
	d := dependencies.NewMockedDeps()
	mockedState := d.MockedState()
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
