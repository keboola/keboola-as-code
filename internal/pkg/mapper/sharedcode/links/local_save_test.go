package links_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestLocalSaveTranWithSharedCode(t *testing.T) {
	t.Parallel()
	mapperInst, context, logs := createMapper(t)

	// Shared code config with rows
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, context.State, context.NamingRegistry)

	// Create transformation with shared code
	transformation := createInternalTranWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, context)

	// Invoke
	recipe := fixtures.NewLocalSaveRecipe(transformation.ConfigManifest, transformation.Local)
	assert.NoError(t, mapperInst.MapBeforeLocalSave(recipe))
	assert.Empty(t, logs.AllMessages())

	// Path to shared code is part of the Content
	configFile, err := recipe.Files.ObjectConfigFile()
	assert.NoError(t, err)
	sharedCodePath, found := configFile.Content.Get(model.SharedCodePathContentKey)
	assert.True(t, found)
	assert.Equal(t, sharedCodePath, `_shared/keboola.python-transformation-v2`)

	// IDs in transformation blocks are replaced by paths
	assert.Equal(t, []*model.Block{
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
						model.StaticScript{Value: "# {{:codes/code1}}"},
					},
					PathInProject: model.NewPathInProject(`branch/transformation/blocks/block-1`, `code-1`),
				},
				{
					CodeKey: model.CodeKey{
						ComponentId: `keboola.python-transformation-v2`,
					},
					Name: `Code 2`,
					Scripts: model.Scripts{
						model.StaticScript{Value: "# {{:codes/code2}}"},
						model.StaticScript{Value: "# {{:codes/code1}}"},
					},
					PathInProject: model.NewPathInProject(`branch/transformation/blocks/block-1`, `code-2`),
				},
			},
			PathInProject: model.NewPathInProject(`branch/transformation/blocks`, `block-1`),
		},
	}, transformation.Local.Transformation.Blocks)
}

func TestLocalSaveTranWithSharedCode_SharedCodeConfigNotFound(t *testing.T) {
	t.Parallel()
	mapperInst, context, logs := createMapper(t)

	// Shared code config with rows
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, context.State, context.NamingRegistry)

	// Create transformation with shared code
	transformation := createInternalTranWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, context)
	transformation.Local.Transformation.LinkToSharedCode.Config.Id = `missing` // <<<<<<<<<<<

	// Invoke
	recipe := fixtures.NewLocalSaveRecipe(transformation.ConfigManifest, transformation.Local)
	assert.NoError(t, mapperInst.MapBeforeLocalSave(recipe))
	expectedLogs := `
WARN  Warning:
  - missing shared code config "branch:123/component:keboola.shared-code/config:missing":
    - referenced from config "branch:123/component:keboola.python-transformation-v2/config:789"
`
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logs.AllMessages())

	// Config file doesn't contain shared code path
	configFile, err := recipe.Files.ObjectConfigFile()
	assert.NoError(t, err)
	_, found := configFile.Content.Get(model.SharedCodePathContentKey)
	assert.False(t, found)

	// IDs in transformation blocks are NOT replaced by paths
	assert.Equal(t, []*model.Block{
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
						model.StaticScript{Value: fmt.Sprintf("{{%s}}", sharedCodeRowsKeys[0].ObjectId())},
					},
					PathInProject: model.NewPathInProject(`branch/transformation/blocks/block-1`, `code-1`),
				},
				{
					CodeKey: model.CodeKey{
						ComponentId: `keboola.python-transformation-v2`,
					},
					Name: `Code 2`,

					Scripts: model.Scripts{
						model.StaticScript{Value: fmt.Sprintf("{{%s}}", sharedCodeRowsKeys[1].ObjectId())},
						model.StaticScript{Value: fmt.Sprintf("{{%s}}", sharedCodeRowsKeys[0].ObjectId())},
					},
					PathInProject: model.NewPathInProject(`branch/transformation/blocks/block-1`, `code-2`),
				},
			},
			PathInProject: model.NewPathInProject(`branch/transformation/blocks`, `block-1`),
		},
	}, transformation.Local.Transformation.Blocks)
}

func TestLocalSaveTranWithSharedCode_SharedCodeRowNotFound(t *testing.T) {
	t.Parallel()
	mapperInst, context, logs := createMapper(t)

	// Shared code config with rows
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, context.State, context.NamingRegistry)

	// Create transformation with shared code
	transformation := createInternalTranWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, context)
	transformation.Local.Transformation.Blocks[0].Codes[1].Scripts[0] = model.LinkScript{Target: model.ConfigRowKey{
		BranchId:    sharedCodeKey.BranchId,
		ComponentId: sharedCodeKey.ComponentId,
		ConfigId:    sharedCodeKey.Id,
		Id:          `missing`, // <<<<<<<<<<<<
	}}

	// Invoke
	recipe := fixtures.NewLocalSaveRecipe(transformation.ConfigManifest, transformation.Local)
	assert.NoError(t, mapperInst.MapBeforeLocalSave(recipe))
	expectedLogs := `
WARN  Warning:
  - missing shared code config row "branch:123/component:keboola.shared-code/config:456/row:missing":
    - referenced from branch/transformation/blocks/block-1/code-2
`
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logs.AllMessages())

	// Link to shared code is set, but without missing row
	configFile, err := recipe.Files.ObjectConfigFile()
	assert.NoError(t, err)
	sharedCodeId, found := configFile.Content.Get(model.SharedCodePathContentKey)
	assert.True(t, found)
	assert.Equal(t, sharedCodeId, `_shared/keboola.python-transformation-v2`)

	// IDs in transformation blocks are replaced by paths, except missing row
	assert.Equal(t, []*model.Block{
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
						model.StaticScript{Value: "# {{:codes/code1}}"},
					},
					PathInProject: model.NewPathInProject(`branch/transformation/blocks/block-1`, `code-1`),
				},
				{
					CodeKey: model.CodeKey{
						ComponentId: `keboola.python-transformation-v2`,
					},
					Name: `Code 2`,
					Scripts: model.Scripts{
						model.StaticScript{Value: "{{missing}}"}, // <<<<<<<<<<<<<<
						model.StaticScript{Value: "# {{:codes/code1}}"},
					},
					PathInProject: model.NewPathInProject(`branch/transformation/blocks/block-1`, `code-2`),
				},
			},
			PathInProject: model.NewPathInProject(`branch/transformation/blocks`, `block-1`),
		},
	}, transformation.Local.Transformation.Blocks)
}

func createInternalTranWithSharedCode(t *testing.T, sharedCodeKey model.ConfigKey, sharedCodeRowsKeys []model.ConfigRowKey, context mapper.Context) *model.ConfigState {
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
				PathInProject: model.NewPathInProject(`branch`, `transformation`),
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
								PathInProject: model.NewPathInProject(`branch/transformation/blocks/block-1`, `code-1`),
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
								PathInProject: model.NewPathInProject(`branch/transformation/blocks/block-1`, `code-2`),
							},
						},
						PathInProject: model.NewPathInProject(`branch/transformation/blocks`, `block-1`),
					},
				},
			},
		},
	}

	assert.NoError(t, context.State.Set(transformation))
	return transformation
}
