package links_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestLocalSaveTranWithSharedCode(t *testing.T) {
	t.Parallel()
	state, d := createStateWithLocalMapper(t)
	logger := d.DebugLogger()

	// Shared code config with rows
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, state)

	// Create transformation with shared code
	transformation := createInternalTransformationWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, state)
	transformationPath, err := state.GetPath(transformation)
	assert.NoError(t, err)

	// Invoke
	recipe := model.NewLocalSaveRecipe(transformationPath, transformation, model.NewChangedFields())
	assert.NoError(t, state.Mapper().MapBeforeLocalSave(recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// path to shared code is part of the Content
	sharedCodePath, found := transformation.Content.Get(model.SharedCodePathContentKey)
	assert.True(t, found)
	assert.Equal(t, sharedCodePath, `_shared/keboola.python-transformation-v2`)

	// IDs in transformation blocks are replaced by paths
	blockKey := model.BlockKey{Parent: transformation.ConfigKey, Index: 0}
	assert.Equal(t, []*model.Block{
		{
			BlockKey: blockKey,
			Name:     `Block 1`,
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{Parent: blockKey, Index: 0},
					Name:    `Code 1`,
					Scripts: model.Scripts{
						model.StaticScript{Value: `print(100)`},
						model.StaticScript{Value: "# {{:codes/code1}}"},
					},
				},
				{
					CodeKey: model.CodeKey{Parent: blockKey, Index: 1},
					Name:    `Code 2`,
					Scripts: model.Scripts{
						model.StaticScript{Value: "# {{:codes/code2}}"},
						model.StaticScript{Value: "# {{:codes/code1}}"},
					},
				},
			},
		},
	}, transformation.Transformation.Blocks)
}

func TestLocalSaveTranWithSharedCode_SharedCodeConfigNotFound(t *testing.T) {
	t.Parallel()
	state, d := createStateWithLocalMapper(t)
	logger := d.DebugLogger()

	// Shared code config with rows
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, state)

	// Create transformation with shared code
	transformation := createInternalTransformationWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, state)
	transformation.Transformation.LinkToSharedCode.Config.Id = `missing` // <<<<<<<<<<<
	transformationPath, err := state.GetPath(transformation)
	assert.NoError(t, err)

	// Invoke
	recipe := model.NewLocalSaveRecipe(transformationPath, transformation, model.NewChangedFields())
	assert.NoError(t, state.Mapper().MapBeforeLocalSave(recipe))
	expectedLogs := `
WARN  Warning:
  - missing shared code config "branch:123/component:keboola.shared-code/config:missing":
    - referenced from config "branch:123/component:keboola.python-transformation-v2/config:789"
`
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logger.AllMessages())

	// Config file doesn't contain shared code path
	_, found := transformation.Content.Get(model.SharedCodePathContentKey)
	assert.False(t, found)

	// IDs in transformation blocks are NOT replaced by paths
	blockKey := model.BlockKey{Parent: transformation.ConfigKey, Index: 0}
	assert.Equal(t, []*model.Block{
		{
			Name: `Block 1`,
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{Parent: blockKey, Index: 0},
					Name:    `Code 1`,
					Scripts: model.Scripts{
						model.StaticScript{Value: `print(100)`},
						model.StaticScript{Value: fmt.Sprintf("{{%s}}", sharedCodeRowsKeys[0].ObjectId())},
					},
				},
				{
					CodeKey: model.CodeKey{Parent: blockKey, Index: 1},
					Name:    `Code 2`,

					Scripts: model.Scripts{
						model.StaticScript{Value: fmt.Sprintf("{{%s}}", sharedCodeRowsKeys[1].ObjectId())},
						model.StaticScript{Value: fmt.Sprintf("{{%s}}", sharedCodeRowsKeys[0].ObjectId())},
					},
				},
			},
		},
	}, transformation.Transformation.Blocks)
}

func TestLocalSaveTranWithSharedCode_SharedCodeRowNotFound(t *testing.T) {
	t.Parallel()
	state, d := createStateWithLocalMapper(t)
	logger := d.DebugLogger()

	// Shared code config with rows
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, state)

	// Create transformation with shared code
	transformation := createInternalTransformationWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, state)
	transformation.Transformation.Blocks[0].Codes[1].Scripts[0] = model.LinkScript{Target: model.ConfigRowKey{
		BranchId:    sharedCodeKey.BranchId,
		ComponentId: sharedCodeKey.ComponentId,
		ConfigId:    sharedCodeKey.Id,
		ConfigRowId: `missing`, // <<<<<<<<<<<<
	}}
	transformationPath, err := state.GetPath(transformation)
	assert.NoError(t, err)

	// Invoke
	recipe := model.NewLocalSaveRecipe(transformationPath, transformation, model.NewChangedFields())
	assert.NoError(t, state.Mapper().MapBeforeLocalSave(recipe))
	expectedLogs := `
WARN  Warning:
  - missing shared code config row "branch:123/component:keboola.shared-code/config:456/row:missing":
    - referenced from branch/transformation/blocks/block-1/code-2
`
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logger.AllMessages())

	// Link to shared code is set, but without missing row
	sharedCodeId, found := transformation.Content.Get(model.SharedCodePathContentKey)
	assert.True(t, found)
	assert.Equal(t, sharedCodeId, `_shared/keboola.python-transformation-v2`)

	// IDs in transformation blocks are replaced by paths, except missing row
	blockKey := model.BlockKey{Parent: transformation.ConfigKey, Index: 0}
	assert.Equal(t, []*model.Block{
		{
			BlockKey: blockKey,
			Name:     `Block 1`,
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{Parent: blockKey, Index: 0},
					Name:    `Code 1`,
					Scripts: model.Scripts{
						model.StaticScript{Value: `print(100)`},
						model.StaticScript{Value: "# {{:codes/code1}}"},
					},
				},
				{
					CodeKey: model.CodeKey{Parent: blockKey, Index: 1},
					Name:    `Code 2`,
					Scripts: model.Scripts{
						model.StaticScript{Value: "{{missing}}"}, // <<<<<<<<<<<<<<
						model.StaticScript{Value: "# {{:codes/code1}}"},
					},
				},
			},
		},
	}, transformation.Transformation.Blocks)
}
