package links_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestLocalSaveTranWithSharedCode(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Shared code config with rows
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, state)

	// Create transformation with shared code
	transformation := createInternalTranWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, state)

	// Invoke
	recipe := model.NewLocalSaveRecipe(transformation.ConfigManifest, transformation.Local, model.NewChangedFields())
	require.NoError(t, state.Mapper().MapBeforeLocalSave(t.Context(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// path to shared code is part of the Content
	sharedCodePath, found := transformation.Local.Content.Get(model.SharedCodePathContentKey)
	assert.True(t, found)
	assert.Equal(t, `_shared/keboola.python-transformation-v2`, sharedCodePath)

	// IDs in transformation blocks are replaced by paths
	assert.Equal(t, []*model.Block{
		{
			Name: `Block 1`,
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{
						ComponentID: `keboola.python-transformation-v2`,
					},
					Name: `Code 1`,
					Scripts: model.Scripts{
						model.StaticScript{Value: `print(100)`},
						model.StaticScript{Value: "# {{:codes/code1}}"},
					},
					AbsPath: model.NewAbsPath(`branch/transformation/blocks/block-1`, `code-1`),
				},
				{
					CodeKey: model.CodeKey{
						ComponentID: `keboola.python-transformation-v2`,
					},
					Name: `Code 2`,
					Scripts: model.Scripts{
						model.StaticScript{Value: "# {{:codes/code2}}"},
						model.StaticScript{Value: "# {{:codes/code1}}"},
					},
					AbsPath: model.NewAbsPath(`branch/transformation/blocks/block-1`, `code-2`),
				},
			},
			AbsPath: model.NewAbsPath(`branch/transformation/blocks`, `block-1`),
		},
	}, transformation.Local.Transformation.Blocks)
}

func TestLocalSaveTranWithSharedCode_SharedCodeConfigNotFound(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Shared code config with rows
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, state)

	// Create transformation with shared code
	transformation := createInternalTranWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, state)
	transformation.Local.Transformation.LinkToSharedCode.Config.ID = `missing` // <<<<<<<<<<<

	// Invoke
	recipe := model.NewLocalSaveRecipe(transformation.ConfigManifest, transformation.Local, model.NewChangedFields())
	require.NoError(t, state.Mapper().MapBeforeLocalSave(t.Context(), recipe))
	expectedLogs := `
WARN  Warning:
- Missing shared code config "branch:123/component:keboola.shared-code/config:missing":
  - Referenced from config "branch:123/component:keboola.python-transformation-v2/config:789".
`
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logger.AllMessagesTxt())

	// Config file doesn't contain shared code path
	_, found := transformation.Local.Content.Get(model.SharedCodePathContentKey)
	assert.False(t, found)

	// IDs in transformation blocks are NOT replaced by paths
	assert.Equal(t, []*model.Block{
		{
			Name: `Block 1`,
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{
						ComponentID: `keboola.python-transformation-v2`,
					},
					Name: `Code 1`,
					Scripts: model.Scripts{
						model.StaticScript{Value: `print(100)`},
						model.StaticScript{Value: fmt.Sprintf("{{%s}}", sharedCodeRowsKeys[0].ObjectID())},
					},
					AbsPath: model.NewAbsPath(`branch/transformation/blocks/block-1`, `code-1`),
				},
				{
					CodeKey: model.CodeKey{
						ComponentID: `keboola.python-transformation-v2`,
					},
					Name: `Code 2`,

					Scripts: model.Scripts{
						model.StaticScript{Value: fmt.Sprintf("{{%s}}", sharedCodeRowsKeys[1].ObjectID())},
						model.StaticScript{Value: fmt.Sprintf("{{%s}}", sharedCodeRowsKeys[0].ObjectID())},
					},
					AbsPath: model.NewAbsPath(`branch/transformation/blocks/block-1`, `code-2`),
				},
			},
			AbsPath: model.NewAbsPath(`branch/transformation/blocks`, `block-1`),
		},
	}, transformation.Local.Transformation.Blocks)
}

func TestLocalSaveTranWithSharedCode_SharedCodeRowNotFound(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Shared code config with rows
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, state)

	// Create transformation with shared code
	transformation := createInternalTranWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, state)
	transformation.Local.Transformation.Blocks[0].Codes[1].Scripts[0] = model.LinkScript{Target: model.ConfigRowKey{
		BranchID:    sharedCodeKey.BranchID,
		ComponentID: sharedCodeKey.ComponentID,
		ConfigID:    sharedCodeKey.ID,
		ID:          `missing`, // <<<<<<<<<<<<
	}}

	// Invoke
	recipe := model.NewLocalSaveRecipe(transformation.ConfigManifest, transformation.Local, model.NewChangedFields())
	require.NoError(t, state.Mapper().MapBeforeLocalSave(t.Context(), recipe))
	expectedLogs := `
WARN  Warning:
- Missing shared code config row "branch:123/component:keboola.shared-code/config:456/row:missing":
  - Referenced from branch/transformation/blocks/block-1/code-2.
`
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logger.AllMessagesTxt())

	// Link to shared code is set, but without missing row
	sharedCodeID, found := transformation.Local.Content.Get(model.SharedCodePathContentKey)
	assert.True(t, found)
	assert.Equal(t, `_shared/keboola.python-transformation-v2`, sharedCodeID)

	// IDs in transformation blocks are replaced by paths, except missing row
	assert.Equal(t, []*model.Block{
		{
			Name: `Block 1`,
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{
						ComponentID: `keboola.python-transformation-v2`,
					},
					Name: `Code 1`,
					Scripts: model.Scripts{
						model.StaticScript{Value: `print(100)`},
						model.StaticScript{Value: "# {{:codes/code1}}"},
					},
					AbsPath: model.NewAbsPath(`branch/transformation/blocks/block-1`, `code-1`),
				},
				{
					CodeKey: model.CodeKey{
						ComponentID: `keboola.python-transformation-v2`,
					},
					Name: `Code 2`,
					Scripts: model.Scripts{
						model.StaticScript{Value: "{{missing}}"}, // <<<<<<<<<<<<<<
						model.StaticScript{Value: "# {{:codes/code1}}"},
					},
					AbsPath: model.NewAbsPath(`branch/transformation/blocks/block-1`, `code-2`),
				},
			},
			AbsPath: model.NewAbsPath(`branch/transformation/blocks`, `block-1`),
		},
	}, transformation.Local.Transformation.Blocks)
}
