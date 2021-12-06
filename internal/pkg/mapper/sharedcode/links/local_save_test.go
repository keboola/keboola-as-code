package links_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestSharedCodeLinksMapBeforeLocalSave(t *testing.T) {
	t.Parallel()
	mapperInst, context, logs := createMapper(t)

	// Shared code config with rows
	fixtures.CreateSharedCode(t, context.State, context.Naming)

	// Config using shared code
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: `keboola.python-transformation-v2`,
		Id:          `789`,
	}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
		},
		Local: &model.Config{
			ConfigKey: configKey,
			Content: utils.PairsToOrderedMap([]utils.Pair{
				{
					Key:   model.SharedCodeIdContentKey,
					Value: `456`,
				},
				{
					Key:   model.SharedCodeRowsIdContentKey,
					Value: []string{`1234`, `5678`},
				},
			}),
			Blocks: model.Blocks{
				{
					Name: `Block 1`,
					Codes: model.Codes{
						{
							CodeKey: model.CodeKey{
								ComponentId: `keboola.python-transformation-v2`,
							},
							Name: `Code 1`,
							Scripts: []string{
								`print(100)`,
								" {{1234}}\n",
							},
						},
						{
							CodeKey: model.CodeKey{
								ComponentId: `keboola.python-transformation-v2`,
							},
							Name: `Code 2`,
							Scripts: []string{
								" {{5678}}\n",
								"{{1234}}",
							},
						},
					},
				},
			},
		},
	}
	assert.NoError(t, context.State.Set(configState))

	// Invoke
	recipe := fixtures.NewLocalSaveRecipe(configState.Manifest(), configState.Local)
	assert.NoError(t, mapperInst.MapBeforeLocalSave(recipe))
	assert.Empty(t, logs.String())

	// Path is replaced by ID
	configFile, err := recipe.Files.ConfigJsonFile()
	assert.NoError(t, err)
	_, found := configFile.Content.Get(model.SharedCodeIdContentKey)
	assert.False(t, found)
	_, found = configFile.Content.Get(model.SharedCodeRowsIdContentKey)
	assert.False(t, found)
	sharedCodeId, found := configFile.Content.Get(model.SharedCodePathContentKey)
	assert.True(t, found)
	assert.Equal(t, sharedCodeId, `_shared/keboola.python-transformation-v2`)

	// IDs in transformation blocks are replaced by paths
	assert.Equal(t, model.Blocks{
		{
			Name: `Block 1`,
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{
						ComponentId: `keboola.python-transformation-v2`,
					},
					Name: `Code 1`,
					Scripts: []string{
						`print(100)`,
						"# {{:codes/code1}}",
					},
				},
				{
					CodeKey: model.CodeKey{
						ComponentId: `keboola.python-transformation-v2`,
					},
					Name: `Code 2`,
					Scripts: []string{
						"# {{:codes/code2}}",
						"# {{:codes/code1}}",
					},
				},
			},
		},
	}, configState.Local.Blocks)
}
