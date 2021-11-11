package links_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestSharedCodeLinksAfterLocalLoad(t *testing.T) {
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
			Paths: model.Paths{
				PathInProject: model.NewPathInProject(`branch`, `transformation`),
			},
		},
		Local: &model.Config{
			ConfigKey: configKey,
			Content: utils.PairsToOrderedMap([]utils.Pair{
				{
					Key:   model.SharedCodePathContentKey,
					Value: `_shared/keboola.python-transformation-v2`,
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
								"# {{:codes/code1}}\n",
							},
						},
						{
							CodeKey: model.CodeKey{
								ComponentId: `keboola.python-transformation-v2`,
							},
							Name: `Code 2`,
							Scripts: []string{
								" {{:codes/code2}}\n",
								"#     {{:codes/code1}}",
							},
						},
					},
				},
			},
		},
	}
	assert.NoError(t, context.State.Set(configState))
	context.Naming.Attach(configState.Key(), configState.PathInProject)

	// Invoke
	assert.NoError(t, mapperInst.OnObjectsLoad(model.StateTypeLocal, []model.Object{configState.Local}))
	assert.Empty(t, logs.String())

	// Path is replaced by ID
	_, found := configState.Local.Content.Get(model.SharedCodePathContentKey)
	assert.False(t, found)
	sharedCodeId, found := configState.Local.Content.Get(model.SharedCodeIdContentKey)
	assert.True(t, found)
	assert.Equal(t, sharedCodeId, `456`)
	sharedCodeRowIds, found := configState.Local.Content.Get(model.SharedCodeRowsIdContentKey)
	assert.True(t, found)
	assert.Equal(t, sharedCodeRowIds, []string{`1234`, `5678`})

	// Paths in transformation blocks are replaced by IDs
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
						"{{1234}}",
					},
				},
				{
					CodeKey: model.CodeKey{
						ComponentId: `keboola.python-transformation-v2`,
					},
					Name: `Code 2`,
					Scripts: []string{
						"{{5678}}",
						"{{1234}}",
					},
				},
			},
		},
	}, configState.Local.Blocks)
}
