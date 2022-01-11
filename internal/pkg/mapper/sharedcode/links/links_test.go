package links_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/links"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func createMapper(t *testing.T) (*mapper.Mapper, mapper.Context, log.DebugLogger) {
	t.Helper()
	logger := log.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, ".")
	assert.NoError(t, err)
	projectState := state.NewRegistry(knownpaths.NewNop(), model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	namingTemplate := naming.TemplateWithIds()
	namingRegistry := naming.NewRegistry()
	namingGenerator := naming.NewGenerator(namingTemplate, namingRegistry)
	context := mapper.Context{Logger: logger, Fs: fs, NamingGenerator: namingGenerator, NamingRegistry: namingRegistry, State: projectState}
	manifest := projectManifest.New(1, `foo.bar`)
	assert.NoError(t, err)
	mapperInst := mapper.New()
	localManager := local.NewManager(logger, fs, manifest, namingGenerator, projectState, mapperInst)
	mapperInst.AddMapper(links.NewMapper(localManager, context))
	return mapperInst, context, logger
}

func createLocalTranWithSharedCode(t *testing.T, context mapper.Context) *model.ConfigState {
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
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key:   model.SharedCodePathContentKey,
					Value: `_shared/keboola.python-transformation-v2`,
				},
			}),
			Transformation: &model.Transformation{
				Blocks: []*model.Block{
					{
						Name:          `Block 1`,
						PathInProject: model.NewPathInProject(`branch/transformation/blocks`, `block-1`),
						Codes: model.Codes{
							{
								CodeKey: model.CodeKey{
									ComponentId: `keboola.python-transformation-v2`,
								},
								Name:          `Code 1`,
								PathInProject: model.NewPathInProject(`branch/transformation/blocks/block-1`, `code-1`),
								Scripts: model.Scripts{
									model.StaticScript{Value: `print(100)`},
									model.StaticScript{Value: "# {{:codes/code1}}\n"},
								},
							},
							{
								CodeKey: model.CodeKey{
									ComponentId: `keboola.python-transformation-v2`,
								},
								Name:          `Code 2`,
								PathInProject: model.NewPathInProject(`branch/transformation/blocks/block-1`, `code-2`),
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
	assert.NoError(t, context.State.Set(transformation))
	assert.NoError(t, context.NamingRegistry.Attach(transformation.Key(), transformation.PathInProject))
	return transformation
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

func createRemoteTranWithSharedCode(t *testing.T, sharedCodeKey model.ConfigKey, sharedCodeRowsKeys []model.ConfigRowKey, context mapper.Context) *model.ConfigState {
	t.Helper()

	// Rows -> rows IDs
	var rows []interface{}
	for _, row := range sharedCodeRowsKeys {
		rows = append(rows, row.Id.String())
	}

	key := model.ConfigKey{
		BranchId:    sharedCodeKey.BranchId,
		ComponentId: model.ComponentId("keboola.python-transformation-v2"),
		Id:          model.ConfigId("001"),
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

	assert.NoError(t, context.State.Set(transformation))
	return transformation
}
