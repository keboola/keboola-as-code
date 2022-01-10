package orchestrator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/orchestrator"
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
	projectState := state.NewRegistry(log.NewNopLogger(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	namingTemplate := naming.TemplateWithIds()
	namingRegistry := naming.NewRegistry()
	namingGenerator := naming.NewGenerator(namingTemplate, namingRegistry)
	context := mapper.Context{Logger: logger, Fs: fs, NamingGenerator: namingGenerator, NamingRegistry: namingRegistry, State: projectState}
	manifest := projectManifest.New(1, `foo.bar`)
	mapperInst := mapper.New()
	localManager := local.NewManager(logger, fs, manifest, namingGenerator, projectState, mapperInst)
	mapperInst.AddMapper(orchestrator.NewMapper(localManager, context))
	return mapperInst, context, logger
}

func createTargetConfigs(t *testing.T, context mapper.Context) (*model.ConfigState, *model.ConfigState, *model.ConfigState) {
	t.Helper()

	// Target config 1
	targetConfigKey1 := model.ConfigKey{
		BranchId:    123,
		ComponentId: `foo.bar1`,
		Id:          `123`,
	}
	targetConfigState1 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: targetConfigKey1,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject(`branch/extractor`, `target-config-1`),
			},
		},
		Local:  &model.Config{ConfigKey: targetConfigKey1},
		Remote: &model.Config{ConfigKey: targetConfigKey1},
	}
	assert.NoError(t, context.State.Set(targetConfigState1))
	assert.NoError(t, context.NamingRegistry.Attach(targetConfigState1.Key(), targetConfigState1.PathInProject))

	// Target config 2
	targetConfigKey2 := model.ConfigKey{
		BranchId:    123,
		ComponentId: `foo.bar2`,
		Id:          `789`,
	}
	targetConfigState2 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: targetConfigKey2,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject(`branch/extractor`, `target-config-2`),
			},
		},
		Local:  &model.Config{ConfigKey: targetConfigKey2},
		Remote: &model.Config{ConfigKey: targetConfigKey2},
	}
	assert.NoError(t, context.State.Set(targetConfigState2))
	assert.NoError(t, context.NamingRegistry.Attach(targetConfigState2.Key(), targetConfigState2.PathInProject))

	// Target config 3
	targetConfigKey3 := model.ConfigKey{
		BranchId:    123,
		ComponentId: `foo.bar2`,
		Id:          `456`,
	}
	targetConfigState3 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: targetConfigKey3,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject(`branch/extractor`, `target-config-3`),
			},
		},
		Local:  &model.Config{ConfigKey: targetConfigKey3},
		Remote: &model.Config{ConfigKey: targetConfigKey3},
	}
	assert.NoError(t, context.State.Set(targetConfigState3))
	assert.NoError(t, context.NamingRegistry.Attach(targetConfigState3.Key(), targetConfigState3.PathInProject))

	return targetConfigState1, targetConfigState2, targetConfigState3
}

func createLocalLoadFixtures(t *testing.T, context mapper.Context) *model.ConfigState {
	t.Helper()

	// Branch
	branchKey := model.BranchKey{
		Id: 123,
	}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{
			BranchKey: branchKey,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject(``, `branch`),
			},
		},
		Local: &model.Branch{BranchKey: branchKey},
	}
	assert.NoError(t, context.State.Set(branchState))
	assert.NoError(t, context.NamingRegistry.Attach(branchState.Key(), branchState.PathInProject))

	// Orchestrator config
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.OrchestratorComponentId,
		Id:          `456`,
	}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject(`branch/other`, `orchestrator`),
			},
		},
		Local: &model.Config{ConfigKey: configKey, Content: orderedmap.New()},
	}
	assert.NoError(t, context.State.Set(configState))
	assert.NoError(t, context.NamingRegistry.Attach(configState.Key(), configState.PathInProject))

	return configState
}

func createLocalSaveFixtures(t *testing.T, context mapper.Context, createTargets bool) *model.ConfigState {
	t.Helper()

	orchestration := &model.Orchestration{
		Phases: []*model.Phase{
			{
				PhaseKey: model.PhaseKey{
					BranchId:    123,
					ComponentId: model.OrchestratorComponentId,
					ConfigId:    `456`,
					Index:       0,
				},
				PathInProject: model.NewPathInProject(`branch/other/orchestrator/phases`, `001-phase`),
				DependsOn:     []model.PhaseKey{},
				Name:          `Phase`,
				Content: orderedmap.FromPairs([]orderedmap.Pair{
					{Key: `foo`, Value: `bar`},
				}),
				Tasks: []*model.Task{
					{
						TaskKey: model.TaskKey{
							PhaseKey: model.PhaseKey{
								BranchId:    123,
								ComponentId: model.OrchestratorComponentId,
								ConfigId:    `456`,
								Index:       0,
							},
							Index: 0,
						},
						PathInProject: model.NewPathInProject(`branch/other/orchestrator/phases/001-phase`, `001-task-1`),
						Name:          `Task 1`,
						ComponentId:   `foo.bar1`,
						ConfigId:      `123`,
						ConfigPath:    `branch/extractor/target-config-1`,
						Content: orderedmap.FromPairs([]orderedmap.Pair{
							{
								Key: `task`,
								Value: orderedmap.FromPairs([]orderedmap.Pair{
									{Key: `mode`, Value: `run`},
								}),
							},
							{Key: `continueOnFailure`, Value: false},
							{Key: `enabled`, Value: true},
						}),
					},
					{
						TaskKey: model.TaskKey{
							PhaseKey: model.PhaseKey{
								BranchId:    123,
								ComponentId: model.OrchestratorComponentId,
								ConfigId:    `456`,
								Index:       0,
							},
							Index: 1,
						},
						PathInProject: model.NewPathInProject(`branch/other/orchestrator/phases/001-phase`, `002-task-2`),
						Name:          `Task 2`,
						ComponentId:   `foo.bar2`,
						ConfigId:      `789`,
						ConfigPath:    `branch/extractor/target-config-2`,
						Content: orderedmap.FromPairs([]orderedmap.Pair{
							{
								Key: `task`,
								Value: orderedmap.FromPairs([]orderedmap.Pair{
									{Key: `mode`, Value: `run`},
								}),
							},
							{Key: `continueOnFailure`, Value: false},
							{Key: `enabled`, Value: false},
						}),
					},
				},
			},
			{
				PhaseKey: model.PhaseKey{
					BranchId:    123,
					ComponentId: model.OrchestratorComponentId,
					ConfigId:    `456`,
					Index:       1,
				},
				PathInProject: model.NewPathInProject(`branch/other/orchestrator/phases`, `002-phase-with-deps`),
				DependsOn: []model.PhaseKey{
					{
						BranchId:    123,
						ComponentId: model.OrchestratorComponentId,
						ConfigId:    `456`,
						Index:       0,
					},
				},
				Name:    `Phase With Deps`,
				Content: orderedmap.New(),
				Tasks: []*model.Task{
					{
						TaskKey: model.TaskKey{
							PhaseKey: model.PhaseKey{
								BranchId:    123,
								ComponentId: model.OrchestratorComponentId,
								ConfigId:    `456`,
								Index:       1,
							},
							Index: 0,
						},
						PathInProject: model.NewPathInProject(`branch/other/orchestrator/phases/002-phase-with-deps`, `001-task-3`),
						Name:          `Task 3`,
						ComponentId:   `foo.bar2`,
						ConfigId:      `456`,
						ConfigPath:    `branch/extractor/target-config-3`,
						Content: orderedmap.FromPairs([]orderedmap.Pair{
							{
								Key: `task`,
								Value: orderedmap.FromPairs([]orderedmap.Pair{
									{Key: `mode`, Value: `run`},
								}),
							},
							{Key: `continueOnFailure`, Value: false},
							{Key: `enabled`, Value: true},
						}),
					},
				},
			},
		},
	}

	// Branch
	branchKey := model.BranchKey{
		Id: 123,
	}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{
			BranchKey: branchKey,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject(``, `branch`),
			},
		},
		Local: &model.Branch{BranchKey: branchKey},
	}
	assert.NoError(t, context.State.Set(branchState))

	// Orchestrator config
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.OrchestratorComponentId,
		Id:          `456`,
	}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject(`branch/other`, `orchestrator`),
			},
		},
		Remote: &model.Config{ConfigKey: configKey, Content: orderedmap.New(), Orchestration: orchestration},
	}
	assert.NoError(t, context.State.Set(configState))

	// Create targets
	if !createTargets {
		return configState
	}

	// Target config 1
	targetConfigKey1 := model.ConfigKey{
		BranchId:    123,
		ComponentId: `foo.bar1`,
		Id:          `123`,
	}
	targetConfigState1 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: targetConfigKey1,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject(`branch/extractor`, `target-config-1`),
			},
		},
		Remote: &model.Config{ConfigKey: targetConfigKey1},
	}
	assert.NoError(t, context.State.Set(targetConfigState1))

	// Target config 2
	targetConfigKey2 := model.ConfigKey{
		BranchId:    123,
		ComponentId: `foo.bar2`,
		Id:          `789`,
	}
	targetConfigState2 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: targetConfigKey2,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject(`branch/extractor`, `target-config-2`),
			},
		},
		Remote: &model.Config{ConfigKey: targetConfigKey2},
	}
	assert.NoError(t, context.State.Set(targetConfigState2))

	// Target config 3
	targetConfigKey3 := model.ConfigKey{
		BranchId:    123,
		ComponentId: `foo.bar2`,
		Id:          `456`,
	}
	targetConfigState3 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: targetConfigKey3,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject(`branch/extractor`, `target-config-3`),
			},
		},
		Remote: &model.Config{ConfigKey: targetConfigKey3},
	}
	assert.NoError(t, context.State.Set(targetConfigState3))

	return configState
}
