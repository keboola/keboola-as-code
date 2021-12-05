package orchestrator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestMapBeforeLocalSave(t *testing.T) {
	t.Parallel()
	mapper, context, logs := createMapper(t)

	// Recipe
	orchestratorConfigState := createLocalSaveFixtures(t, context, true)
	recipe := &model.LocalSaveRecipe{
		ObjectManifest: orchestratorConfigState.ConfigManifest,
		Object:         orchestratorConfigState.Remote,
		Metadata:       filesystem.NewJsonFile(model.MetaFile, utils.NewOrderedMap()),
		Configuration:  filesystem.NewJsonFile(model.ConfigFile, utils.NewOrderedMap()),
		Description:    filesystem.NewFile(model.DescriptionFile, ``),
	}

	// Save
	assert.NoError(t, mapper.MapBeforeLocalSave(recipe))
	assert.Empty(t, logs.String())

	// Minify JSON
	for _, file := range recipe.ExtraFiles {
		data := utils.NewOrderedMap()
		if err := json.DecodeString(file.Content, data); err == nil {
			file.Content = json.MustEncodeString(data, false)
		}
	}

	// Check generated files
	phasesDir := context.Naming.PhasesDir(orchestratorConfigState.Path())
	assert.Equal(t, []*filesystem.File{
		filesystem.NewFile(phasesDir+`/.gitkeep`, ``),
		filesystem.
			NewFile(
				phasesDir+`/001-phase/phase.json`,
				`{"name":"Phase","dependsOn":[],"foo":"bar"}`,
			).
			SetDescription(`phase config file`),
		filesystem.
			NewFile(
				phasesDir+`/001-phase/001-task-1/task.json`,
				`{"name":"Task 1","task":{"mode":"run","configPath":"extractor/target-config-1"},"continueOnFailure":false,"enabled":true}`,
			).
			SetDescription(`task config file`),
		filesystem.
			NewFile(
				phasesDir+`/001-phase/002-task-2/task.json`,
				`{"name":"Task 2","task":{"mode":"run","configPath":"extractor/target-config-2"},"continueOnFailure":false,"enabled":false}`,
			).
			SetDescription(`task config file`),
		filesystem.
			NewFile(
				phasesDir+`/002-phase-with-deps/phase.json`,
				`{"name":"Phase With Deps","dependsOn":["001-phase"]}`,
			).
			SetDescription(`phase config file`),
		filesystem.
			NewFile(
				phasesDir+`/002-phase-with-deps/001-task-3/task.json`,
				`{"name":"Task 3","task":{"mode":"run","configPath":"extractor/target-config-3"},"continueOnFailure":false,"enabled":true}`,
			).
			SetDescription(`task config file`),
	}, recipe.ExtraFiles)
}

func TestMapBeforeLocalSaveWarnings(t *testing.T) {
	t.Parallel()
	mapper, context, logs := createMapper(t)

	// Recipe
	orchestratorConfigState := createLocalSaveFixtures(t, context, false)
	recipe := &model.LocalSaveRecipe{
		ObjectManifest: orchestratorConfigState.ConfigManifest,
		Object:         orchestratorConfigState.Remote,
		Metadata:       filesystem.NewJsonFile(model.MetaFile, utils.NewOrderedMap()),
		Configuration:  filesystem.NewJsonFile(model.ConfigFile, utils.NewOrderedMap()),
		Description:    filesystem.NewFile(model.DescriptionFile, ``),
	}

	// Save
	assert.NoError(t, mapper.MapBeforeLocalSave(recipe))
	expectedWarnings := `
WARN  Warning: cannot save orchestrator config "branch/other/orchestrator":
  - cannot save phase "001-phase":
    - cannot save task "001-task-1":
      - config "branch:123/component:foo.bar1/config:123" not found
    - cannot save task "002-task-2":
      - config "branch:123/component:foo.bar2/config:789" not found
  - cannot save phase "002-phase-with-deps":
    - cannot save task "001-task-3":
      - config "branch:123/component:foo.bar2/config:456" not found
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logs.String())
}

func createLocalSaveFixtures(t *testing.T, context model.MapperContext, createTargets bool) *model.ConfigState {
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
				Content: utils.PairsToOrderedMap([]utils.Pair{
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
						Content: utils.PairsToOrderedMap([]utils.Pair{
							{
								Key: `task`,
								Value: *utils.PairsToOrderedMap([]utils.Pair{
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
						Content: utils.PairsToOrderedMap([]utils.Pair{
							{
								Key: `task`,
								Value: *utils.PairsToOrderedMap([]utils.Pair{
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
				Content: utils.NewOrderedMap(),
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
						Content: utils.PairsToOrderedMap([]utils.Pair{
							{
								Key: `task`,
								Value: *utils.PairsToOrderedMap([]utils.Pair{
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
		Remote: &model.Config{ConfigKey: configKey, Content: utils.NewOrderedMap(), Orchestration: orchestration},
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
