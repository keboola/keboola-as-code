package orchestrator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestMapAfterLocalLoad(t *testing.T) {
	t.Parallel()
	context, logs := createMapperContext(t)
	orchestratorConfigState := createLocalLoadFixtures(t, context, true)

	// Local files
	phasesDir := context.Naming.PhasesDir(orchestratorConfigState.Path())
	files := []*filesystem.File{
		filesystem.
			CreateFile(
				phasesDir+`/001-phase/phase.json`,
				`{"name":"Phase","dependsOn":[],"foo":"bar"}`,
			).
			SetDescription(`phase config file`),
		filesystem.
			CreateFile(
				phasesDir+`/001-phase/001-task-1/task.json`,
				`{"name":"Task 1","task":{"mode":"run","configPath":"extractor/target-config-1"},"continueOnFailure":false,"enabled":true}`,
			).
			SetDescription(`task config file`),
		filesystem.
			CreateFile(
				phasesDir+`/001-phase/002-task-2/task.json`,
				`{"name":"Task 2","task":{"mode":"run","configPath":"extractor/target-config-2"},"continueOnFailure":false,"enabled":false}`,
			).
			SetDescription(`task config file`),
		filesystem.
			CreateFile(
				phasesDir+`/002-phase-with-deps/phase.json`,
				`{"name":"Phase With Deps","dependsOn":["001-phase"]}`,
			).
			SetDescription(`phase config file`),
		filesystem.
			CreateFile(
				phasesDir+`/002-phase-with-deps/001-task-3/task.json`,
				`{"name":"Task 3","task":{"mode":"run","configPath":"extractor/target-config-3"},"continueOnFailure":false,"enabled":true}`,
			).
			SetDescription(`task config file`),
	}
	for _, file := range files {
		assert.NoError(t, context.Fs.WriteFile(file))
	}
	logs.Truncate()

	// Recipe
	event := model.OnObjectsLoadEvent{
		StateType:  model.StateTypeLocal,
		NewObjects: []model.Object{orchestratorConfigState.Local},
		AllObjects: context.State.LocalObjects(),
	}

	// Load
	assert.NoError(t, NewMapper(context).OnObjectsLoad(event))

	// Logs
	expectedLogs := `
DEBUG  Loaded "branch/other/orchestrator/phases/001-phase/phase.json"
DEBUG  Loaded "branch/other/orchestrator/phases/001-phase/001-task-1/task.json"
DEBUG  Loaded "branch/other/orchestrator/phases/001-phase/002-task-2/task.json"
DEBUG  Loaded "branch/other/orchestrator/phases/002-phase-with-deps/phase.json"
DEBUG  Loaded "branch/other/orchestrator/phases/002-phase-with-deps/001-task-3/task.json"
`
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logs.String())

	// Orchestration
	expectedOrchestration := &model.Orchestration{
		Phases: []model.Phase{
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
				Tasks: []model.Task{
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
				Tasks: []model.Task{
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
	assert.Equal(t, expectedOrchestration, orchestratorConfigState.Local.Orchestration)
}

func TestMapAfterLocalLoadError(t *testing.T) {
	t.Parallel()
	context, logs := createMapperContext(t)
	orchestratorConfigState := createLocalLoadFixtures(t, context, false)

	// Local files
	phasesDir := context.Naming.PhasesDir(orchestratorConfigState.Path())
	files := []*filesystem.File{
		filesystem.
			CreateFile(
				phasesDir+`/001-phase/phase.json`,
				`{"name":"Phase","dependsOn":["missing-phase"],"foo":"bar"}`,
			).
			SetDescription(`phase config file`),
		filesystem.
			CreateFile(
				phasesDir+`/001-phase/001-task-1/task.json`,
				`{"name":"Task 1","task":{"mode":"run","configPath":"extractor/target-config-1"},"continueOnFailure":false,"enabled":true}`,
			).
			SetDescription(`task config file`),
		filesystem.
			CreateFile(
				phasesDir+`/001-phase/002-task-2/task.json`,
				`{"name":"Task 2","task":{"mode":"run","configPath":"extractor/target-config-2"},"continueOnFailure":false,"enabled":false}`,
			).
			SetDescription(`task config file`),
	}
	for _, file := range files {
		assert.NoError(t, context.Fs.WriteFile(file))
	}
	assert.NoError(t, context.Fs.Mkdir(phasesDir+`/002-phase-with-deps`))
	logs.Truncate()

	// Recipe
	event := model.OnObjectsLoadEvent{
		StateType:  model.StateTypeLocal,
		NewObjects: []model.Object{orchestratorConfigState.Local},
		AllObjects: context.State.LocalObjects(),
	}

	// Load
	err := NewMapper(context).OnObjectsLoad(event)
	assert.Error(t, err)

	// Assert error
	expectedError := `
- invalid config "branch/other/orchestrator/phases/001-phase/001-task-1/task.json":
  - config "branch/extractor/target-config-1" not found, referenced from task[0] "Task 1"
- invalid config "branch/other/orchestrator/phases/001-phase/002-task-2/task.json":
  - config "branch/extractor/target-config-2" not found, referenced from task[1] "Task 2"
- missing phase config file "branch/other/orchestrator/phases/002-phase-with-deps/phase.json"
- missing phase "missing-phase", referenced from "branch/other/orchestrator/phases/001-phase"
`
	assert.Equal(t, strings.Trim(expectedError, "\n"), err.Error())
}

func TestMapAfterLocalLoadDepsCycle(t *testing.T) {
	t.Parallel()
	context, logs := createMapperContext(t)
	orchestratorConfigState := createLocalLoadFixtures(t, context, true)

	// Local files
	phasesDir := context.Naming.PhasesDir(orchestratorConfigState.Path())
	files := []*filesystem.File{
		filesystem.
			CreateFile(
				phasesDir+`/001-phase/phase.json`,
				`{"name":"Phase 1","dependsOn":[],"foo":"bar"}`,
			).
			SetDescription(`phase config file`),
		filesystem.
			CreateFile(
				phasesDir+`/001-phase/001-task-1/task.json`,
				`{"name":"Task 1","task":{"mode":"run","configPath":"extractor/target-config-1"},"continueOnFailure":false,"enabled":true}`,
			).
			SetDescription(`task config file`),
		filesystem.
			CreateFile(
				phasesDir+`/002-phase/phase.json`,
				`{"name":"Phase 2","dependsOn":["003-phase"],"foo":"bar"}`,
			).
			SetDescription(`phase config file`),
		filesystem.
			CreateFile(
				phasesDir+`/003-phase/phase.json`,
				`{"name":"Phase 3","dependsOn":["002-phase"],"foo":"bar"}`,
			).
			SetDescription(`phase config file`),
	}
	for _, file := range files {
		assert.NoError(t, context.Fs.WriteFile(file))
	}
	logs.Truncate()

	// Recipe
	event := model.OnObjectsLoadEvent{
		StateType:  model.StateTypeLocal,
		NewObjects: []model.Object{orchestratorConfigState.Local},
		AllObjects: context.State.LocalObjects(),
	}

	// Load
	err := NewMapper(context).OnObjectsLoad(event)
	assert.Error(t, err)

	// Assert error
	expectedError := `
found cycles in phases "dependsOn" in "branch/other/orchestrator/phases"
  - "002-phase" -> "003-phase" -> "002-phase"
`
	assert.Equal(t, strings.Trim(expectedError, "\n"), err.Error())
}

func createLocalLoadFixtures(t *testing.T, context model.MapperContext, createTargets bool) *model.ConfigState {
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
	context.Naming.Attach(branchState.Key(), branchState.PathInProject)

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
		Local: &model.Config{ConfigKey: configKey, Content: utils.NewOrderedMap()},
	}
	assert.NoError(t, context.State.Set(configState))
	context.Naming.Attach(configState.Key(), configState.PathInProject)

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
		Local: &model.Config{ConfigKey: targetConfigKey1},
	}
	assert.NoError(t, context.State.Set(targetConfigState1))
	context.Naming.Attach(targetConfigState1.Key(), targetConfigState1.PathInProject)

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
		Local: &model.Config{ConfigKey: targetConfigKey2},
	}
	assert.NoError(t, context.State.Set(targetConfigState2))
	context.Naming.Attach(targetConfigState2.Key(), targetConfigState2.PathInProject)

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
		Local: &model.Config{ConfigKey: targetConfigKey3},
	}
	assert.NoError(t, context.State.Set(targetConfigState3))
	context.Naming.Attach(targetConfigState3.Key(), targetConfigState3.PathInProject)

	return configState
}
