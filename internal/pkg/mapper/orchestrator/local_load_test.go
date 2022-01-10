package orchestrator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestMapAfterLocalLoad(t *testing.T) {
	t.Parallel()
	mapper, context, logs := createMapper(t)
	orchestratorConfigState := createLocalLoadFixtures(t, context)
	target1, target2, target3 := createTargetConfigs(t, context)

	// Local files
	phasesDir := context.NamingGenerator.PhasesDir(orchestratorConfigState.Path())
	files := []*filesystem.File{
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
	}
	for _, file := range files {
		assert.NoError(t, context.Fs.WriteFile(file))
	}
	logs.Truncate()

	// Load
	changes := model.NewLocalChanges()
	changes.AddLoaded(orchestratorConfigState)
	assert.NoError(t, mapper.OnLocalChange(changes))

	// Logs
	expectedLogs := `
DEBUG  Loaded "branch/other/orchestrator/phases/001-phase/phase.json"
DEBUG  Loaded "branch/other/orchestrator/phases/001-phase/001-task-1/task.json"
DEBUG  Loaded "branch/other/orchestrator/phases/001-phase/002-task-2/task.json"
DEBUG  Loaded "branch/other/orchestrator/phases/002-phase-with-deps/phase.json"
DEBUG  Loaded "branch/other/orchestrator/phases/002-phase-with-deps/001-task-3/task.json"
`
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logs.AllMessages())

	// Check target configs relation
	rel1, err := target1.Local.Relations.GetOneByType(model.UsedInOrchestratorRelType)
	assert.NoError(t, err)
	assert.Equal(t, orchestratorConfigState.Id, rel1.(*model.UsedInOrchestratorRelation).ConfigId)
	rel2, err := target2.Local.Relations.GetOneByType(model.UsedInOrchestratorRelType)
	assert.NoError(t, err)
	assert.Equal(t, orchestratorConfigState.Id, rel2.(*model.UsedInOrchestratorRelation).ConfigId)
	rel3, err := target3.Local.Relations.GetOneByType(model.UsedInOrchestratorRelType)
	assert.NoError(t, err)
	assert.Equal(t, orchestratorConfigState.Id, rel3.(*model.UsedInOrchestratorRelation).ConfigId)

	// Orchestration
	expectedOrchestration := &model.Orchestration{
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
	assert.Equal(t, expectedOrchestration, orchestratorConfigState.Local.Orchestration)
}

func TestMapAfterLocalLoadError(t *testing.T) {
	t.Parallel()
	mapper, context, logs := createMapper(t)
	orchestratorConfigState := createLocalLoadFixtures(t, context)

	// Local files
	phasesDir := context.NamingGenerator.PhasesDir(orchestratorConfigState.Path())
	files := []*filesystem.File{
		filesystem.
			NewFile(
				phasesDir+`/001-phase/phase.json`,
				`{"name":"Phase","dependsOn":["missing-phase"],"foo":"bar"}`,
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
	}
	for _, file := range files {
		assert.NoError(t, context.Fs.WriteFile(file))
	}
	assert.NoError(t, context.Fs.Mkdir(phasesDir+`/002-phase-with-deps`))
	logs.Truncate()

	// Load
	changes := model.NewLocalChanges()
	changes.AddLoaded(orchestratorConfigState)
	err := mapper.OnLocalChange(changes)
	assert.Error(t, err)

	// Assert error
	expectedError := `
invalid orchestrator config "branch/other/orchestrator":
  - invalid phase "001-phase":
    - invalid task "001-task-1":
      - config "branch/extractor/target-config-1" not found
    - invalid task "002-task-2":
      - config "branch/extractor/target-config-2" not found
  - invalid phase "002-phase-with-deps":
    - missing phase config file "phases/002-phase-with-deps/phase.json"
  - missing phase "missing-phase", referenced from "001-phase"
`
	assert.Equal(t, strings.Trim(expectedError, "\n"), err.Error())
}

func TestMapAfterLocalLoadDepsCycle(t *testing.T) {
	t.Parallel()
	mapper, context, logs := createMapper(t)
	orchestratorConfigState := createLocalLoadFixtures(t, context)
	createTargetConfigs(t, context)

	// Local files
	phasesDir := context.NamingGenerator.PhasesDir(orchestratorConfigState.Path())
	files := []*filesystem.File{
		filesystem.
			NewFile(
				phasesDir+`/001-phase/phase.json`,
				`{"name":"Phase 1","dependsOn":[],"foo":"bar"}`,
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
				phasesDir+`/002-phase/phase.json`,
				`{"name":"Phase 2","dependsOn":["003-phase"],"foo":"bar"}`,
			).
			SetDescription(`phase config file`),
		filesystem.
			NewFile(
				phasesDir+`/003-phase/phase.json`,
				`{"name":"Phase 3","dependsOn":["002-phase"],"foo":"bar"}`,
			).
			SetDescription(`phase config file`),
	}
	for _, file := range files {
		assert.NoError(t, context.Fs.WriteFile(file))
	}
	logs.Truncate()

	// Load
	changes := model.NewLocalChanges()
	changes.AddLoaded(orchestratorConfigState)
	err := mapper.OnLocalChange(changes)
	assert.Error(t, err)

	// Assert error
	expectedError := `
invalid orchestrator config "branch/other/orchestrator":
  - found cycles in phases "dependsOn":
    - 002-phase -> 003-phase -> 002-phase
`
	assert.Equal(t, strings.Trim(expectedError, "\n"), err.Error())
}
