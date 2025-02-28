package orchestrator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestMapAfterLocalLoad(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	fs := state.ObjectsRoot()
	logger := d.DebugLogger()

	orchestratorConfigState := createLocalLoadFixtures(t, state)
	target1, target2, target3 := createTargetConfigs(t, state)

	// Local files
	phasesDir := state.NamingGenerator().PhasesDir(orchestratorConfigState.Path())
	files := []filesystem.File{
		filesystem.
			NewRawFile(
				phasesDir+`/001-phase/phase.json`,
				`{"name":"Phase","dependsOn":[],"foo":"bar"}`,
			),
		filesystem.
			NewRawFile(
				phasesDir+`/001-phase/001-task-1/task.json`,
				`{"name":"Task 1","task":{"mode":"run","configPath":"extractor/target-config-1"},"continueOnFailure":false,"enabled":true}`,
			),
		filesystem.
			NewRawFile(
				phasesDir+`/001-phase/002-task-2/task.json`,
				`{"name":"Task 2 - disabled","enabled":false,"task":{"mode":"run","configPath":"extractor/target-config-2"},"continueOnFailure":false}`,
			),
		filesystem.
			NewRawFile(
				phasesDir+`/001-phase/003-task-3/task.json`,
				`{"name":"Task 3 - disabled without configId","enabled":false,"task":{"mode":"run","componentId":"foo.bar2"},"continueOnFailure":false}`,
			),
		filesystem.
			NewRawFile(
				phasesDir+`/002-phase-with-deps/phase.json`,
				`{"name":"Phase With Deps","dependsOn":["001-phase"]}`,
			),
		filesystem.
			NewRawFile(
				phasesDir+`/002-phase-with-deps/001-task-4/task.json`,
				`{"name":"Task 4","task":{"mode":"run","configPath":"extractor/target-config-3"},"continueOnFailure":false,"enabled":true}`,
			),
		filesystem.
			NewRawFile(
				phasesDir+`/002-phase-with-deps/002-task-5/task.json`,
				`{"name":"Task 5 - configData","task":{"mode":"run","configData":{"params":"value"},"componentId":"foo.bar3"},"continueOnFailure":false,"enabled":true}`,
			),
	}
	for _, file := range files {
		require.NoError(t, fs.WriteFile(t.Context(), file))
	}
	logger.Truncate()

	// Load
	changes := model.NewLocalChanges()
	changes.AddLoaded(orchestratorConfigState)
	require.NoError(t, state.Mapper().AfterLocalOperation(t.Context(), changes))

	// Logs
	expectedLogs := `
{"level":"debug","message":"Loaded \"branch/other/orchestrator/phases/001-phase/phase.json\""}
{"level":"debug","message":"Loaded \"branch/other/orchestrator/phases/001-phase/001-task-1/task.json\""}
{"level":"debug","message":"Loaded \"branch/other/orchestrator/phases/001-phase/002-task-2/task.json\""}
{"level":"debug","message":"Loaded \"branch/other/orchestrator/phases/001-phase/003-task-3/task.json\""}
{"level":"debug","message":"Loaded \"branch/other/orchestrator/phases/002-phase-with-deps/phase.json\""}
{"level":"debug","message":"Loaded \"branch/other/orchestrator/phases/002-phase-with-deps/001-task-4/task.json\""}
{"level":"debug","message":"Loaded \"branch/other/orchestrator/phases/002-phase-with-deps/002-task-5/task.json\""}
`
	logger.AssertJSONMessages(t, expectedLogs)

	// Check target configs relation
	rel1, err := target1.Local.Relations.GetOneByType(model.UsedInOrchestratorRelType)
	require.NoError(t, err)
	assert.Equal(t, orchestratorConfigState.ID, rel1.(*model.UsedInOrchestratorRelation).ConfigID)
	rel2, err := target2.Local.Relations.GetOneByType(model.UsedInOrchestratorRelType)
	require.NoError(t, err)
	assert.Equal(t, orchestratorConfigState.ID, rel2.(*model.UsedInOrchestratorRelation).ConfigID)
	rel3, err := target3.Local.Relations.GetOneByType(model.UsedInOrchestratorRelType)
	require.NoError(t, err)
	assert.Equal(t, orchestratorConfigState.ID, rel3.(*model.UsedInOrchestratorRelation).ConfigID)

	// Orchestration
	phase1Key := model.PhaseKey{
		BranchID:    123,
		ComponentID: keboola.OrchestratorComponentID,
		ConfigID:    `456`,
		Index:       0,
	}
	phase2Key := model.PhaseKey{
		BranchID:    123,
		ComponentID: keboola.OrchestratorComponentID,
		ConfigID:    `456`,
		Index:       1,
	}
	expectedOrchestration := &model.Orchestration{
		Phases: []*model.Phase{
			{
				PhaseKey:  phase1Key,
				AbsPath:   model.NewAbsPath(`branch/other/orchestrator/phases`, `001-phase`),
				DependsOn: []model.PhaseKey{},
				Name:      `Phase`,
				Content: orderedmap.FromPairs([]orderedmap.Pair{
					{Key: `foo`, Value: `bar`},
				}),
				Tasks: []*model.Task{
					{
						TaskKey:     model.TaskKey{PhaseKey: phase1Key, Index: 0},
						AbsPath:     model.NewAbsPath(`branch/other/orchestrator/phases/001-phase`, `001-task-1`),
						Name:        `Task 1`,
						Enabled:     true,
						ComponentID: `foo.bar1`,
						ConfigID:    `123`,
						ConfigPath:  `branch/extractor/target-config-1`,
						Content: orderedmap.FromPairs([]orderedmap.Pair{
							{
								Key: `task`,
								Value: orderedmap.FromPairs([]orderedmap.Pair{
									{Key: `mode`, Value: `run`},
								}),
							},
							{Key: `continueOnFailure`, Value: false},
						}),
					},
					{
						TaskKey:     model.TaskKey{PhaseKey: phase1Key, Index: 1},
						AbsPath:     model.NewAbsPath(`branch/other/orchestrator/phases/001-phase`, `002-task-2`),
						Name:        `Task 2 - disabled`,
						Enabled:     false,
						ComponentID: `foo.bar2`,
						ConfigID:    `789`,
						ConfigPath:  `branch/extractor/target-config-2`,
						Content: orderedmap.FromPairs([]orderedmap.Pair{
							{
								Key: `task`,
								Value: orderedmap.FromPairs([]orderedmap.Pair{
									{Key: `mode`, Value: `run`},
								}),
							},
							{Key: `continueOnFailure`, Value: false},
						}),
					},
					{
						TaskKey:     model.TaskKey{PhaseKey: phase1Key, Index: 2},
						AbsPath:     model.NewAbsPath(`branch/other/orchestrator/phases/001-phase`, `003-task-3`),
						Name:        `Task 3 - disabled without configId`,
						Enabled:     false,
						ComponentID: `foo.bar2`,
						Content: orderedmap.FromPairs([]orderedmap.Pair{
							{
								Key: `task`,
								Value: orderedmap.FromPairs([]orderedmap.Pair{
									{Key: `mode`, Value: `run`},
								}),
							},
							{Key: `continueOnFailure`, Value: false},
						}),
					},
				},
			},
			{
				PhaseKey: phase2Key,
				AbsPath:  model.NewAbsPath(`branch/other/orchestrator/phases`, `002-phase-with-deps`),
				DependsOn: []model.PhaseKey{
					{
						BranchID:    123,
						ComponentID: keboola.OrchestratorComponentID,
						ConfigID:    `456`,
						Index:       0,
					},
				},
				Name:    `Phase With Deps`,
				Content: orderedmap.New(),
				Tasks: []*model.Task{
					{
						TaskKey:     model.TaskKey{PhaseKey: phase2Key, Index: 0},
						AbsPath:     model.NewAbsPath(`branch/other/orchestrator/phases/002-phase-with-deps`, `001-task-4`),
						Name:        `Task 4`,
						Enabled:     true,
						ComponentID: `foo.bar2`,
						ConfigID:    `456`,
						ConfigPath:  `branch/extractor/target-config-3`,
						Content: orderedmap.FromPairs([]orderedmap.Pair{
							{
								Key: `task`,
								Value: orderedmap.FromPairs([]orderedmap.Pair{
									{Key: `mode`, Value: `run`},
								}),
							},
							{Key: `continueOnFailure`, Value: false},
						}),
					},
					{
						TaskKey:     model.TaskKey{PhaseKey: phase2Key, Index: 1},
						AbsPath:     model.NewAbsPath(`branch/other/orchestrator/phases/002-phase-with-deps`, `002-task-5`),
						Name:        `Task 5 - configData`,
						Enabled:     true,
						ComponentID: `foo.bar3`,
						ConfigData:  orderedmap.FromPairs([]orderedmap.Pair{{Key: "params", Value: "value"}}),
						Content: orderedmap.FromPairs([]orderedmap.Pair{
							{
								Key: `task`,
								Value: orderedmap.FromPairs([]orderedmap.Pair{
									{Key: `mode`, Value: `run`},
								}),
							},
							{Key: `continueOnFailure`, Value: false},
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
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()
	fs := state.ObjectsRoot()
	orchestratorConfigState := createLocalLoadFixtures(t, state)
	ctx := t.Context()

	// Local files
	phasesDir := state.NamingGenerator().PhasesDir(orchestratorConfigState.Path())
	files := []filesystem.File{
		filesystem.
			NewRawFile(
				phasesDir+`/001-phase/phase.json`,
				`{"name":"Phase","dependsOn":["missing-phase"],"foo":"bar"}`,
			).
			SetDescription(`phase config file`),
		filesystem.
			NewRawFile(
				phasesDir+`/001-phase/001-task-1/task.json`,
				`{"name":"Task 1","task":{"mode":"run","configPath":"extractor/target-config-1"},"continueOnFailure":false,"enabled":true}`,
			).
			SetDescription(`task config file`),
		filesystem.
			NewRawFile(
				phasesDir+`/001-phase/002-task-2/task.json`,
				`{"name":"Task 2","task":{"mode":"run","configPath":"extractor/target-config-2"},"continueOnFailure":false,"enabled":false}`,
			).
			SetDescription(`task config file`),
	}
	for _, file := range files {
		require.NoError(t, fs.WriteFile(ctx, file))
	}
	require.NoError(t, fs.Mkdir(ctx, phasesDir+`/002-phase-with-deps`))
	logger.Truncate()

	// Load
	changes := model.NewLocalChanges()
	changes.AddLoaded(orchestratorConfigState)
	err := state.Mapper().AfterLocalOperation(t.Context(), changes)
	require.Error(t, err)

	// Assert error
	expectedError := `
invalid orchestrator config "branch/other/orchestrator":
- invalid phase "001-phase":
  - invalid task "001-task-1":
    - target config "branch/extractor/target-config-1" not found
  - invalid task "002-task-2":
    - target config "branch/extractor/target-config-2" not found
- invalid phase "002-phase-with-deps":
  - missing phase config file "phases/002-phase-with-deps/phase.json"
- missing phase "missing-phase", referenced from "001-phase"
`
	assert.Equal(t, strings.Trim(expectedError, "\n"), err.Error())
}

func TestMapAfterLocalLoadDepsCycle(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()
	fs := state.ObjectsRoot()
	orchestratorConfigState := createLocalLoadFixtures(t, state)
	ctx := t.Context()
	createTargetConfigs(t, state)

	// Local files
	phasesDir := state.NamingGenerator().PhasesDir(orchestratorConfigState.Path())
	files := []filesystem.File{
		filesystem.
			NewRawFile(
				phasesDir+`/001-phase/phase.json`,
				`{"name":"Phase 1","dependsOn":[],"foo":"bar"}`,
			).
			SetDescription(`phase config file`),
		filesystem.
			NewRawFile(
				phasesDir+`/001-phase/001-task-1/task.json`,
				`{"name":"Task 1","task":{"mode":"run","configPath":"extractor/target-config-1"},"continueOnFailure":false,"enabled":true}`,
			).
			SetDescription(`task config file`),
		filesystem.
			NewRawFile(
				phasesDir+`/002-phase/phase.json`,
				`{"name":"Phase 2","dependsOn":["003-phase"],"foo":"bar"}`,
			).
			SetDescription(`phase config file`),
		filesystem.
			NewRawFile(
				phasesDir+`/003-phase/phase.json`,
				`{"name":"Phase 3","dependsOn":["002-phase"],"foo":"bar"}`,
			).
			SetDescription(`phase config file`),
	}
	for _, file := range files {
		require.NoError(t, fs.WriteFile(ctx, file))
	}
	logger.Truncate()

	// Load
	changes := model.NewLocalChanges()
	changes.AddLoaded(orchestratorConfigState)
	err := state.Mapper().AfterLocalOperation(t.Context(), changes)
	require.Error(t, err)

	// Assert error
	expectedError := `
invalid orchestrator config "branch/other/orchestrator":
- found cycles in phases "dependsOn":
  - 002-phase -> 003-phase -> 002-phase
`
	assert.Equal(t, strings.Trim(expectedError, "\n"), err.Error())
}
