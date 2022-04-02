package orchestrator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

func TestOrchestratorLocalMapper_AfterLocalOperation(t *testing.T) {
	t.Parallel()
	state, d := createLocalStateWithMapper(t)
	fs := d.Fs()
	logger := d.DebugLogger()

	orchestratorConfig, orchestratorPath := createLocalLoadFixtures(t, state)
	target1, target2, target3 := createTargetConfigs(t, state, state.NamingRegistry())

	// Local files
	phasesDir := state.NamingGenerator().PhasesDir(orchestratorPath).String()
	files := []filesystem.File{
		filesystem.
			NewRawFile(
				phasesDir+`/001-phase/phase.json`,
				`{"name":"Phase","dependsOn":[],"foo":"bar"}`,
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
		filesystem.
			NewRawFile(
				phasesDir+`/002-phase-with-deps/phase.json`,
				`{"name":"Phase With Deps","dependsOn":["001-phase"]}`,
			).
			SetDescription(`phase config file`),
		filesystem.
			NewRawFile(
				phasesDir+`/002-phase-with-deps/001-task-3/task.json`,
				`{"name":"Task 3","task":{"mode":"run","configPath":"extractor/target-config-3"},"continueOnFailure":false,"enabled":true}`,
			).
			SetDescription(`task config file`),
	}
	for _, file := range files {
		assert.NoError(t, fs.WriteFile(file))
	}
	logger.Truncate()

	// Load
	assert.NoError(t, state.Mapper().AfterLocalOperation(model.NewChanges().AddLoaded(orchestratorConfig)))

	// Logs
	expectedLogs := `
DEBUG  Loaded "branch/other/orchestrator/phases/001-phase/phase.json"
DEBUG  Loaded "branch/other/orchestrator/phases/001-phase/001-task-1/task.json"
DEBUG  Loaded "branch/other/orchestrator/phases/001-phase/002-task-2/task.json"
DEBUG  Loaded "branch/other/orchestrator/phases/002-phase-with-deps/phase.json"
DEBUG  Loaded "branch/other/orchestrator/phases/002-phase-with-deps/001-task-3/task.json"
`
	testhelper.AssertWildcards(t, strings.TrimLeft(expectedLogs, "\n"), logger.AllMessages(), ``)

	// Check target configs relation
	rel1, err := target1.Relations.GetOneByType(model.UsedInOrchestratorRelType)
	assert.NoError(t, err)
	assert.Equal(t, orchestratorConfig.ConfigId, rel1.(*model.UsedInOrchestratorRelation).ConfigId)
	rel2, err := target2.Relations.GetOneByType(model.UsedInOrchestratorRelType)
	assert.NoError(t, err)
	assert.Equal(t, orchestratorConfig.ConfigId, rel2.(*model.UsedInOrchestratorRelation).ConfigId)
	rel3, err := target3.Relations.GetOneByType(model.UsedInOrchestratorRelType)
	assert.NoError(t, err)
	assert.Equal(t, orchestratorConfig.ConfigId, rel3.(*model.UsedInOrchestratorRelation).ConfigId)

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
				DependsOn: []model.PhaseKey{},
				Name:      `Phase`,
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
						Name:        `Task 1`,
						ComponentId: `foo.bar1`,
						ConfigId:    `123`,
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
						Name:        `Task 2`,
						ComponentId: `foo.bar2`,
						ConfigId:    `789`,
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
						Name:        `Task 3`,
						ComponentId: `foo.bar2`,
						ConfigId:    `456`,
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
	assert.Equal(t, expectedOrchestration, orchestratorConfig.Orchestration)

	// Check naming registry
	assert.Equal(t, map[string]string{
		`branch "123"`: `branch`,
		`config "branch:123/component:foo.bar1/config:123"`:                          `branch/extractor/target-config-1`,
		`config "branch:123/component:foo.bar2/config:456"`:                          `branch/extractor/target-config-3`,
		`config "branch:123/component:foo.bar2/config:789"`:                          `branch/extractor/target-config-2`,
		`config "branch:123/component:keboola.orchestrator/config:456"`:              `branch/other/orchestrator`,
		`phase "branch:123/component:keboola.orchestrator/config:456/phase:0"`:       `branch/other/orchestrator/phases/001-phase`,
		`task "branch:123/component:keboola.orchestrator/config:456/phase:0/task:0"`: `branch/other/orchestrator/phases/001-phase/001-task-1`,
		`task "branch:123/component:keboola.orchestrator/config:456/phase:0/task:1"`: `branch/other/orchestrator/phases/001-phase/002-task-2`,
		`phase "branch:123/component:keboola.orchestrator/config:456/phase:1"`:       `branch/other/orchestrator/phases/002-phase-with-deps`,
		`task "branch:123/component:keboola.orchestrator/config:456/phase:1/task:0"`: `branch/other/orchestrator/phases/002-phase-with-deps/001-task-3`,
	}, state.NamingRegistry().AllStrings())

}

func TestOrchestratorLocalMapper_AfterLocalOperation_Error(t *testing.T) {
	t.Parallel()
	state, d := createLocalStateWithMapper(t)
	logger := d.DebugLogger()
	fs := d.Fs()
	orchestratorConfig, orchestratorPath := createLocalLoadFixtures(t, state)

	// Local files
	phasesDir := state.NamingGenerator().PhasesDir(orchestratorPath).String()
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
		assert.NoError(t, fs.WriteFile(file))
	}
	assert.NoError(t, fs.Mkdir(phasesDir+`/002-phase-with-deps`))
	logger.Truncate()

	// Load
	err := state.Mapper().AfterLocalOperation(model.NewChanges().AddLoaded(orchestratorConfig))
	assert.Error(t, err)

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

func TestOrchestratorLocalMapper_AfterLocalOperation_CyclicDependsOn(t *testing.T) {
	t.Parallel()
	state, d := createLocalStateWithMapper(t)
	logger := d.DebugLogger()
	fs := d.Fs()
	orchestratorConfig, orchestratorPath := createLocalLoadFixtures(t, state)
	createTargetConfigs(t, state, state.NamingRegistry())

	// Local files
	phasesDir := state.NamingGenerator().PhasesDir(orchestratorPath).String()
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
		assert.NoError(t, fs.WriteFile(file))
	}
	logger.Truncate()

	// Load
	err := state.Mapper().AfterLocalOperation(model.NewChanges().AddLoaded(orchestratorConfig))
	assert.Error(t, err)

	// Assert error
	expectedError := `
invalid orchestrator config "branch/other/orchestrator":
  - found cycles in phases "dependsOn":
    - 002-phase -> 003-phase -> 002-phase
`
	assert.Equal(t, strings.Trim(expectedError, "\n"), err.Error())
}
