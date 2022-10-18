package orchestrator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/read/mapper/corefiles"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/read/mapper/orchestrator"
)

func TestMapAfterLocalLoad(t *testing.T) {
	t.Parallel()
	s, d := createStateWithMapper(t)
	fs := s.ObjectsRoot()
	logger := d.DebugLogger()

	orchestratorConfigState := createOrchestratorConfig(t, s)
	target1, target2, target3 := createTargetConfigs(t, s)

	// Local files
	phasesDir := s.NamingGenerator().PhasesDir(orchestratorConfigState.Path())
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
		assert.NoError(t, fs.WriteFile(file))
	}
	logger.Truncate()

	// Load
	changes := model.NewLocalChanges()
	changes.AddLoaded(orchestratorConfigState)
	assert.NoError(t, s.Mapper().AfterLocalOperation(context.Background(), changes))

	// Logs
	expectedLogs := `
DEBUG  Loaded "branch/other/orchestrator/phases/001-phase/phase.json"
DEBUG  Loaded "branch/other/orchestrator/phases/001-phase/001-task-1/task.json"
DEBUG  Loaded "branch/other/orchestrator/phases/001-phase/002-task-2/task.json"
DEBUG  Loaded "branch/other/orchestrator/phases/001-phase/003-task-3/task.json"
DEBUG  Loaded "branch/other/orchestrator/phases/002-phase-with-deps/phase.json"
DEBUG  Loaded "branch/other/orchestrator/phases/002-phase-with-deps/001-task-4/task.json"
DEBUG  Loaded "branch/other/orchestrator/phases/002-phase-with-deps/002-task-5/task.json"
`
	wildcards.Assert(t, strings.TrimLeft(expectedLogs, "\n"), logger.AllMessages(), ``)

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
	phase1Key := model.PhaseKey{
		BranchId:    123,
		ComponentId: storageapi.OrchestratorComponentID,
		ConfigId:    `456`,
		Index:       0,
	}
	phase2Key := model.PhaseKey{
		BranchId:    123,
		ComponentId: storageapi.OrchestratorComponentID,
		ConfigId:    `456`,
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
						ComponentId: `foo.bar1`,
						ConfigId:    `123`,
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
						ComponentId: `foo.bar2`,
						ConfigId:    `789`,
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
						ComponentId: `foo.bar2`,
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
						BranchId:    123,
						ComponentId: storageapi.OrchestratorComponentID,
						ConfigId:    `456`,
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
						ComponentId: `foo.bar2`,
						ConfigId:    `456`,
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
						ComponentId: `foo.bar3`,
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
	s, d := createStateWithMapper(t)
	logger := d.DebugLogger()
	fs := s.ObjectsRoot()
	orchestratorConfigState := createOrchestratorConfig(t, s)

	// Local files
	phasesDir := s.NamingGenerator().PhasesDir(orchestratorConfigState.Path())
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
	changes := model.NewLocalChanges()
	changes.AddLoaded(orchestratorConfigState)
	err := s.Mapper().AfterLocalOperation(context.Background(), changes)
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

func TestMapAfterLocalLoadDepsCycle(t *testing.T) {
	t.Parallel()
	s, d := createStateWithMapper(t)
	logger := d.DebugLogger()
	fs := s.ObjectsRoot()
	orchestratorConfigState := createOrchestratorConfig(t, s)
	createTargetConfigs(t, s)

	// Local files
	phasesDir := s.NamingGenerator().PhasesDir(orchestratorConfigState.Path())
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
	changes := model.NewLocalChanges()
	changes.AddLoaded(orchestratorConfigState)
	err := s.Mapper().AfterLocalOperation(context.Background(), changes)
	assert.Error(t, err)

	// Assert error
	expectedError := `
invalid orchestrator config "branch/other/orchestrator":
  - found cycles in phases "dependsOn":
    - 002-phase -> 003-phase -> 002-phase
`
	assert.Equal(t, strings.Trim(expectedError, "\n"), err.Error())
}

func createStateWithMapper(t *testing.T) (*state.State, dependencies.Mocked) {
	t.Helper()
	d := dependencies.NewMockedDeps()
	mockedState := d.MockedState()
	mockedState.Mapper().AddMapper(corefiles.NewMapper(mockedState))
	mockedState.Mapper().AddMapper(orchestrator.NewMapper(mockedState))
	return mockedState, d
}

func createOrchestratorConfig(t *testing.T, state *state.State) *model.ConfigState {
	t.Helper()

	// Branch
	branchKey := model.BranchKey{
		Id: 123,
	}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{
			BranchKey: branchKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(``, `branch`),
			},
		},
		Local: &model.Branch{BranchKey: branchKey},
	}
	assert.NoError(t, state.Set(branchState))

	// Orchestrator config
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: storageapi.OrchestratorComponentID,
		Id:          `456`,
	}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(`branch`, `other/orchestrator`),
			},
		},
		Local: &model.Config{ConfigKey: configKey, Content: orderedmap.New()},
	}

	assert.NoError(t, state.Set(configState))
	return configState
}

func createTargetConfigs(t *testing.T, state *state.State) (*model.ConfigState, *model.ConfigState, *model.ConfigState) {
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
				AbsPath: model.NewAbsPath(`branch/extractor`, `target-config-1`),
			},
		},
		Local:  &model.Config{ConfigKey: targetConfigKey1},
		Remote: &model.Config{ConfigKey: targetConfigKey1},
	}
	assert.NoError(t, state.Set(targetConfigState1))

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
				AbsPath: model.NewAbsPath(`branch/extractor`, `target-config-2`),
			},
		},
		Local:  &model.Config{ConfigKey: targetConfigKey2},
		Remote: &model.Config{ConfigKey: targetConfigKey2},
	}
	assert.NoError(t, state.Set(targetConfigState2))

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
				AbsPath: model.NewAbsPath(`branch/extractor`, `target-config-3`),
			},
		},
		Local:  &model.Config{ConfigKey: targetConfigKey3},
		Remote: &model.Config{ConfigKey: targetConfigKey3},
	}
	assert.NoError(t, state.Set(targetConfigState3))

	return targetConfigState1, targetConfigState2, targetConfigState3
}
