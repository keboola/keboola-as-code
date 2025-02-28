package orchestrator_test

import (
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/corefiles"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func createStateWithMapper(t *testing.T) (*state.State, dependencies.Mocked) {
	t.Helper()
	d := dependencies.NewMocked(t, t.Context())
	mockedState := d.MockedState()
	mockedState.Mapper().AddMapper(corefiles.NewMapper(mockedState))
	mockedState.Mapper().AddMapper(orchestrator.NewMapper(mockedState))
	return mockedState, d
}

func createTargetConfigs(t *testing.T, state *state.State) (*model.ConfigState, *model.ConfigState, *model.ConfigState) {
	t.Helper()

	// Target config 1
	targetConfigKey1 := model.ConfigKey{
		BranchID:    123,
		ComponentID: `foo.bar1`,
		ID:          `123`,
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
	require.NoError(t, state.Set(targetConfigState1))

	// Target config 2
	targetConfigKey2 := model.ConfigKey{
		BranchID:    123,
		ComponentID: `foo.bar2`,
		ID:          `789`,
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
	require.NoError(t, state.Set(targetConfigState2))

	// Target config 3
	targetConfigKey3 := model.ConfigKey{
		BranchID:    123,
		ComponentID: `foo.bar2`,
		ID:          `456`,
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
	require.NoError(t, state.Set(targetConfigState3))

	return targetConfigState1, targetConfigState2, targetConfigState3
}

func createLocalLoadFixtures(t *testing.T, state *state.State) *model.ConfigState {
	t.Helper()

	// Branch
	branchKey := model.BranchKey{
		ID: 123,
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
	require.NoError(t, state.Set(branchState))

	// Orchestrator config
	configKey := model.ConfigKey{
		BranchID:    123,
		ComponentID: keboola.OrchestratorComponentID,
		ID:          `456`,
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

	require.NoError(t, state.Set(configState))
	return configState
}

func createLocalSaveFixtures(t *testing.T, state *state.State, createTargets bool) *model.ConfigState {
	t.Helper()

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
	orchestration := &model.Orchestration{
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
				PhaseKey:  phase2Key,
				AbsPath:   model.NewAbsPath(`branch/other/orchestrator/phases`, `002-phase-with-deps`),
				DependsOn: []model.PhaseKey{phase1Key},
				Name:      `Phase With Deps`,
				Content:   orderedmap.New(),
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

	// Branch
	branchKey := model.BranchKey{
		ID: 123,
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
	require.NoError(t, state.Set(branchState))

	// Orchestrator config
	configKey := model.ConfigKey{
		BranchID:    123,
		ComponentID: keboola.OrchestratorComponentID,
		ID:          `456`,
	}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(`branch`, `other/orchestrator`),
			},
		},
		Remote: &model.Config{
			ConfigKey:     configKey,
			Name:          "My Orchestration",
			Content:       orderedmap.New(),
			Orchestration: orchestration,
		},
	}
	require.NoError(t, state.Set(configState))

	// Create targets
	if !createTargets {
		return configState
	}

	// Target config 1
	targetConfigKey1 := model.ConfigKey{
		BranchID:    123,
		ComponentID: `foo.bar1`,
		ID:          `123`,
	}
	targetConfigState1 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: targetConfigKey1,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(`branch/extractor`, `target-config-1`),
			},
		},
		Remote: &model.Config{ConfigKey: targetConfigKey1},
	}
	require.NoError(t, state.Set(targetConfigState1))

	// Target config 2
	targetConfigKey2 := model.ConfigKey{
		BranchID:    123,
		ComponentID: `foo.bar2`,
		ID:          `789`,
	}
	targetConfigState2 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: targetConfigKey2,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(`branch/extractor`, `target-config-2`),
			},
		},
		Remote: &model.Config{ConfigKey: targetConfigKey2},
	}
	require.NoError(t, state.Set(targetConfigState2))

	// Target config 3
	targetConfigKey3 := model.ConfigKey{
		BranchID:    123,
		ComponentID: `foo.bar2`,
		ID:          `456`,
	}
	targetConfigState3 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: targetConfigKey3,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(`branch/extractor`, `target-config-3`),
			},
		},
		Remote: &model.Config{ConfigKey: targetConfigKey3},
	}
	require.NoError(t, state.Set(targetConfigState3))

	return configState
}
