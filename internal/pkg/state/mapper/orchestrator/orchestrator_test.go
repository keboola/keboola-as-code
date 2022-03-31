package orchestrator_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper/corefiles"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func createLocalStateWithMapper(t *testing.T) (*local.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyLocalState()
	mockedState.Mapper().AddMapper(corefiles.NewLocalMapper(mockedState))
	mockedState.Mapper().AddMapper(orchestrator.NewLocalMapper(mockedState, d))
	return mockedState, d
}

func createRemoteStateWithMapper(t *testing.T) (*remote.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyRemoteState()
	mockedState.Mapper().AddMapper(orchestrator.NewRemoteMapper(mockedState, d))
	return mockedState, d
}

func createTargetConfigs(t *testing.T, objects model.Objects, naming *naming.Registry) (*model.Config, *model.Config, *model.Config) {
	t.Helper()

	// Target config 1
	targetConfigKey1 := model.ConfigKey{
		BranchId:    123,
		ComponentId: `foo.bar1`,
		Id:          `123`,
	}
	targetConfig1 := &model.Config{ConfigKey: targetConfigKey1}
	objects.MustAdd(targetConfig1)
	if naming != nil {
		naming.MustAttach(targetConfigKey1, model.NewAbsPath(`branch/extractor`, `target-config-1`))
	}

	// Target config 2
	targetConfigKey2 := model.ConfigKey{
		BranchId:    123,
		ComponentId: `foo.bar2`,
		Id:          `789`,
	}
	targetConfig2 := &model.Config{ConfigKey: targetConfigKey2}
	objects.MustAdd(targetConfig2)
	if naming != nil {
		naming.MustAttach(targetConfigKey2, model.NewAbsPath(`branch/extractor`, `target-config-2`))
	}

	// Target config 3
	targetConfigKey3 := model.ConfigKey{
		BranchId:    123,
		ComponentId: `foo.bar2`,
		Id:          `456`,
	}
	targetConfig3 := &model.Config{ConfigKey: targetConfigKey3}
	objects.MustAdd(targetConfig3)
	if naming != nil {
		naming.MustAttach(targetConfigKey3, model.NewAbsPath(`branch/extractor`, `target-config-3`))
	}

	return targetConfig1, targetConfig2, targetConfig3
}

func createLocalLoadFixtures(t *testing.T, state *local.State) (*model.Config, model.AbsPath) {
	t.Helper()

	// Branch
	branchKey := model.BranchKey{Id: 123}
	branch := &model.Branch{BranchKey: branchKey}
	state.MustAdd(branch)
	state.NamingRegistry().MustAttach(branchKey, model.NewAbsPath(``, `branch`))

	// Orchestrator config
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.OrchestratorComponentId,
		Id:          `456`,
	}
	config := &model.Config{ConfigKey: configKey, Content: orderedmap.New()}
	configPath := model.NewAbsPath(`branch`, `other/orchestrator`)
	state.MustAdd(config)
	state.NamingRegistry().MustAttach(configKey, configPath)

	return config, configPath
}

func createLocalSaveFixtures(t *testing.T, state *local.State, createTargets bool) (*model.Config, model.AbsPath) {
	t.Helper()

	phase1Key := model.PhaseKey{
		BranchId:    123,
		ComponentId: model.OrchestratorComponentId,
		ConfigId:    `456`,
		Index:       0,
	}

	phase2Key := model.PhaseKey{
		BranchId:    123,
		ComponentId: model.OrchestratorComponentId,
		ConfigId:    `456`,
		Index:       1,
	}

	task1Key := model.TaskKey{PhaseKey: phase1Key, Index: 0}
	task2Key := model.TaskKey{PhaseKey: phase1Key, Index: 1}
	task3Key := model.TaskKey{PhaseKey: phase2Key, Index: 0}

	orchestration := &model.Orchestration{
		Phases: []*model.Phase{
			{
				PhaseKey:  phase1Key,
				DependsOn: []model.PhaseKey{},
				Name:      `Phase`,
				Content: orderedmap.FromPairs([]orderedmap.Pair{
					{Key: `foo`, Value: `bar`},
				}),
				Tasks: []*model.Task{
					{
						TaskKey:     task1Key,
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
						TaskKey:     task2Key,
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
				PhaseKey: phase2Key,
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
						TaskKey:     task3Key,
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

	// Branch
	branchKey := model.BranchKey{Id: 123}
	branch := &model.Branch{BranchKey: branchKey}
	state.MustAdd(branch)
	state.NamingRegistry().MustAttach(branchKey, model.NewAbsPath(``, `branch`))

	// Orchestrator config
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.OrchestratorComponentId,
		Id:          `456`,
	}
	config := &model.Config{
		ConfigKey:     configKey,
		Name:          "My Orchestration",
		Content:       orderedmap.New(),
		Orchestration: orchestration,
	}
	configPath := model.NewAbsPath(`branch`, `other/orchestrator`)
	state.MustAdd(config)
	state.NamingRegistry().MustAttach(configKey, configPath)

	// Create targets
	if createTargets {
		createTargetConfigs(t, state, state.NamingRegistry())
	}

	return config, configPath
}

func createRemoteSaveFixtures(state *remote.State) *model.Config {
	orchestration := &model.Orchestration{
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
						Name:        `Task 3`,
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
				DependsOn: []model.PhaseKey{{Index: 0}},
				Name:      `Phase With Deps`,
				Content:   orderedmap.New(),
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
						Name:        `Task 2`,
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

	// Branch
	branchKey := model.BranchKey{Id: 123}
	branch := &model.Branch{BranchKey: branchKey}
	state.MustAdd(branch)

	// Orchestrator config
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.OrchestratorComponentId,
		Id:          `456`,
	}
	config := &model.Config{ConfigKey: configKey, Content: orderedmap.New()}
	config.Orchestration = orchestration
	return config
}
