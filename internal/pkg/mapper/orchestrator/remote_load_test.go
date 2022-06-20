package orchestrator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestOrchestratorMapAfterRemoteLoad(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	contentStr := `
{
  "phases": [
    {
      "id": 456,
      "name": "Phase With Deps",
      "dependsOn": [
        123
      ],
      "foo": "bar"
    },
    {
      "id": 123,
      "name": "Phase",
      "dependsOn": []
    }
  ],
  "tasks": [
    {
      "id": 1001,
      "name": "Task 1",
      "phase": 123,
      "task": {
        "componentId": "foo.bar1",
        "configId": "123",
        "mode": "run"
      },
      "continueOnFailure": false,
      "enabled": true
    },
    {
      "id": 1002,
      "name": "Task 2",
      "phase": 456,
      "task": {
        "componentId": "foo.bar2",
        "configId": "456",
        "mode": "run"
      },
      "continueOnFailure": false,
      "enabled": true
    },
    {
      "id": 1003,
      "name": "Task 3",
      "phase": 123,
      "task": {
        "componentId": "foo.bar2",
        "configId": "789",
        "mode": "run"
      },
      "continueOnFailure": false,
      "enabled": false
    }
  ]
}
`
	// Orchestrator config
	content := orderedmap.New()
	json.MustDecodeString(contentStr, content)
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: storageapi.OrchestratorComponentID,
		Id:          `456`,
	}
	configManifest := &model.ConfigManifest{
		ConfigKey: configKey,
		Paths: model.Paths{
			AbsPath: model.NewAbsPath(`branch`, `config`),
		},
	}
	config := &model.Config{ConfigKey: configKey, Content: content}
	configState := &model.ConfigState{
		ConfigManifest: configManifest,
		Remote:         config,
	}
	assert.NoError(t, state.Set(configState))

	// Target configs
	target1, target2, target3 := createTargetConfigs(t, state)

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(configState)
	assert.NoError(t, state.Mapper().AfterRemoteOperation(context.Background(), changes))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Check target configs relation
	rel1, err := target1.Remote.Relations.GetOneByType(model.UsedInOrchestratorRelType)
	assert.NoError(t, err)
	assert.Equal(t, config.Id, rel1.(*model.UsedInOrchestratorRelation).ConfigId)
	rel2, err := target2.Remote.Relations.GetOneByType(model.UsedInOrchestratorRelType)
	assert.NoError(t, err)
	assert.Equal(t, config.Id, rel2.(*model.UsedInOrchestratorRelation).ConfigId)
	rel3, err := target3.Remote.Relations.GetOneByType(model.UsedInOrchestratorRelType)
	assert.NoError(t, err)
	assert.Equal(t, config.Id, rel3.(*model.UsedInOrchestratorRelation).ConfigId)

	// Assert orchestration
	assert.Equal(t, `{}`, json.MustEncodeString(config.Content, false))
	assert.Equal(t, &model.Orchestration{
		Phases: []*model.Phase{
			{
				PhaseKey: model.PhaseKey{
					BranchId:    123,
					ComponentId: storageapi.OrchestratorComponentID,
					ConfigId:    `456`,
					Index:       0,
				},
				AbsPath:   model.NewAbsPath(`branch/config/phases`, `001-phase`),
				DependsOn: []model.PhaseKey{},
				Name:      `Phase`,
				Content:   orderedmap.New(),
				Tasks: []*model.Task{
					{
						TaskKey: model.TaskKey{
							PhaseKey: model.PhaseKey{
								BranchId:    123,
								ComponentId: storageapi.OrchestratorComponentID,
								ConfigId:    `456`,
								Index:       0,
							},
							Index: 0,
						},
						AbsPath:     model.NewAbsPath(`branch/config/phases/001-phase`, `001-task-1`),
						Name:        `Task 1`,
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
							{Key: `enabled`, Value: true},
						}),
					},
					{
						TaskKey: model.TaskKey{
							PhaseKey: model.PhaseKey{
								BranchId:    123,
								ComponentId: storageapi.OrchestratorComponentID,
								ConfigId:    `456`,
								Index:       0,
							},
							Index: 1,
						},
						AbsPath:     model.NewAbsPath(`branch/config/phases/001-phase`, `002-task-3`),
						Name:        `Task 3`,
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
							{Key: `enabled`, Value: false},
						}),
					},
				},
			},
			{
				PhaseKey: model.PhaseKey{
					BranchId:    123,
					ComponentId: storageapi.OrchestratorComponentID,
					ConfigId:    `456`,
					Index:       1,
				},
				AbsPath: model.NewAbsPath(`branch/config/phases`, `002-phase-with-deps`),
				DependsOn: []model.PhaseKey{
					{
						BranchId:    123,
						ComponentId: storageapi.OrchestratorComponentID,
						ConfigId:    `456`,
						Index:       0,
					},
				},
				Name: `Phase With Deps`,
				Content: orderedmap.FromPairs([]orderedmap.Pair{
					{Key: `foo`, Value: `bar`},
				}),
				Tasks: []*model.Task{
					{
						TaskKey: model.TaskKey{
							PhaseKey: model.PhaseKey{
								BranchId:    123,
								ComponentId: storageapi.OrchestratorComponentID,
								ConfigId:    `456`,
								Index:       1,
							},
							Index: 0,
						},
						AbsPath:     model.NewAbsPath(`branch/config/phases/002-phase-with-deps`, `001-task-2`),
						Name:        `Task 2`,
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
							{Key: `enabled`, Value: true},
						}),
					},
				},
			},
		},
	}, config.Orchestration)
}

func TestMapAfterRemoteLoadWarnings(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	contentStr := `
{
  "phases": [
    {
      "id": 123,
      "name": "Phase",
      "dependsOn": []
    },
    {
      "id": 456
    },
    {}
  ],
  "tasks": [
    {
      "id": 1001,
      "name": "Task 1",
      "phase": 123,
      "task": {
        "componentId": "foo.bar1",
        "configId": "123",
        "mode": "run"
      }
    },
    {
      "id": 1002,
      "name": "Task 2",
      "phase": 789,
      "task": {
        "componentId": "foo.bar2",
        "configId": "456",
        "mode": "run"
      }
    },
    {}
  ]
}
`

	// Orchestrator
	content := orderedmap.New()
	json.MustDecodeString(contentStr, content)
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: storageapi.OrchestratorComponentID,
		Id:          `456`,
	}
	configManifest := &model.ConfigManifest{
		ConfigKey: configKey,
	}
	config := &model.Config{ConfigKey: configKey, Content: content}
	configState := &model.ConfigState{
		ConfigManifest: configManifest,
		Remote:         config,
	}
	assert.NoError(t, state.Set(configState))

	// Target configs
	createTargetConfigs(t, state)

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(configState)
	assert.NoError(t, state.Mapper().AfterRemoteOperation(context.Background(), changes))

	// Warnings
	expectedWarnings := `
WARN  Warning: invalid orchestrator config "branch:123/component:keboola.orchestrator/config:456":
  - invalid phase[1]: missing "name" key
  - invalid phase[2]:
    - missing "id" key
    - missing "name" key
  - invalid task[1]: phase "789" not found
  - invalid task[2]:
    - missing "id" key
    - missing "name" key
    - missing "phase" key
    - missing "task" key
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logger.WarnAndErrorMessages())

	// Assert orchestration
	assert.Equal(t, `{}`, json.MustEncodeString(config.Content, false))
	assert.Equal(t, &model.Orchestration{
		Phases: []*model.Phase{
			{
				PhaseKey: model.PhaseKey{
					BranchId:    123,
					ComponentId: storageapi.OrchestratorComponentID,
					ConfigId:    `456`,
					Index:       0,
				},
				DependsOn: []model.PhaseKey{},
				Name:      `Phase`,
				Content:   orderedmap.New(),
				Tasks: []*model.Task{
					{
						TaskKey: model.TaskKey{
							PhaseKey: model.PhaseKey{
								BranchId:    123,
								ComponentId: storageapi.OrchestratorComponentID,
								ConfigId:    `456`,
								Index:       0,
							},
							Index: 0,
						},
						Name:        `Task 1`,
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
						}),
					},
				},
			},
		},
	}, config.Orchestration)
}

func TestMapAfterRemoteLoadSortByDeps(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	contentStr := `
{
  "phases": [
    {
      "id": 1,
      "name": "Phase 1",
      "dependsOn": [5]
    },
    {
      "id": 2,
      "name": "Phase 2",
      "dependsOn": []
    },
    {
      "id": 3,
      "name": "Phase 3",
      "dependsOn": [1, 4, 5]
    },
    {
      "id": 4,
      "name": "Phase 4",
      "dependsOn": [2, 5]
    },
    {
      "id": 5,
      "name": "Phase 5",
      "dependsOn": []
    }
  ],
  "tasks": []
}
`

	content := orderedmap.New()
	json.MustDecodeString(contentStr, content)
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: storageapi.OrchestratorComponentID,
		Id:          `456`,
	}
	configManifest := &model.ConfigManifest{
		ConfigKey: configKey,
	}
	config := &model.Config{ConfigKey: configKey, Content: content}
	configState := &model.ConfigState{
		ConfigManifest: configManifest,
		Remote:         config,
	}
	assert.NoError(t, state.Set(configState))

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(configState)
	assert.NoError(t, state.Mapper().AfterRemoteOperation(context.Background(), changes))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Internal object
	assert.Equal(t, `{}`, json.MustEncodeString(config.Content, false))
	assert.Equal(t, &model.Orchestration{
		Phases: []*model.Phase{
			{
				PhaseKey: model.PhaseKey{
					BranchId:    123,
					ComponentId: storageapi.OrchestratorComponentID,
					ConfigId:    `456`,
					Index:       0,
				},
				DependsOn: []model.PhaseKey{},
				Name:      `Phase 5`,
				Content:   orderedmap.New(),
			},
			{
				PhaseKey: model.PhaseKey{
					BranchId:    123,
					ComponentId: storageapi.OrchestratorComponentID,
					ConfigId:    `456`,
					Index:       1,
				},
				DependsOn: []model.PhaseKey{
					{
						BranchId:    123,
						ComponentId: storageapi.OrchestratorComponentID,
						ConfigId:    `456`,
						Index:       0,
					},
				},
				Name:    `Phase 1`,
				Content: orderedmap.New(),
			},
			{
				PhaseKey: model.PhaseKey{
					BranchId:    123,
					ComponentId: storageapi.OrchestratorComponentID,
					ConfigId:    `456`,
					Index:       2,
				},
				DependsOn: []model.PhaseKey{},
				Name:      `Phase 2`,
				Content:   orderedmap.New(),
			},
			{
				PhaseKey: model.PhaseKey{
					BranchId:    123,
					ComponentId: storageapi.OrchestratorComponentID,
					ConfigId:    `456`,
					Index:       3,
				},
				DependsOn: []model.PhaseKey{
					{
						BranchId:    123,
						ComponentId: storageapi.OrchestratorComponentID,
						ConfigId:    `456`,
						Index:       0,
					},
					{
						BranchId:    123,
						ComponentId: storageapi.OrchestratorComponentID,
						ConfigId:    `456`,
						Index:       2,
					},
				},
				Name:    `Phase 4`,
				Content: orderedmap.New(),
			},
			{
				PhaseKey: model.PhaseKey{
					BranchId:    123,
					ComponentId: storageapi.OrchestratorComponentID,
					ConfigId:    `456`,
					Index:       4,
				},
				DependsOn: []model.PhaseKey{
					{
						BranchId:    123,
						ComponentId: storageapi.OrchestratorComponentID,
						ConfigId:    `456`,
						Index:       0,
					},
					{
						BranchId:    123,
						ComponentId: storageapi.OrchestratorComponentID,
						ConfigId:    `456`,
						Index:       1,
					},
					{
						BranchId:    123,
						ComponentId: storageapi.OrchestratorComponentID,
						ConfigId:    `456`,
						Index:       3,
					},
				},
				Name:    `Phase 3`,
				Content: orderedmap.New(),
			},
		},
	}, config.Orchestration)
}

func TestMapAfterRemoteLoadDepsCycles(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	contentStr := `
{
  "phases": [
    {
      "id": 1,
      "name": "Phase 1",
      "dependsOn": [2]
    },
    {
      "id": 2,
      "name": "Phase 2",
      "dependsOn": [3, 1]
    },
    {
      "id": 3,
      "name": "Phase 3",
      "dependsOn": [4]
    },
    {
      "id": 4,
      "name": "Phase 4",
      "dependsOn": [3]
    },
    {
      "id": 5,
      "name": "Phase 5",
      "dependsOn": [8]
    },
    {
      "id": 6,
      "name": "Phase 6",
      "dependsOn": [5]
    },
    {
      "id": 7,
      "name": "Phase 7",
      "dependsOn": [6]
    },
    {
      "id": 8,
      "name": "Phase 8",
      "dependsOn": [7]
    }
  ],
  "tasks": []
}
`

	content := orderedmap.New()
	json.MustDecodeString(contentStr, content)
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: storageapi.OrchestratorComponentID,
		Id:          `456`,
	}
	configManifest := &model.ConfigManifest{
		ConfigKey: configKey,
		Paths: model.Paths{
			AbsPath: model.NewAbsPath(`branch`, `config`),
		},
	}
	config := &model.Config{ConfigKey: configKey, Content: content}
	configState := &model.ConfigState{
		ConfigManifest: configManifest,
		Remote:         config,
	}
	assert.NoError(t, state.Set(configState))

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(configState)
	assert.NoError(t, state.Mapper().AfterRemoteOperation(context.Background(), changes))

	// Warnings
	expectedWarnings := `
WARN  Warning: invalid orchestrator config "branch:123/component:keboola.orchestrator/config:456":
  - found cycles in phases "dependsOn":
    - 3 -> 4 -> 3
    - 1 -> 2 -> 1
    - 5 -> 8 -> 7 -> 6 -> 5
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logger.WarnAndErrorMessages())
}
