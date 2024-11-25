package orchestrator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
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
      "enabled": true,
      "phase": 123,
      "task": {
        "componentId": "foo.bar1",
        "configId": "123",
        "mode": "run"
      },
      "continueOnFailure": false
    },
    {
      "id": 1002,
      "enabled": true,
      "name": "Task 2",
      "phase": 456,
      "task": {
        "componentId": "foo.bar2",
        "configId": "456",
        "mode": "run"
      },
      "continueOnFailure": false
    },
    {
      "id": 1003,
      "enabled": false,
      "name": "Task 3",
      "phase": 123,
      "task": {
        "componentId": "foo.bar2",
        "configId": "789",
        "mode": "run"
      },
      "continueOnFailure": false
    },
    {
      "id": 1004,
      "enabled": true,
      "name": "Task 4 - ConfigData",
      "phase": 456,
      "task": {
        "componentId": "foo.bar3",
        "configData":{"params":"value"},
        "mode": "run"
      },
      "continueOnFailure": false
    }
  ]
}
`
	// Orchestrator config
	content := orderedmap.New()
	json.MustDecodeString(contentStr, content)
	configKey := model.ConfigKey{
		BranchID:    123,
		ComponentID: keboola.OrchestratorComponentID,
		ID:          `456`,
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
	require.NoError(t, state.Set(configState))

	// Target configs
	target1, target2, target3 := createTargetConfigs(t, state)

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(configState)
	require.NoError(t, state.Mapper().AfterRemoteOperation(context.Background(), changes))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Check target configs relation
	rel1, err := target1.Remote.Relations.GetOneByType(model.UsedInOrchestratorRelType)
	require.NoError(t, err)
	assert.Equal(t, config.ID, rel1.(*model.UsedInOrchestratorRelation).ConfigID)
	rel2, err := target2.Remote.Relations.GetOneByType(model.UsedInOrchestratorRelType)
	require.NoError(t, err)
	assert.Equal(t, config.ID, rel2.(*model.UsedInOrchestratorRelation).ConfigID)
	rel3, err := target3.Remote.Relations.GetOneByType(model.UsedInOrchestratorRelType)
	require.NoError(t, err)
	assert.Equal(t, config.ID, rel3.(*model.UsedInOrchestratorRelation).ConfigID)

	// Assert orchestration
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
	assert.Equal(t, `{}`, json.MustEncodeString(config.Content, false))
	assert.Equal(t, &model.Orchestration{
		Phases: []*model.Phase{
			{
				PhaseKey:  phase1Key,
				AbsPath:   model.NewAbsPath(`branch/config/phases`, `001-phase`),
				DependsOn: []model.PhaseKey{},
				Name:      `Phase`,
				Content:   orderedmap.New(),
				Tasks: []*model.Task{
					{
						TaskKey:     model.TaskKey{PhaseKey: phase1Key, Index: 0},
						AbsPath:     model.NewAbsPath(`branch/config/phases/001-phase`, `001-task-1`),
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
						AbsPath:     model.NewAbsPath(`branch/config/phases/001-phase`, `002-task-3`),
						Name:        `Task 3`,
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
				},
			},
			{
				PhaseKey: phase2Key,
				AbsPath:  model.NewAbsPath(`branch/config/phases`, `002-phase-with-deps`),
				DependsOn: []model.PhaseKey{
					{
						BranchID:    123,
						ComponentID: keboola.OrchestratorComponentID,
						ConfigID:    `456`,
						Index:       0,
					},
				},
				Name: `Phase With Deps`,
				Content: orderedmap.FromPairs([]orderedmap.Pair{
					{Key: `foo`, Value: `bar`},
				}),
				Tasks: []*model.Task{
					{
						TaskKey:     model.TaskKey{PhaseKey: phase2Key, Index: 0},
						AbsPath:     model.NewAbsPath(`branch/config/phases/002-phase-with-deps`, `001-task-2`),
						Name:        `Task 2`,
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
						AbsPath:     model.NewAbsPath(`branch/config/phases/002-phase-with-deps`, `002-task-4-config-data`),
						Name:        `Task 4 - ConfigData`,
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
		BranchID:    123,
		ComponentID: keboola.OrchestratorComponentID,
		ID:          `456`,
	}
	configManifest := &model.ConfigManifest{
		ConfigKey: configKey,
	}
	config := &model.Config{ConfigKey: configKey, Content: content}
	configState := &model.ConfigState{
		ConfigManifest: configManifest,
		Remote:         config,
	}
	require.NoError(t, state.Set(configState))

	// Target configs
	createTargetConfigs(t, state)

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(configState)
	require.NoError(t, state.Mapper().AfterRemoteOperation(context.Background(), changes))

	// Warnings
	expectedWarnings := `
WARN  Warning:
- Invalid orchestrator config "branch:123/component:keboola.orchestrator/config:456":
  - Invalid phase[1]: Missing "name" key.
  - Invalid phase[2]:
    - Missing "id" key.
    - Missing "name" key.
  - Invalid task[1]: Phase "789" not found.
  - Invalid task[2]:
    - Missing "id" key.
    - Missing "name" key.
    - Missing "phase" key.
    - Missing "task" key.
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logger.AllMessagesTxt())

	// Assert orchestration
	assert.Equal(t, `{}`, json.MustEncodeString(config.Content, false))
	assert.Equal(t, &model.Orchestration{
		Phases: []*model.Phase{
			{
				PhaseKey: model.PhaseKey{
					BranchID:    123,
					ComponentID: keboola.OrchestratorComponentID,
					ConfigID:    `456`,
					Index:       0,
				},
				DependsOn: []model.PhaseKey{},
				Name:      `Phase`,
				Content:   orderedmap.New(),
				Tasks: []*model.Task{
					{
						TaskKey: model.TaskKey{
							PhaseKey: model.PhaseKey{
								BranchID:    123,
								ComponentID: keboola.OrchestratorComponentID,
								ConfigID:    `456`,
								Index:       0,
							},
							Index: 0,
						},
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
		BranchID:    123,
		ComponentID: keboola.OrchestratorComponentID,
		ID:          `456`,
	}
	configManifest := &model.ConfigManifest{
		ConfigKey: configKey,
	}
	config := &model.Config{ConfigKey: configKey, Content: content}
	configState := &model.ConfigState{
		ConfigManifest: configManifest,
		Remote:         config,
	}
	require.NoError(t, state.Set(configState))

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(configState)
	require.NoError(t, state.Mapper().AfterRemoteOperation(context.Background(), changes))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Internal object
	assert.Equal(t, `{}`, json.MustEncodeString(config.Content, false))
	assert.Equal(t, &model.Orchestration{
		Phases: []*model.Phase{
			{
				PhaseKey: model.PhaseKey{
					BranchID:    123,
					ComponentID: keboola.OrchestratorComponentID,
					ConfigID:    `456`,
					Index:       0,
				},
				DependsOn: []model.PhaseKey{},
				Name:      `Phase 5`,
				Content:   orderedmap.New(),
			},
			{
				PhaseKey: model.PhaseKey{
					BranchID:    123,
					ComponentID: keboola.OrchestratorComponentID,
					ConfigID:    `456`,
					Index:       1,
				},
				DependsOn: []model.PhaseKey{
					{
						BranchID:    123,
						ComponentID: keboola.OrchestratorComponentID,
						ConfigID:    `456`,
						Index:       0,
					},
				},
				Name:    `Phase 1`,
				Content: orderedmap.New(),
			},
			{
				PhaseKey: model.PhaseKey{
					BranchID:    123,
					ComponentID: keboola.OrchestratorComponentID,
					ConfigID:    `456`,
					Index:       2,
				},
				DependsOn: []model.PhaseKey{},
				Name:      `Phase 2`,
				Content:   orderedmap.New(),
			},
			{
				PhaseKey: model.PhaseKey{
					BranchID:    123,
					ComponentID: keboola.OrchestratorComponentID,
					ConfigID:    `456`,
					Index:       3,
				},
				DependsOn: []model.PhaseKey{
					{
						BranchID:    123,
						ComponentID: keboola.OrchestratorComponentID,
						ConfigID:    `456`,
						Index:       0,
					},
					{
						BranchID:    123,
						ComponentID: keboola.OrchestratorComponentID,
						ConfigID:    `456`,
						Index:       2,
					},
				},
				Name:    `Phase 4`,
				Content: orderedmap.New(),
			},
			{
				PhaseKey: model.PhaseKey{
					BranchID:    123,
					ComponentID: keboola.OrchestratorComponentID,
					ConfigID:    `456`,
					Index:       4,
				},
				DependsOn: []model.PhaseKey{
					{
						BranchID:    123,
						ComponentID: keboola.OrchestratorComponentID,
						ConfigID:    `456`,
						Index:       0,
					},
					{
						BranchID:    123,
						ComponentID: keboola.OrchestratorComponentID,
						ConfigID:    `456`,
						Index:       1,
					},
					{
						BranchID:    123,
						ComponentID: keboola.OrchestratorComponentID,
						ConfigID:    `456`,
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
		BranchID:    123,
		ComponentID: keboola.OrchestratorComponentID,
		ID:          `456`,
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
	require.NoError(t, state.Set(configState))

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(configState)
	require.NoError(t, state.Mapper().AfterRemoteOperation(context.Background(), changes))

	// Warnings
	expectedWarnings := `
WARN  Warning:
- Invalid orchestrator config "branch:123/component:keboola.orchestrator/config:456":
  - Found cycles in phases "dependsOn":
    - 3 -> 4 -> 3
    - 1 -> 2 -> 1
    - 5 -> 8 -> 7 -> 6 -> 5
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logger.AllMessagesTxt())
}
