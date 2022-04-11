package orchestration_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestOrchestratorRemoteMapper_AfterRemoteOperation(t *testing.T) {
	t.Parallel()
	state, d := createRemoteStateWithMapper(t)
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
	// Branch
	branchKey := model.BranchKey{BranchId: 123}
	branch := &model.Branch{BranchKey: branchKey}
	state.MustAdd(branch)

	// Orchestrator config
	content := orderedmap.New()
	json.MustDecodeString(contentStr, content)
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.OrchestratorComponentId,
		ConfigId:    `456`,
	}
	config := &model.Config{ConfigKey: configKey, Content: content}
	state.MustAdd(config)

	// Target configs
	target1, target2, target3 := createTargetConfigs(t, state, nil)

	// Invoke
	assert.NoError(t, state.Mapper().AfterRemoteOperation(model.NewChanges().AddLoaded(config)))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Check target configs relation
	rel1, err := target1.Relations.GetOneByType(model.UsedInOrchestratorRelType)
	assert.NoError(t, err)
	assert.Equal(t, config.ConfigId, rel1.(*model.UsedInOrchestratorRelation).ConfigId)
	rel2, err := target2.Relations.GetOneByType(model.UsedInOrchestratorRelType)
	assert.NoError(t, err)
	assert.Equal(t, config.ConfigId, rel2.(*model.UsedInOrchestratorRelation).ConfigId)
	rel3, err := target3.Relations.GetOneByType(model.UsedInOrchestratorRelType)
	assert.NoError(t, err)
	assert.Equal(t, config.ConfigId, rel3.(*model.UsedInOrchestratorRelation).ConfigId)

	// Assert orchestration
	assert.Equal(t, `{}`, json.MustEncodeString(config.Content, false))
	assert.Equal(t, &model.Orchestration{
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
				Content:   orderedmap.New(),
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
				DependsOn: []model.PhaseKey{
					{
						BranchId:    123,
						ComponentId: model.OrchestratorComponentId,
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
	}, config.Orchestration)
}

func TestOrchestratorRemoteMapper_AfterRemoteOperation_Warnings(t *testing.T) {
	t.Parallel()
	state, d := createRemoteStateWithMapper(t)
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

	// Branch
	branchKey := model.BranchKey{BranchId: 123}
	branch := &model.Branch{BranchKey: branchKey}
	state.MustAdd(branch)

	// Orchestrator config
	content := orderedmap.New()
	json.MustDecodeString(contentStr, content)
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.OrchestratorComponentId,
		ConfigId:    `456`,
	}
	config := &model.Config{ConfigKey: configKey, Content: content}
	state.MustAdd(config)

	// Target configs
	createTargetConfigs(t, state, nil)

	// Invoke
	assert.NoError(t, state.Mapper().AfterRemoteOperation(model.NewChanges().AddLoaded(config)))

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
					ComponentId: model.OrchestratorComponentId,
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
						}),
					},
				},
			},
		},
	}, config.Orchestration)
}

func TestOrchestratorRemoteMapper_AfterRemoteOperation_Sort(t *testing.T) {
	t.Parallel()
	state, d := createRemoteStateWithMapper(t)
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

	// Branch
	branchKey := model.BranchKey{BranchId: 123}
	branch := &model.Branch{BranchKey: branchKey}
	state.MustAdd(branch)

	// Orchestrator config
	content := orderedmap.New()
	json.MustDecodeString(contentStr, content)
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.OrchestratorComponentId,
		ConfigId:    `456`,
	}
	config := &model.Config{ConfigKey: configKey, Content: content}
	state.MustAdd(config)

	// Invoke
	assert.NoError(t, state.Mapper().AfterRemoteOperation(model.NewChanges().AddLoaded(config)))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Internal object
	assert.Equal(t, `{}`, json.MustEncodeString(config.Content, false))
	assert.Equal(t, &model.Orchestration{
		Phases: []*model.Phase{
			{
				PhaseKey: model.PhaseKey{
					BranchId:    123,
					ComponentId: model.OrchestratorComponentId,
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
				Name:    `Phase 1`,
				Content: orderedmap.New(),
			},
			{
				PhaseKey: model.PhaseKey{
					BranchId:    123,
					ComponentId: model.OrchestratorComponentId,
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
					ComponentId: model.OrchestratorComponentId,
					ConfigId:    `456`,
					Index:       3,
				},
				DependsOn: []model.PhaseKey{
					{
						BranchId:    123,
						ComponentId: model.OrchestratorComponentId,
						ConfigId:    `456`,
						Index:       0,
					},
					{
						BranchId:    123,
						ComponentId: model.OrchestratorComponentId,
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
					ComponentId: model.OrchestratorComponentId,
					ConfigId:    `456`,
					Index:       4,
				},
				DependsOn: []model.PhaseKey{
					{
						BranchId:    123,
						ComponentId: model.OrchestratorComponentId,
						ConfigId:    `456`,
						Index:       0,
					},
					{
						BranchId:    123,
						ComponentId: model.OrchestratorComponentId,
						ConfigId:    `456`,
						Index:       1,
					},
					{
						BranchId:    123,
						ComponentId: model.OrchestratorComponentId,
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

func TestOrchestratorRemoteMapper_AfterRemoteOperation_DependsOnCycles(t *testing.T) {
	t.Parallel()
	state, d := createRemoteStateWithMapper(t)
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

	// Branch
	branchKey := model.BranchKey{BranchId: 123}
	branch := &model.Branch{BranchKey: branchKey}
	state.MustAdd(branch)

	// Orchestrator config
	content := orderedmap.New()
	json.MustDecodeString(contentStr, content)
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.OrchestratorComponentId,
		ConfigId:    `456`,
	}
	config := &model.Config{ConfigKey: configKey, Content: content}
	state.MustAdd(config)

	// Invoke
	assert.NoError(t, state.Mapper().AfterRemoteOperation(model.NewChanges().AddLoaded(config)))

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
