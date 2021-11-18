package orchestrator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestMapAfterRemoteLoad(t *testing.T) {
	t.Parallel()
	mapper, _, logs := createMapper(t)

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
	content := utils.NewOrderedMap()
	json.MustDecodeString(contentStr, content)
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.OrchestratorComponentId,
		Id:          `456`,
	}
	configManifest := &model.ConfigManifest{
		ConfigKey: configKey,
		Paths: model.Paths{
			PathInProject: model.NewPathInProject(`branch`, `config`),
		},
	}
	apiObject := &model.Config{ConfigKey: configKey, Content: content}
	internalObject := apiObject.Clone().(*model.Config)
	recipe := &model.RemoteLoadRecipe{Manifest: configManifest, ApiObject: apiObject, InternalObject: internalObject}

	// Invoke
	assert.Empty(t, apiObject.Relations)
	assert.Empty(t, internalObject.Relations)
	assert.NoError(t, mapper.MapAfterRemoteLoad(recipe))
	assert.Empty(t, logs.String())

	// ApiObject is not changed
	assert.Equal(t, strings.TrimLeft(contentStr, "\n"), json.MustEncodeString(apiObject.Content, true))
	assert.Nil(t, apiObject.Orchestration)

	// Internal object
	assert.Equal(t, `{}`, json.MustEncodeString(internalObject.Content, false))
	assert.Equal(t, &model.Orchestration{
		Phases: []*model.Phase{
			{
				PhaseKey: model.PhaseKey{
					BranchId:    123,
					ComponentId: model.OrchestratorComponentId,
					ConfigId:    `456`,
					Index:       0,
				},
				PathInProject: model.NewPathInProject(`branch/config/phases`, `001-phase`),
				DependsOn:     []model.PhaseKey{},
				Name:          `Phase`,
				Content:       utils.NewOrderedMap(),
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
						PathInProject: model.NewPathInProject(`branch/config/phases/001-phase`, `001-task-1`),
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
						PathInProject: model.NewPathInProject(`branch/config/phases/001-phase`, `002-task-3`),
						Name:          `Task 3`,
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
				PathInProject: model.NewPathInProject(`branch/config/phases`, `002-phase-with-deps`),
				DependsOn: []model.PhaseKey{
					{
						BranchId:    123,
						ComponentId: model.OrchestratorComponentId,
						ConfigId:    `456`,
						Index:       0,
					},
				},
				Name: `Phase With Deps`,
				Content: utils.PairsToOrderedMap([]utils.Pair{
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
						PathInProject: model.NewPathInProject(`branch/config/phases/002-phase-with-deps`, `001-task-2`),
						Name:          `Task 2`,
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
	}, internalObject.Orchestration)
}

func TestMapAfterRemoteLoadWarnings(t *testing.T) {
	t.Parallel()
	mapper, _, logs := createMapper(t)

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

	content := utils.NewOrderedMap()
	json.MustDecodeString(contentStr, content)
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.OrchestratorComponentId,
		Id:          `456`,
	}
	apiObject := &model.Config{ConfigKey: configKey, Content: content}
	internalObject := apiObject.Clone().(*model.Config)
	recipe := &model.RemoteLoadRecipe{Manifest: &model.ConfigManifest{}, ApiObject: apiObject, InternalObject: internalObject}

	// Invoke
	assert.Empty(t, apiObject.Relations)
	assert.Empty(t, internalObject.Relations)
	assert.NoError(t, mapper.MapAfterRemoteLoad(recipe))

	// Warnings
	expectedWarnings := `
WARN  Warning: invalid orchestrator config "branch:123/component:keboola.orchestrator/config:456":
  - invalid phase[1]:
    - missing "name" key
  - invalid phase[2]:
    - missing "id" key
    - missing "name" key
  - invalid task[1]:
    - phase "789" not found
  - invalid task[2]:
    - missing "id" key
    - missing "name" key
    - missing "phase" key
    - missing "task" key
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logs.String())

	// ApiObject is not changed
	assert.Equal(t, strings.TrimLeft(contentStr, "\n"), json.MustEncodeString(apiObject.Content, true))
	assert.Nil(t, apiObject.Orchestration)

	// Internal object
	assert.Equal(t, `{}`, json.MustEncodeString(internalObject.Content, false))
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
				Content:   utils.NewOrderedMap(),
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
						Content: utils.PairsToOrderedMap([]utils.Pair{
							{
								Key: `task`,
								Value: *utils.PairsToOrderedMap([]utils.Pair{
									{Key: `mode`, Value: `run`},
								}),
							},
						}),
					},
				},
			},
		},
	}, internalObject.Orchestration)
}

func TestMapAfterRemoteLoadSortByDeps(t *testing.T) {
	t.Parallel()
	mapper, _, logs := createMapper(t)

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

	content := utils.NewOrderedMap()
	json.MustDecodeString(contentStr, content)
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.OrchestratorComponentId,
		Id:          `456`,
	}
	apiObject := &model.Config{ConfigKey: configKey, Content: content}
	internalObject := apiObject.Clone().(*model.Config)
	recipe := &model.RemoteLoadRecipe{Manifest: &model.ConfigManifest{}, ApiObject: apiObject, InternalObject: internalObject}

	// Invoke
	assert.Empty(t, apiObject.Relations)
	assert.Empty(t, internalObject.Relations)
	assert.NoError(t, mapper.MapAfterRemoteLoad(recipe))
	assert.Empty(t, logs.String())

	// Internal object
	assert.Equal(t, `{}`, json.MustEncodeString(internalObject.Content, false))
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
				Content:   utils.NewOrderedMap(),
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
				Content: utils.NewOrderedMap(),
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
				Content:   utils.NewOrderedMap(),
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
				Content: utils.NewOrderedMap(),
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
				Content: utils.NewOrderedMap(),
			},
		},
	}, internalObject.Orchestration)
}

func TestMapAfterRemoteLoadDepsCycles(t *testing.T) {
	t.Parallel()
	mapper, _, logs := createMapper(t)

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

	content := utils.NewOrderedMap()
	json.MustDecodeString(contentStr, content)
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.OrchestratorComponentId,
		Id:          `456`,
	}
	apiObject := &model.Config{ConfigKey: configKey, Content: content}
	internalObject := apiObject.Clone().(*model.Config)
	recipe := &model.RemoteLoadRecipe{Manifest: &model.ConfigManifest{}, ApiObject: apiObject, InternalObject: internalObject}

	// Invoke
	assert.Empty(t, apiObject.Relations)
	assert.Empty(t, internalObject.Relations)
	assert.NoError(t, mapper.MapAfterRemoteLoad(recipe))

	// Warnings
	expectedWarnings := `
WARN  Warning: invalid orchestrator config "branch:123/component:keboola.orchestrator/config:456":
  - found cycles in phases "dependsOn"
    - 3 -> 4 -> 3
    - 1 -> 2 -> 1
    - 5 -> 8 -> 7 -> 6 -> 5
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logs.String())
}
