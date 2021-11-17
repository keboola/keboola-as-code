package orchestrator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestMapBeforeRemoteSave(t *testing.T) {
	t.Parallel()
	context, logs := createMapperContext(t)
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
				DependsOn: []model.PhaseKey{{Index: 0}},
				Name:      `Phase With Deps`,
				Content:   utils.NewOrderedMap(),
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
	}

	key := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.OrchestratorComponentId,
		Id:          `456`,
	}
	record := &model.ConfigManifest{ConfigKey: key}
	internalObject := &model.Config{ConfigKey: key, Content: utils.NewOrderedMap()}
	internalObject.Orchestration = orchestration
	apiObject := internalObject.Clone().(*model.Config)
	recipe := &model.RemoteSaveRecipe{
		ChangedFields:  model.NewChangedFields("orchestration"),
		Manifest:       record,
		InternalObject: internalObject,
		ApiObject:      apiObject,
	}

	// Save
	assert.NoError(t, NewMapper(context).MapBeforeRemoteSave(recipe))
	assert.Empty(t, logs.String())

	// Internal object is not modified
	assert.NotNil(t, internalObject.Orchestration)
	assert.Nil(t, utils.GetFromMap(internalObject.Content, []string{`parameters`, `orchestration`}))

	// Orchestration is stored in API object content
	expectedContent := `
{
  "phases": [
    {
      "name": "Phase",
      "dependsOn": [],
      "foo": "bar"
    },
    {
      "name": "Phase With Deps",
      "dependsOn": [
        1
      ]
    }
  ],
  "tasks": [
    {
      "id": 1,
      "name": "Task 1",
      "phase": 1,
      "task": {
        "mode": "run",
        "componentId": "foo.bar1",
        "configId": "123"
      },
      "continueOnFailure": false,
      "enabled": true
    },
    {
      "id": 2,
      "name": "Task 3",
      "phase": 1,
      "task": {
        "mode": "run",
        "componentId": "foo.bar2",
        "configId": "789"
      },
      "continueOnFailure": false,
      "enabled": false
    },
    {
      "id": 3,
      "name": "Task 2",
      "phase": 2,
      "task": {
        "mode": "run",
        "componentId": "foo.bar2",
        "configId": "456"
      },
      "continueOnFailure": false,
      "enabled": true
    }
  ]
}
`
	assert.Nil(t, apiObject.Orchestration)
	assert.Equal(t, strings.TrimLeft(expectedContent, "\n"), json.MustEncodeString(apiObject.Content, true))
}
