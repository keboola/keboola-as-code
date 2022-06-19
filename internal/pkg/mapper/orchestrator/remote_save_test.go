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

func TestMapBeforeRemoteSave(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	orchestration := &model.Orchestration{
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
								ComponentId: storageapi.OrchestratorComponentID,
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
					ComponentId: storageapi.OrchestratorComponentID,
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
								ComponentId: storageapi.OrchestratorComponentID,
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

	key := model.ConfigKey{
		BranchId:    123,
		ComponentId: storageapi.OrchestratorComponentID,
		Id:          `456`,
	}
	manifest := &model.ConfigManifest{ConfigKey: key}
	object := &model.Config{ConfigKey: key, Content: orderedmap.New()}
	object.Orchestration = orchestration
	recipe := model.NewRemoteSaveRecipe(manifest, object, model.NewChangedFields("orchestration"))

	// Save
	assert.NoError(t, state.Mapper().MapBeforeRemoteSave(context.Background(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Orchestration is stored in API object content
	expectedContent := `
{
  "phases": [
    {
      "id": 1,
      "name": "Phase",
      "dependsOn": [],
      "foo": "bar"
    },
    {
      "id": 2,
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
	assert.Nil(t, object.Orchestration)
	assert.Equal(t, strings.TrimLeft(expectedContent, "\n"), json.MustEncodeString(object.Content, true))
}
