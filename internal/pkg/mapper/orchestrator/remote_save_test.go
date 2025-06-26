package orchestrator_test

import (
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestMapBeforeRemoteSave(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

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
				DependsOn: []model.PhaseKey{},
				Name:      `Phase`,
				Content: orderedmap.FromPairs([]orderedmap.Pair{
					{Key: `foo`, Value: `bar`},
				}),
				Tasks: []*model.Task{
					{
						TaskKey:     model.TaskKey{PhaseKey: phase1Key, Index: 0},
						Name:        `Task 1`,
						Enabled:     true,
						ComponentID: `foo.bar1`,
						ConfigID:    `123`,
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
						Name:        `Task 3`,
						Enabled:     false,
						ComponentID: `foo.bar2`,
						ConfigID:    `789`,
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
				DependsOn: []model.PhaseKey{{Index: 0}},
				Name:      `Phase With Deps`,
				Content:   orderedmap.New(),
				Tasks: []*model.Task{
					{
						TaskKey:     model.TaskKey{PhaseKey: phase2Key, Index: 0},
						Name:        `Task 2`,
						Enabled:     true,
						ComponentID: `foo.bar2`,
						ConfigID:    `456`,
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
	}

	key := model.ConfigKey{
		BranchID:    123,
		ComponentID: keboola.OrchestratorComponentID,
		ID:          `456`,
	}
	manifest := &model.ConfigManifest{ConfigKey: key}
	object := &model.Config{ConfigKey: key, Content: orderedmap.New()}
	object.Orchestration = orchestration
	recipe := model.NewRemoteSaveRecipe(manifest, object, model.NewChangedFields("orchestration"))

	// Save
	require.NoError(t, state.Mapper().MapBeforeRemoteSave(t.Context(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Orchestration is stored in API object content
	expectedContent := `
{
  "phases": [
    {
      "id": "1",
      "name": "Phase",
      "dependsOn": [],
      "foo": "bar"
    },
    {
      "id": "2",
      "name": "Phase With Deps",
      "dependsOn": [
        "1"
      ]
    }
  ],
  "tasks": [
    {
      "id": "1",
      "name": "Task 1",
      "enabled": true,
      "phase": "1",
      "task": {
        "mode": "run",
        "componentId": "foo.bar1",
        "configId": "123"
      },
      "continueOnFailure": false
    },
    {
      "id": "2",
      "name": "Task 3",
      "enabled": false,
      "phase": "1",
      "task": {
        "mode": "run",
        "componentId": "foo.bar2",
        "configId": "789"
      },
      "continueOnFailure": false
    },
    {
      "id": "3",
      "name": "Task 2",
      "enabled": true,
      "phase": "2",
      "task": {
        "mode": "run",
        "componentId": "foo.bar2",
        "configId": "456"
      },
      "continueOnFailure": false
    },
    {
      "id": "4",
      "name": "Task 4 - ConfigData",
      "enabled": true,
      "phase": "2",
      "task": {
        "mode": "run",
        "componentId": "foo.bar3",
        "configData": {
          "params": "value"
        }
      },
      "continueOnFailure": false
    }
  ]
}
`
	assert.Nil(t, object.Orchestration)
	assert.Equal(t, strings.TrimLeft(expectedContent, "\n"), json.MustEncodeString(object.Content, true))
}
