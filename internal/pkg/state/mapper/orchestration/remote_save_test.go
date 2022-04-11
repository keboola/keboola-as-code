package orchestration_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestOrchestratorRemoteMapper_MapBeforeRemoteSave(t *testing.T) {
	t.Parallel()
	state, d := createRemoteStateWithMapper(t)
	logger := d.DebugLogger()

	config := createRemoteSaveFixtures(state)
	recipe := model.NewRemoteSaveRecipe(config, model.NewChangedFields("orchestration"))

	// Save
	assert.NoError(t, state.Mapper().MapBeforeRemoteSave(recipe))
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
	assert.Nil(t, config.Orchestration)
	assert.Equal(t, strings.TrimLeft(expectedContent, "\n"), json.MustEncodeString(config.Content, true))
}
