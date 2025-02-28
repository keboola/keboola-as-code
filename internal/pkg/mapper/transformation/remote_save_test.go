package transformation_test

import (
	"context"
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestRemoteSaveTransformation(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	configState := createTestFixtures(t, "keboola.snowflake-transformation")

	blocks := []*model.Block{
		{
			Name: "001",
			Codes: model.Codes{
				{
					Name: "001-001",
					Scripts: model.Scripts{
						model.StaticScript{Value: "SELECT 1"},
					},
				},
				{
					Name: "001-002",
					Scripts: model.Scripts{
						model.StaticScript{Value: "SELECT 1;"},
						model.StaticScript{Value: "SELECT 2;"},
					},
				},
			},
		},
		{
			Name: "002",
			Codes: model.Codes{
				{
					Name: "002-001",
					Scripts: model.Scripts{
						model.StaticScript{Value: "SELECT 3"},
					},
				},
			},
		},
		{
			Name:  "003",
			Codes: model.Codes{},
		},
	}

	object := &model.Config{
		ConfigKey: configState.ConfigKey,
		Content:   orderedmap.New(),
	}
	object.Transformation = &model.Transformation{Blocks: blocks}
	recipe := model.NewRemoteSaveRecipe(configState.Manifest(), object, model.NewChangedFields("transformation"))

	// Save
	require.NoError(t, state.Mapper().MapBeforeRemoteSave(t.Context(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Blocks are stored in API object content
	expectedBlocks := `
[
  {
    "name": "001",
    "codes": [
      {
        "name": "001-001",
        "script": [
          "SELECT 1"
        ]
      },
      {
        "name": "001-002",
        "script": [
          "SELECT 1;",
          "SELECT 2;"
        ]
      }
    ]
  },
  {
    "name": "002",
    "codes": [
      {
        "name": "002-001",
        "script": [
          "SELECT 3"
        ]
      }
    ]
  },
  {
    "name": "003",
    "codes": []
  }
]
`
	assert.Empty(t, object.Transformation)
	apiBlocks := object.Content.GetNestedOrNil(`parameters.blocks`)
	assert.NotNil(t, blocks)
	assert.Equal(t, strings.TrimLeft(expectedBlocks, "\n"), json.MustEncodeString(apiBlocks, true))

	// Check changed fields
	assert.Equal(t, `configuration`, recipe.ChangedFields.String())
}
