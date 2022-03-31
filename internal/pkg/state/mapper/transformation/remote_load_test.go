package transformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestTransformationRemoteMapper_MapAfterRemoteLoad(t *testing.T) {
	t.Parallel()
	state, d := createRemoteStateWithMapper(t)
	logger := d.DebugLogger()

	config, _ := createTestFixtures(t, "keboola.snowflake-transformation", state)

	// Api representation
	configInApi := `
{
  "parameters": {
    "blocks": [
      {
        "name": "block-1",
        "codes": [
          {
            "name": "code-1",
            "script": [
              "SELECT 1"
            ]
          },
          {
            "name": "code-2",
            "script": [
              "SELECT 1;",
              "SELECT 2;"
            ]
          }
        ]
      },
      {
        "name": "block-2",
        "codes": [
          {
            "name": "code-3",
            "script": [
              "SELECT 3"
            ]
          }
        ]
      }
    ]
  }
}
`

	// Load

	json.MustDecodeString(configInApi, config.Content)
	recipe := model.NewRemoteLoadRecipe(config)
	assert.NoError(t, state.Mapper().MapAfterRemoteLoad(recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Internal representation
	expected := []*model.Block{
		{
			BlockKey: model.BlockKey{Parent: config.ConfigKey, Index: 0},
			Name:     "block-1",
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{Parent: model.BlockKey{Parent: config.ConfigKey, Index: 0}, Index: 0},
					Name:    "code-1",
					Scripts: model.Scripts{
						model.StaticScript{Value: "SELECT 1"},
					},
				},
				{
					CodeKey: model.CodeKey{Parent: model.BlockKey{Parent: config.ConfigKey, Index: 0}, Index: 1},
					Name:    "code-2",
					Scripts: model.Scripts{
						model.StaticScript{Value: "SELECT 1;"},
						model.StaticScript{Value: "SELECT 2;"},
					},
				},
			},
		},
		{
			BlockKey: model.BlockKey{Parent: config.ConfigKey, Index: 1},
			Name:     "block-2",
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{Parent: model.BlockKey{Parent: config.ConfigKey, Index: 1}, Index: 0},
					Name:    "code-3",
					Scripts: model.Scripts{
						model.StaticScript{Value: "SELECT 3"},
					},
				},
			},
		},
	}

	// Blocks have been moved from Content to config.Transformation.Blocks
	assert.Equal(t, `{"parameters":{}}`, json.MustEncodeString(config.Content, false))
	assert.Equal(t, expected, config.Transformation.Blocks)
}
