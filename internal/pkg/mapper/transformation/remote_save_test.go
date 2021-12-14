package transformation_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/transformation"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestRemoteSaveTransformation(t *testing.T) {
	t.Parallel()
	context, configState := createTestFixtures(t, "keboola.snowflake-transformation")
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

	internalConfig := &model.Config{
		ConfigKey: configState.ConfigKey,
		Content:   orderedmap.New(),
	}
	internalConfig.Transformation = &model.Transformation{Blocks: blocks}
	apiConfig := internalConfig.Clone().(*model.Config)
	recipe := &model.RemoteSaveRecipe{
		ChangedFields:  model.NewChangedFields("blocks"),
		ObjectManifest: configState.Manifest(),
		InternalObject: internalConfig,
		ApiObject:      apiConfig,
	}

	// Save
	assert.NoError(t, NewMapper(context).MapBeforeRemoteSave(recipe))

	// Internal object is not modified
	assert.NotEmpty(t, internalConfig.Transformation.Blocks)
	assert.Nil(t, internalConfig.Content.GetNestedOrNil(`parameters.blocks`))

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
	assert.Empty(t, apiConfig.Transformation)
	apiBlocks := apiConfig.Content.GetNestedOrNil(`parameters.blocks`)
	assert.NotNil(t, blocks)
	assert.Equal(t, strings.TrimLeft(expectedBlocks, "\n"), json.MustEncodeString(apiBlocks, true))

	// Check changed fields
	assert.Equal(t, `configuration`, recipe.ChangedFields.String())
}
