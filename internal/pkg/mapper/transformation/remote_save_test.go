package transformation_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/transformation"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestRemoteSaveTransformation(t *testing.T) {
	t.Parallel()
	context, internalConfig, configRecord := createTestFixtures(t, "keboola.snowflake-transformation")
	blocks := model.Blocks{
		{
			Name: "001",
			Codes: model.Codes{
				{
					Name: "001-001",
					Scripts: []string{
						"SELECT 1",
					},
				},
				{
					Name: "001-002",
					Scripts: []string{
						"SELECT 1;",
						"SELECT 2;",
					},
				},
			},
		},
		{
			Name: "002",
			Codes: model.Codes{
				{
					Name: "002-001",
					Scripts: []string{
						"SELECT 3",
					},
				},
			},
		},
		{
			Name:  "003",
			Codes: model.Codes{},
		},
	}
	internalConfig.Blocks = blocks
	apiConfig := internalConfig.Clone().(*model.Config)
	recipe := &model.RemoteSaveRecipe{
		ChangedFields:  model.NewChangedFields("blocks"),
		Manifest:       configRecord,
		InternalObject: internalConfig,
		ApiObject:      apiConfig,
	}

	// Save
	assert.NoError(t, NewMapper(context).MapBeforeRemoteSave(recipe))

	// Internal object is not modified
	assert.NotEmpty(t, internalConfig.Blocks)
	assert.Nil(t, utils.GetFromMap(internalConfig.Content, []string{`parameters`, `blocks`}))

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
	assert.Empty(t, apiConfig.Blocks)
	apiBlocks := utils.GetFromMap(apiConfig.Content, []string{`parameters`, `blocks`})
	assert.NotNil(t, blocks)
	assert.Equal(t, strings.TrimLeft(expectedBlocks, "\n"), json.MustEncodeString(apiBlocks, true))

	// Check changed fields
	assert.Equal(t, `configuration`, recipe.ChangedFields.String())
}
