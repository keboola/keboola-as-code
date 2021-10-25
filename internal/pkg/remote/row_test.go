package remote_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/testproject"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestConfigRowApiCalls(t *testing.T) {
	t.Parallel()
	project := testproject.GetTestProject(t, env.Empty())
	project.SetState("empty.json")
	api := project.Api()

	// Get default branch
	branch, err := api.GetDefaultBranch()
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	// Create config
	config := &model.ConfigWithRows{
		Config: &model.Config{
			ConfigKey: model.ConfigKey{
				BranchId:    branch.Id,
				ComponentId: "ex-generic-v2",
			},
			Name:              "Test",
			Description:       "Test description",
			ChangeDescription: "My test",
			Content: utils.PairsToOrderedMap([]utils.Pair{
				{
					Key: "foo",
					Value: utils.PairsToOrderedMap([]utils.Pair{
						{Key: "bar", Value: "baz"},
					}),
				},
			}),
		},
	}
	resConfig, err := api.CreateConfig(config)
	assert.NoError(t, err)
	assert.Same(t, config, resConfig)

	// Create row1
	row1 := &model.ConfigRow{
		ConfigRowKey: model.ConfigRowKey{
			BranchId:    branch.Id,
			ComponentId: "ex-generic-v2",
			ConfigId:    config.Id,
		},
		Name:              "Row1",
		Description:       "Row1 description",
		ChangeDescription: "Row1 test",
		IsDisabled:        true,
		Content: utils.PairsToOrderedMap([]utils.Pair{
			{Key: "row1", Value: "value1"},
		}),
	}
	resRow1, err := api.CreateConfigRow(row1)
	assert.NoError(t, err)
	assert.Same(t, row1, resRow1)

	// Create row2
	row2 := &model.ConfigRow{
		ConfigRowKey: model.ConfigRowKey{
			BranchId:    branch.Id,
			ComponentId: "ex-generic-v2",
			ConfigId:    config.Id,
		},
		Name:              "Row2",
		Description:       "Row2 description",
		ChangeDescription: "Row2 test",
		IsDisabled:        false,
		Content: utils.PairsToOrderedMap([]utils.Pair{
			{Key: "row2", Value: "value2"},
		}),
	}
	resRow2, err := api.CreateConfigRow(row2)
	assert.NoError(t, err)
	assert.Same(t, row2, resRow2)

	// Update row 1
	row1.Name = "Row1 modified"
	row1.Description = "Row1 description modified"
	row1.ChangeDescription = "updated"
	row1.Content = utils.PairsToOrderedMap([]utils.Pair{
		{Key: "row1", Value: "xyz"},
	})
	resRow1, err = api.UpdateConfigRow(row1, []string{"name", "description", "changeDescription", "configuration"})
	assert.NoError(t, err)
	assert.Same(t, row1, resRow1)

	// Delete row 2
	err = api.DeleteConfigRow(row2.ConfigRowKey)
	assert.NoError(t, err)

	// List components
	components, err := api.ListComponents(branch.Id)
	assert.NotNil(t, components)
	assert.NoError(t, err)
	testhelper.AssertWildcards(t, expectedComponentsConfigRowTest(), json.MustEncodeString(components, true), "Unexpected components")
}

func expectedComponentsConfigRowTest() string {
	return `[
  {
    "branchId": %s,
    "id": "ex-generic-v2",
    "type": "extractor",
    "name": "Generic",
    "flags": [
      "genericUI",
      "encrypt"
    ],
    "configurationSchema": {},
    "configurationRowSchema": {},
    "emptyConfiguration": {},
    "emptyConfigurationRow": {},
    "configurations": [
      {
        "branchId": %s,
        "componentId": "ex-generic-v2",
        "id": "%s",
        "name": "Test",
        "description": "Test description",
        "changeDescription": "Row %s deleted",
        "configuration": {
          "foo": {
            "bar": "baz"
          }
        },
        "rows": [
          {
            "id": "%s",
            "name": "Row1 modified",
            "description": "Row1 description modified",
            "changeDescription": "updated",
            "isDisabled": true,
            "configuration": {
              "row1": "xyz"
            }
          }
        ]
      }
    ]
  }
]
`
}
