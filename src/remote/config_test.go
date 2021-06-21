package remote

import (
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/json"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
	"testing"
)

func TestConfigApiCalls(t *testing.T) {
	SetStateOfTestProject(t, "empty.json")
	a, _ := TestStorageApiWithToken(t)

	// Get default branch
	branch, err := a.GetDefaultBranch()
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	// List components - no component
	components, err := a.ListComponents(branch.Id)
	assert.NotNil(t, components)
	assert.NoError(t, err)
	assert.Len(t, *components, 0)

	// Create config with rows
	row1 := &model.ConfigRow{
		Name:              "Row1",
		Description:       "Row1 description",
		ChangeDescription: "Row1 test",
		IsDisabled:        false,
		Config: utils.PairsToOrderedMap([]utils.Pair{
			{Key: "row1", Value: "value1"},
		}),
	}
	row2 := &model.ConfigRow{
		Name:              "Row2",
		Description:       "Row2 description",
		ChangeDescription: "Row2 test",
		IsDisabled:        true,
		Config: utils.PairsToOrderedMap([]utils.Pair{
			{Key: "row2", Value: "value2"},
		}),
	}
	config := &model.Config{
		BranchId:          branch.Id,
		ComponentId:       "ex-generic-v2",
		Name:              "Test",
		Description:       "Test description",
		ChangeDescription: "My test",
		Config: utils.PairsToOrderedMap([]utils.Pair{
			{
				Key: "foo",
				Value: utils.PairsToOrderedMap([]utils.Pair{
					{Key: "bar", Value: "baz"},
				}),
			},
		}),
		Rows: []*model.ConfigRow{row1, row2},
	}
	resConfig, err := a.CreateConfig(config)
	assert.NoError(t, err)
	assert.Same(t, config, resConfig)
	assert.NotEmpty(t, config.Id)
	assert.Equal(t, config.Id, row1.ConfigId)
	assert.Equal(t, "ex-generic-v2", row1.ComponentId)
	assert.Equal(t, branch.Id, row1.BranchId)
	assert.Equal(t, config.Id, row2.ConfigId)
	assert.Equal(t, "ex-generic-v2", row2.ComponentId)
	assert.Equal(t, branch.Id, row2.BranchId)

	// Update config
	config.Name = "Test modified"
	config.Description = "Test description modified"
	config.ChangeDescription = "updated"
	config.Config = utils.PairsToOrderedMap([]utils.Pair{
		{
			Key: "foo",
			Value: utils.PairsToOrderedMap([]utils.Pair{
				{Key: "bar", Value: "modified"},
			}),
		},
	})
	resConfig, err = a.UpdateConfig(config, []string{"name", "description", "changeDescription", "configuration"})
	assert.NoError(t, err)
	assert.Same(t, config, resConfig)

	// List components
	components, err = a.ListComponents(branch.Id)
	assert.NotNil(t, components)
	assert.NoError(t, err)
	componentsJson, err := json.EncodeString(components, true)
	assert.NoError(t, err)
	utils.AssertWildcards(t, expectedComponentsConfigTest(), componentsJson, "Unexpected components")

	// Delete configuration
	err = a.DeleteConfig(config.ComponentId, config.Id)
	assert.NoError(t, err)

	// List components - no component
	components, err = a.ListComponents(branch.Id)
	assert.NotNil(t, components)
	assert.NoError(t, err)
	assert.Len(t, *components, 0)
}

func expectedComponentsConfigTest() string {
	return `[
  {
    "branchId": %s,
    "id": "ex-generic-v2",
    "type": "extractor",
    "name": "Generic",
    "configurations": [
      {
        "branchId": %s,
        "componentId": "ex-generic-v2",
        "id": "%s",
        "name": "Test modified",
        "description": "Test description modified",
        "changeDescription": "updated",
        "configuration": {
          "foo": {
            "bar": "modified"
          }
        },
        "rows": [
          {
            "branchId": %s,
            "componentId": "ex-generic-v2",
            "configId": "%s",
            "id": "%s",
            "name": "Row1",
            "description": "Row1 description",
            "changeDescription": "Row1 test",
            "isDisabled": false,
            "configuration": {
              "row1": "value1"
            }
          },
          {
            "branchId": %s,
            "componentId": "ex-generic-v2",
            "configId": "%s",
            "id": "%s",
            "name": "Row2",
            "description": "Row2 description",
            "changeDescription": "Row2 test",
            "isDisabled": true,
            "configuration": {
              "row2": "value2"
            }
          }
        ]
      }
    ]
  }
]
`
}
