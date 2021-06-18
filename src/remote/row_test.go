package remote

import (
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/json"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
	"testing"
)

func TestConfigRowApiCalls(t *testing.T) {
	SetStateOfTestProject(t, "empty.json")
	a, _ := TestStorageApiWithToken(t)

	// Get default branch
	branch, err := a.GetDefaultBranch()
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	// Create config
	config := &model.Config{
		BranchId:          branch.Id,
		ComponentId:       "ex-generic-v2",
		Name:              "Test",
		Description:       "Test description",
		ChangeDescription: "My test",
		Config: map[string]interface{}{
			"foo": map[string]interface{}{
				"bar": "baz",
			},
		},
	}
	resConfig, err := a.CreateConfig(config)
	assert.NoError(t, err)
	assert.Same(t, config, resConfig)

	// Create row1
	row1 := &model.ConfigRow{
		BranchId:          branch.Id,
		ComponentId:       "ex-generic-v2",
		ConfigId:          config.Id,
		Name:              "Row1",
		Description:       "Row1 description",
		ChangeDescription: "Row1 test",
		IsDisabled:        true,
		Config: map[string]interface{}{
			"row1": "value1",
		},
	}
	resRow1, err := a.CreateConfigRow(row1)
	assert.NoError(t, err)
	assert.Same(t, row1, resRow1)

	// Create row2
	row2 := &model.ConfigRow{
		BranchId:          branch.Id,
		ComponentId:       "ex-generic-v2",
		ConfigId:          config.Id,
		Name:              "Row2",
		Description:       "Row2 description",
		ChangeDescription: "Row2 test",
		IsDisabled:        false,
		Config: map[string]interface{}{
			"row2": "value2",
		},
	}
	resRow2, err := a.CreateConfigRow(row2)
	assert.NoError(t, err)
	assert.Same(t, row2, resRow2)

	// Update row 1
	row1.Name = "Row1 modified"
	row1.Description = "Row1 description modified"
	row1.ChangeDescription = "updated"
	row1.Config = map[string]interface{}{
		"row1": "xyz",
	}
	resRow1, err = a.UpdateConfigRow(row1, []string{"name", "description", "changeDescription", "configuration"})
	assert.NoError(t, err)
	assert.Same(t, row1, resRow1)

	// Delete row 2
	err = a.DeleteConfigRow(row2.ComponentId, row2.ConfigId, row2.Id)
	assert.NoError(t, err)

	// List components
	components, err := a.ListComponents(branch.Id)
	assert.NotNil(t, components)
	assert.NoError(t, err)
	componentsJson, err := json.EncodeString(components, true)
	assert.NoError(t, err)
	utils.AssertWildcards(t, expectedComponentsConfigRowTest(), componentsJson, "Unexpected components")
}

func expectedComponentsConfigRowTest() string {
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
            "branchId": %s,
            "componentId": "ex-generic-v2",
            "configId": "%s",
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
