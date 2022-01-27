package storageapi_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/testproject"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestConfigApiCalls(t *testing.T) {
	t.Parallel()
	project := testproject.GetTestProject(t, env.Empty())
	project.SetState("empty.json")
	api := project.StorageApi()

	// Get default branch
	branch, err := api.GetDefaultBranch()
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	// List components - no component
	components, err := api.ListComponents(branch.Id)
	assert.NotNil(t, components)
	assert.NoError(t, err)
	assert.Len(t, components, 0)

	// Create config with rows
	row1 := &model.ConfigRow{
		Name:              "Row1",
		Description:       "Row1 description",
		ChangeDescription: "Row1 test",
		IsDisabled:        false,
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{Key: "row1", Value: "value1"},
		}),
	}
	row2 := &model.ConfigRow{
		Name:              "Row2",
		Description:       "Row2 description",
		ChangeDescription: "Row2 test",
		IsDisabled:        true,
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{Key: "row2", Value: "value2"},
		}),
	}
	config := &model.ConfigWithRows{
		Config: &model.Config{
			ConfigKey: model.ConfigKey{
				BranchId:    branch.Id,
				ComponentId: "ex-generic-v2",
			},
			Name:              "Test",
			Description:       "Test description",
			ChangeDescription: "My test",
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key: "foo",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "bar", Value: "baz"},
					}),
				},
			}),
		},
		Rows: []*model.ConfigRow{row1, row2},
	}
	resConfig, err := api.CreateConfig(config)
	assert.NoError(t, err)
	assert.Same(t, config, resConfig)
	assert.NotEmpty(t, config.Id)
	assert.Equal(t, config.Id, row1.ConfigId)
	assert.Equal(t, model.ComponentId("ex-generic-v2"), row1.ComponentId)
	assert.Equal(t, branch.Id, row1.BranchId)
	assert.Equal(t, config.Id, row2.ConfigId)
	assert.Equal(t, model.ComponentId("ex-generic-v2"), row2.ComponentId)
	assert.Equal(t, branch.Id, row2.BranchId)

	// Update config
	config.Name = "Test modified +++úěš!@#"
	config.Description = "Test description modified"
	config.ChangeDescription = "updated"
	config.Content = orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "foo",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "bar", Value: "modified"},
			}),
		},
	})
	resConfigUpdate, err := api.UpdateConfig(config.Config, model.NewChangedFields("name", "description", "changeDescription", "configuration"))
	assert.NoError(t, err)
	assert.Same(t, config.Config, resConfigUpdate)

	// List components
	components, err = api.ListComponents(branch.Id)
	assert.NotNil(t, components)
	assert.NoError(t, err)
	testhelper.AssertWildcards(t, expectedComponentsConfigTest(), json.MustEncodeString(components, true), "Unexpected components")

	// Update metadata
	config.Metadata = map[string]string{"KBC.KaC.meta1": fmt.Sprintf("%d", rand.Intn(100))}
	assert.NoError(t, api.UpdateConfigMetadataRequest(config.Config).Send().Err())

	// List metadata
	req := api.ListConfigMetadataRequest(branch.Id).Send()
	assert.NoError(t, req.Err())
	var configMetadata storageapi.ConfigMetadataResponseItem
	for _, item := range *req.Result.(*storageapi.ConfigMetadataResponse) {
		if item.ComponentId == config.ComponentId && item.ConfigId == config.Id {
			configMetadata = item
			break
		}
	}
	assert.NotNil(t, configMetadata)
	assert.Len(t, configMetadata.Metadata, 1)
	assert.Equal(t, "KBC.KaC.meta1", configMetadata.Metadata[0].Key)
	assert.Equal(t, config.Metadata["KBC.KaC.meta1"], configMetadata.Metadata[0].Value)

	// Delete configuration
	err = api.DeleteConfig(config.ConfigKey)
	assert.NoError(t, err)

	// List components - no component
	components, err = api.ListComponents(branch.Id)
	assert.NotNil(t, components)
	assert.NoError(t, err)
	assert.Len(t, components, 0)
}

func expectedComponentsConfigTest() string {
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
    "data": {
      "default_bucket": false,
      "default_bucket_stage": ""
    },
    "configurations": [
      {
        "branchId": %s,
        "componentId": "ex-generic-v2",
        "id": "%s",
        "name": "Test modified +++úěš!@#",
        "description": "Test description modified",
        "changeDescription": "updated",
        "configuration": {
          "foo": {
            "bar": "modified"
          }
        },
        "rows": [
          {
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
