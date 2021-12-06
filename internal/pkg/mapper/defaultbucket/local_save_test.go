package defaultbucket_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestDefaultBucketMapper_MapBeforeLocalSave(t *testing.T) {
	t.Parallel()
	mapperInst, context, logs := createMapper(t)

	// Config referenced by the default bucket
	configKey1 := model.ConfigKey{
		BranchId:    123,
		ComponentId: `keboola.ex-db-mysql`,
		Id:          `123`,
	}
	configState1 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey1,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject("branch", "extractor/keboola.ex-db-mysql/test"),
			},
		},
		Local: &model.Config{
			ConfigKey: configKey1,
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
	assert.NoError(t, context.State.Set(configState1))

	// Config with the input mapping
	configKey2 := model.ConfigKey{
		BranchId:    123,
		ComponentId: `keboola.snowflake-transformation`,
		Id:          `789`,
	}
	contentStr := `
{
  "parameters": {},
  "storage": {
    "input": {
      "tables": [
        {
          "columns": [],
          "source": "in.c-keboola-ex-db-mysql-123.accounts",
          "destination": "accounts",
          "where_column": "",
          "where_operator": "eq",
          "where_values": []
        },
        {
          "columns": [],
          "source": "in.c-keboola-ex-db-mysql-456.contacts",
          "destination": "contacts",
          "where_column": "",
          "where_operator": "eq",
          "where_values": []
        }
      ],
      "files": []
    },
    "output": {
      "tables": [],
      "files": []
    }
  }
}`
	content := utils.NewOrderedMap()
	json.MustDecodeString(contentStr, content)
	configState2 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey2,
		},
		Local: &model.Config{
			ConfigKey: configKey2,
			Content:   content,
		},
	}
	assert.NoError(t, context.State.Set(configState2))

	// Invoke
	recipe := createLocalSaveRecipe(configState2.Local, configState2.ConfigManifest)
	assert.NoError(t, mapperInst.MapBeforeLocalSave(recipe))

	// Check warning of missing default bucket config
	expectedWarnings := `
WARN  Warning: - config "branch:123/component:keboola.ex-db-mysql/config:456" not found
  - referenced  from configuration config "branch:123/component:keboola.snowflake-transformation/config:789"
  - input mapping "in.c-keboola-ex-db-mysql-456.contacts"
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logs.String())

	// Check default bucket replacement
	configContent := json.MustEncodeString(recipe.Object.(*model.Config).Content, false)
	assert.Equal(t, `{"parameters":{},"storage":{"input":{"tables":[{"columns":[],"source":"{{:default-bucket:extractor/keboola.ex-db-mysql/test}}.accounts","destination":"accounts","where_column":"","where_operator":"eq","where_values":[]},{"columns":[],"source":"in.c-keboola-ex-db-mysql-456.contacts","destination":"contacts","where_column":"","where_operator":"eq","where_values":[]}],"files":[]},"output":{"tables":[],"files":[]}}}`, configContent)
}
