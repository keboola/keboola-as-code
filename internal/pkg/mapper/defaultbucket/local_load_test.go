package defaultbucket_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestDefaultBucketMapper_MapBeforeLocalLoad(t *testing.T) {
	t.Parallel()
	mapperInst, context, logs := createMapper(t)

	// Branch
	branchKey := model.BranchKey{Id: 123}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{
			BranchKey: branchKey,
			Paths:     model.Paths{PathInProject: model.NewPathInProject(``, `branch`)},
		},
		Local: &model.Branch{BranchKey: branchKey},
	}
	assert.NoError(t, context.State.Set(branchState))
	context.Naming.Attach(branchKey, branchState.PathInProject)

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
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key: "foo",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "bar", Value: "baz"},
					}),
				},
			}),
		},
	}
	assert.NoError(t, context.State.Set(configState1))
	context.Naming.Attach(configState1.Key(), configState1.PathInProject)

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
          "source": "{{:default-bucket:extractor/keboola.ex-db-mysql/test}}.accounts",
          "destination": "accounts",
          "where_column": "",
          "where_operator": "eq",
          "where_values": []
        },
        {
          "columns": [],
          "source": "{{:default-bucket:extractor/keboola.ex-db-mysql/test2}}.contacts",
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
	content := orderedmap.New()
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
	recipe := createLocalLoadRecipe(configState2.Local, configState2.ConfigManifest)
	assert.NoError(t, mapperInst.MapAfterLocalLoad(recipe))

	// Check warning of missing default bucket config
	expectedWarnings := `
WARN  Warning: configuration config "branch:123/component:keboola.snowflake-transformation/config:789" contains table "{{:default-bucket:extractor/keboola.ex-db-mysql/test2}}.contacts" in input mapping referencing to a non-existing configuration
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logs.String())

	// Check default bucket replacement
	configContent := json.MustEncodeString(recipe.Object.(*model.Config).Content, false)
	assert.Equal(t, `{"parameters":{},"storage":{"input":{"tables":[{"columns":[],"source":"in.c-keboola-ex-db-mysql-123.accounts","destination":"accounts","where_column":"","where_operator":"eq","where_values":[]},{"columns":[],"source":"{{:default-bucket:extractor/keboola.ex-db-mysql/test2}}.contacts","destination":"contacts","where_column":"","where_operator":"eq","where_values":[]}],"files":[]},"output":{"tables":[],"files":[]}}}`, configContent)
}
