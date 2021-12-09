package defaultbucket_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

const localSaveConfigContentSample = `
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

func TestDefaultBucketMapper_MapBeforeLocalSaveConfig(t *testing.T) {
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
		Remote: &model.Config{
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

	// Config with the input mapping
	configKey2 := model.ConfigKey{
		BranchId:    123,
		ComponentId: `keboola.snowflake-transformation`,
		Id:          `789`,
	}

	content := orderedmap.New()
	json.MustDecodeString(localSaveConfigContentSample, content)
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
	recipe := fixtures.NewLocalSaveRecipe(configState2.ConfigManifest, configState2.Local)
	configFile, err := recipe.Files.ObjectConfigFile()
	assert.NoError(t, err)
	configFile.Content = content
	assert.NoError(t, mapperInst.MapBeforeLocalSave(recipe))

	// Check warning of missing default bucket config
	expectedWarnings := `
WARN  Warning: - config "branch:123/component:keboola.ex-db-mysql/config:456" not found
  - referenced from config "branch:123/component:keboola.snowflake-transformation/config:789"
  - input mapping "in.c-keboola-ex-db-mysql-456.contacts"
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logs.String())

	// Check default bucket replacement
	configFile, err = recipe.Files.ObjectConfigFile()
	assert.NoError(t, err)
	configContent := json.MustEncodeString(configFile.Content, false)
	assert.Equal(t, `{"parameters":{},"storage":{"input":{"tables":[{"columns":[],"source":"{{:default-bucket:extractor/keboola.ex-db-mysql/test}}.accounts","destination":"accounts","where_column":"","where_operator":"eq","where_values":[]},{"columns":[],"source":"in.c-keboola-ex-db-mysql-456.contacts","destination":"contacts","where_column":"","where_operator":"eq","where_values":[]}],"files":[]},"output":{"tables":[],"files":[]}}}`, configContent)
}

func TestDefaultBucketMapper_MapBeforeLocalSaveRow(t *testing.T) {
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
		Remote: &model.Config{
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

	// Config with the input mapping
	configKey2 := model.ConfigKey{
		BranchId:    123,
		ComponentId: `keboola.snowflake-transformation`,
		Id:          `789`,
	}

	configState2 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey2,
		},
		Local: &model.Config{
			ConfigKey: configKey2,
			Content:   orderedmap.New(),
		},
	}
	assert.NoError(t, context.State.Set(configState2))

	rowKey := model.ConfigRowKey{
		BranchId:    123,
		ConfigId:    configKey2.Id,
		Id:          `456`,
		ComponentId: configKey2.ComponentId,
	}
	content := orderedmap.New()
	json.MustDecodeString(localSaveConfigContentSample, content)
	rowState := &model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{
			ConfigRowKey: rowKey,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject(
					"branch/config",
					"row",
				),
			},
		},
		Local: &model.ConfigRow{
			ConfigRowKey: rowKey,
			Content:      content,
		},
	}
	assert.NoError(t, context.State.Set(rowState))

	// Invoke
	recipe := fixtures.NewLocalSaveRecipe(rowState.ConfigRowManifest, rowState.Local)
	configFile, err := recipe.Files.ObjectConfigFile()
	assert.NoError(t, err)
	configFile.Content = content
	assert.NoError(t, mapperInst.MapBeforeLocalSave(recipe))

	// Check warning of missing default bucket config
	expectedWarnings := `
WARN  Warning: - config "branch:123/component:keboola.ex-db-mysql/config:456" not found
  - referenced from config row "branch:123/component:keboola.snowflake-transformation/config:789/row:456"
  - input mapping "in.c-keboola-ex-db-mysql-456.contacts"
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logs.String())

	// Check default bucket replacement
	configFile, err = recipe.Files.ObjectConfigFile()
	assert.NoError(t, err)
	configContent := json.MustEncodeString(configFile.Content, false)
	assert.Equal(t, `{"parameters":{},"storage":{"input":{"tables":[{"columns":[],"source":"{{:default-bucket:extractor/keboola.ex-db-mysql/test}}.accounts","destination":"accounts","where_column":"","where_operator":"eq","where_values":[]},{"columns":[],"source":"in.c-keboola-ex-db-mysql-456.contacts","destination":"contacts","where_column":"","where_operator":"eq","where_values":[]}],"files":[]},"output":{"tables":[],"files":[]}}}`, configContent)
}
