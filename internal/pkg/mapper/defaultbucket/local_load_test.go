package defaultbucket_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

const localLoadConfigContentSample = `
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

func TestDefaultBucketMapper_MapBeforeLocalLoadConfig(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Branch
	branchKey := model.BranchKey{Id: 123}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{
			BranchKey: branchKey,
			Paths:     model.Paths{PathInProject: model.NewPathInProject(``, `branch`)},
		},
		Local: &model.Branch{BranchKey: branchKey},
	}
	assert.NoError(t, state.Set(branchState))

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
	assert.NoError(t, state.Set(configState1))

	// Config with the input mapping
	configKey2 := model.ConfigKey{
		BranchId:    123,
		ComponentId: `keboola.snowflake-transformation`,
		Id:          `789`,
	}

	content := orderedmap.New()
	json.MustDecodeString(localLoadConfigContentSample, content)
	configState2 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey2,
		},
		Local: &model.Config{
			ConfigKey: configKey2,
			Content:   content,
		},
	}
	assert.NoError(t, state.Set(configState2))

	// Invoke
	changes := model.NewLocalChanges()
	changes.AddLoaded(configState2)
	recipe := model.NewLocalLoadRecipe(configState2.ConfigManifest, configState2.Local)
	assert.NoError(t, state.Mapper().OnLocalChange(changes))

	// Check warning of missing default bucket config
	expectedWarnings := `
WARN  Warning:
  - config "branch:123/component:keboola.snowflake-transformation/config:789" contains table "{{:default-bucket:extractor/keboola.ex-db-mysql/test2}}.contacts" in input mapping referencing to a non-existing configuration
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logger.WarnAndErrorMessages())

	// Check default bucket replacement
	configContent := json.MustEncodeString(recipe.Object.(*model.Config).Content, false)
	assert.Equal(t, `{"parameters":{},"storage":{"input":{"tables":[{"columns":[],"source":"in.c-keboola-ex-db-mysql-123.accounts","destination":"accounts","where_column":"","where_operator":"eq","where_values":[]},{"columns":[],"source":"{{:default-bucket:extractor/keboola.ex-db-mysql/test2}}.contacts","destination":"contacts","where_column":"","where_operator":"eq","where_values":[]}],"files":[]},"output":{"tables":[],"files":[]}}}`, configContent)
}

func TestDefaultBucketMapper_MapBeforeLocalLoadRow(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Branch
	branchKey := model.BranchKey{Id: 123}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{
			BranchKey: branchKey,
			Paths:     model.Paths{PathInProject: model.NewPathInProject(``, `branch`)},
		},
		Local: &model.Branch{BranchKey: branchKey},
	}
	assert.NoError(t, state.Set(branchState))

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
	assert.NoError(t, state.Set(configState1))

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
	assert.NoError(t, state.Set(configState2))

	rowKey := model.ConfigRowKey{
		BranchId:    123,
		ConfigId:    configKey2.Id,
		Id:          `456`,
		ComponentId: configKey2.ComponentId,
	}
	content := orderedmap.New()
	json.MustDecodeString(localLoadConfigContentSample, content)
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
	assert.NoError(t, state.Set(rowState))

	// Invoke
	changes := model.NewLocalChanges()
	changes.AddLoaded(rowState)
	recipe := model.NewLocalLoadRecipe(rowState.ConfigRowManifest, rowState.Local)
	assert.NoError(t, state.Mapper().OnLocalChange(changes))

	// Check warning of missing default bucket config
	expectedWarnings := `
WARN  Warning:
  - config row "branch:123/component:keboola.snowflake-transformation/config:789/row:456" contains table "{{:default-bucket:extractor/keboola.ex-db-mysql/test2}}.contacts" in input mapping referencing to a non-existing configuration
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logger.WarnAndErrorMessages())

	// Check default bucket replacement
	configContent := json.MustEncodeString(recipe.Object.(*model.ConfigRow).Content, false)
	assert.Equal(t, `{"parameters":{},"storage":{"input":{"tables":[{"columns":[],"source":"in.c-keboola-ex-db-mysql-123.accounts","destination":"accounts","where_column":"","where_operator":"eq","where_values":[]},{"columns":[],"source":"{{:default-bucket:extractor/keboola.ex-db-mysql/test2}}.contacts","destination":"contacts","where_column":"","where_operator":"eq","where_values":[]}],"files":[]},"output":{"tables":[],"files":[]}}}`, configContent)
}
