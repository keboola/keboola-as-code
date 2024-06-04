package defaultbucket_test

import (
	"context"
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

const localLoadConfigContentSample = `
{
  "parameters": {},
  "storage": {
    "input": {
      "tables": [
        {
          "columns": [],
          "source": "{{:default-bucket:extractor/keboola.ex-aws-s3/test}}.accounts",
          "destination": "accounts",
          "where_column": "",
          "where_operator": "eq",
          "where_values": []
        },
        {
          "columns": [],
          "source": "{{:default-bucket:extractor/keboola.ex-aws-s3/test2}}.contacts",
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
	branchKey := model.BranchKey{ID: 123}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{
			BranchKey: branchKey,
			Paths:     model.Paths{AbsPath: model.NewAbsPath(``, `branch`)},
		},
		Local: &model.Branch{BranchKey: branchKey},
	}
	require.NoError(t, state.Set(branchState))

	// Config referenced by the default bucket
	configKey1 := model.ConfigKey{
		BranchID:    123,
		ComponentID: `keboola.ex-aws-s3`,
		ID:          `123`,
	}
	configState1 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey1,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath("branch", "extractor/keboola.ex-aws-s3/test"),
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
	require.NoError(t, state.Set(configState1))

	// Config with the input mapping
	configKey2 := model.ConfigKey{
		BranchID:    123,
		ComponentID: `keboola.snowflake-transformation`,
		ID:          `789`,
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
	require.NoError(t, state.Set(configState2))

	// Invoke
	changes := model.NewLocalChanges()
	changes.AddLoaded(configState2)
	recipe := model.NewLocalLoadRecipe(state.FileLoader(), configState2.ConfigManifest, configState2.Local)
	require.NoError(t, state.Mapper().AfterLocalOperation(context.Background(), changes))

	// Check warning of missing default bucket config
	expectedWarnings := `
WARN  Warning:
- Config "branch:123/component:keboola.snowflake-transformation/config:789" contains table "{{:default-bucket:extractor/keboola.ex-aws-s3/test2}}.contacts" in input mapping referencing to a non-existing configuration.
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logger.AllMessagesTxt())

	// Check default bucket replacement
	configContent := json.MustEncodeString(recipe.Object.(*model.Config).Content, false)
	assert.Equal(t, `{"parameters":{},"storage":{"input":{"tables":[{"columns":[],"source":"in.c-keboola-ex-aws-s3-123.accounts","destination":"accounts","where_column":"","where_operator":"eq","where_values":[]},{"columns":[],"source":"{{:default-bucket:extractor/keboola.ex-aws-s3/test2}}.contacts","destination":"contacts","where_column":"","where_operator":"eq","where_values":[]}],"files":[]},"output":{"tables":[],"files":[]}}}`, configContent)
}

func TestDefaultBucketMapper_MapBeforeLocalLoadRow(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Branch
	branchKey := model.BranchKey{ID: 123}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{
			BranchKey: branchKey,
			Paths:     model.Paths{AbsPath: model.NewAbsPath(``, `branch`)},
		},
		Local: &model.Branch{BranchKey: branchKey},
	}
	require.NoError(t, state.Set(branchState))

	// Config referenced by the default bucket
	configKey1 := model.ConfigKey{
		BranchID:    123,
		ComponentID: `keboola.ex-aws-s3`,
		ID:          `123`,
	}
	configState1 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey1,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath("branch", "extractor/keboola.ex-aws-s3/test"),
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
	require.NoError(t, state.Set(configState1))

	// Config with the input mapping
	configKey2 := model.ConfigKey{
		BranchID:    123,
		ComponentID: `keboola.snowflake-transformation`,
		ID:          `789`,
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
	require.NoError(t, state.Set(configState2))

	rowKey := model.ConfigRowKey{
		BranchID:    123,
		ConfigID:    configKey2.ID,
		ID:          `456`,
		ComponentID: configKey2.ComponentID,
	}
	content := orderedmap.New()
	json.MustDecodeString(localLoadConfigContentSample, content)
	rowState := &model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{
			ConfigRowKey: rowKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(
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
	require.NoError(t, state.Set(rowState))

	// Invoke
	changes := model.NewLocalChanges()
	changes.AddLoaded(rowState)
	recipe := model.NewLocalLoadRecipe(state.FileLoader(), rowState.ConfigRowManifest, rowState.Local)
	require.NoError(t, state.Mapper().AfterLocalOperation(context.Background(), changes))

	// Check warning of missing default bucket config
	expectedWarnings := `
WARN  Warning:
- Config row "branch:123/component:keboola.snowflake-transformation/config:789/row:456" contains table "{{:default-bucket:extractor/keboola.ex-aws-s3/test2}}.contacts" in input mapping referencing to a non-existing configuration.
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logger.AllMessagesTxt())

	// Check default bucket replacement
	configContent := json.MustEncodeString(recipe.Object.(*model.ConfigRow).Content, false)
	assert.Equal(t, `{"parameters":{},"storage":{"input":{"tables":[{"columns":[],"source":"in.c-keboola-ex-aws-s3-123.accounts","destination":"accounts","where_column":"","where_operator":"eq","where_values":[]},{"columns":[],"source":"{{:default-bucket:extractor/keboola.ex-aws-s3/test2}}.contacts","destination":"contacts","where_column":"","where_operator":"eq","where_values":[]}],"files":[]},"output":{"tables":[],"files":[]}}}`, configContent)
}
