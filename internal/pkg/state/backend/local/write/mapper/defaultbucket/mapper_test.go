package defaultbucket_test

import (
	"context"
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

const localSaveConfigContentSample = `
{
  "parameters": {},
  "storage": {
    "input": {
      "tables": [
        {
          "columns": [],
          "source": "in.c-keboola-ex-aws-s3-123.accounts",
          "destination": "accounts",
          "where_column": "",
          "where_operator": "eq",
          "where_values": []
        },
        {
          "columns": [],
          "source": "in.c-keboola-ex-aws-s3-456.contacts",
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
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Config referenced by the default bucket
	configKey1 := model.ConfigKey{
		BranchId:    123,
		ComponentId: `keboola.ex-aws-s3`,
		Id:          `123`,
	}
	configState1 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey1,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath("branch", "extractor/keboola.ex-aws-s3/test"),
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
	assert.NoError(t, state.Set(configState1))

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
	assert.NoError(t, state.Set(configState2))

	// Invoke
	object := deepcopy.Copy(configState2.Local).(*model.Config)
	recipe := model.NewLocalSaveRecipe(configState2.ConfigManifest, object, model.NewChangedFields())
	assert.NoError(t, state.Mapper().MapBeforeLocalSave(context.Background(), recipe))

	// Check warning of missing default bucket config
	expectedWarnings := `
WARN  Warning: config "branch:123/component:keboola.ex-aws-s3/config:456" not found:
- referenced from config "branch:123/component:keboola.snowflake-transformation/config:789"
- input mapping "in.c-keboola-ex-aws-s3-456.contacts"
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logger.WarnAndErrorMessages())

	// Check default bucket replacement
	configContent := json.MustEncodeString(object.Content, false)
	assert.Equal(t, `{"parameters":{},"storage":{"input":{"tables":[{"columns":[],"source":"{{:default-bucket:extractor/keboola.ex-aws-s3/test}}.accounts","destination":"accounts","where_column":"","where_operator":"eq","where_values":[]},{"columns":[],"source":"in.c-keboola-ex-aws-s3-456.contacts","destination":"contacts","where_column":"","where_operator":"eq","where_values":[]}],"files":[]},"output":{"tables":[],"files":[]}}}`, configContent)
}

func TestDefaultBucketMapper_MapBeforeLocalSaveRow(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Config referenced by the default bucket
	configKey1 := model.ConfigKey{
		BranchId:    123,
		ComponentId: `keboola.ex-aws-s3`,
		Id:          `123`,
	}
	configState1 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey1,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath("branch", "extractor/keboola.ex-aws-s3/test"),
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
	json.MustDecodeString(localSaveConfigContentSample, content)
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
	assert.NoError(t, state.Set(rowState))

	// Invoke
	object := deepcopy.Copy(rowState.Local).(*model.ConfigRow)
	recipe := model.NewLocalSaveRecipe(rowState.ConfigRowManifest, object, model.NewChangedFields())
	object.Content = content
	assert.NoError(t, state.Mapper().MapBeforeLocalSave(context.Background(), recipe))

	// Check warning of missing default bucket config
	expectedWarnings := `
WARN  Warning: config "branch:123/component:keboola.ex-aws-s3/config:456" not found:
- referenced from config row "branch:123/component:keboola.snowflake-transformation/config:789/row:456"
- input mapping "in.c-keboola-ex-aws-s3-456.contacts"
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logger.WarnAndErrorMessages())

	// Check default bucket replacement
	configContent := json.MustEncodeString(object.Content, false)
	assert.Equal(t, `{"parameters":{},"storage":{"input":{"tables":[{"columns":[],"source":"{{:default-bucket:extractor/keboola.ex-aws-s3/test}}.accounts","destination":"accounts","where_column":"","where_operator":"eq","where_values":[]},{"columns":[],"source":"in.c-keboola-ex-aws-s3-456.contacts","destination":"contacts","where_column":"","where_operator":"eq","where_values":[]}],"files":[]},"output":{"tables":[],"files":[]}}}`, configContent)
}
