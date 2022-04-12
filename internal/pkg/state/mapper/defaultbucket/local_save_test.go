package defaultbucket_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/deepcopy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

const localSaveConfigContentSample = `
{
  "parameters": {},
  "storage": {
    "input": {
      "tables": [
        {
          "source": "in.c-keboola-ex-aws-s3-123.accounts",
          "destination": "accounts"
        }
      ]
    }
  }
}`

func TestDefaultBucketMapper_MapBeforeLocalSave_Config(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Branch
	branchKey := model.BranchKey{BranchId: 123}
	state.MustAdd(&model.Branch{BranchKey: branchKey})

	// Config referenced by the default bucket
	sourceConfigKey := model.ConfigKey{BranchKey: branchKey, ComponentId: `keboola.ex-aws-s3`, ConfigId: `123`}
	state.MustAdd(&model.Config{ConfigKey: sourceConfigKey})
	state.NamingRegistry().MustAttach(
		sourceConfigKey,
		model.NewAbsPath("branch", "extractor/keboola.ex-aws-s3/test"),
	)

	// Config with the input mapping
	configKey := model.ConfigKey{BranchKey: branchKey, ComponentId: `keboola.snowflake-transformation`, ConfigId: `789`}
	configPath := model.NewAbsPath("branch", "transformation/keboola.snowflake-transformation/test")
	state.NamingRegistry().MustAttach(configKey, configPath)
	config := &model.Config{ConfigKey: configKey, Content: orderedmap.New()}
	json.MustDecodeString(localSaveConfigContentSample, config.Content)
	state.MustAdd(config)

	// Invoke
	object := deepcopy.Copy(config).(*model.Config)
	_, err := state.Mapper().MapBeforeLocalSave(context.Background(), object, nil)
	assert.NoError(t, err)
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Default bucket is replaced with the placeholder
	configContent := json.MustEncodeString(object.Content, false)
	assert.Equal(t, `{"parameters":{},"storage":{"input":{"tables":[{"source":"{{:default-bucket:extractor/keboola.ex-aws-s3/test}}.accounts","destination":"accounts"}]}}}`, configContent)
}

func TestDefaultBucketMapper_MapBeforeLocalSave_Config_Missing(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Branch
	branchKey := model.BranchKey{BranchId: 123}
	state.MustAdd(&model.Branch{BranchKey: branchKey})

	// Config with the input mapping
	configKey := model.ConfigKey{BranchKey: branchKey, ComponentId: `keboola.snowflake-transformation`, ConfigId: `789`}
	configPath := model.NewAbsPath("branch", "transformation/keboola.snowflake-transformation/test")
	state.NamingRegistry().MustAttach(configKey, configPath)
	config := &model.Config{ConfigKey: configKey, Content: orderedmap.New()}
	json.MustDecodeString(localSaveConfigContentSample, config.Content)
	state.MustAdd(config)

	// Invoke
	object := deepcopy.Copy(config).(*model.Config)
	_, err := state.Mapper().MapBeforeLocalSave(context.Background(), object, nil)
	assert.NoError(t, err)

	// Check warning of missing default bucket config
	expectedWarnings := `
WARN  Warning: - config "branch:123/component:keboola.ex-aws-s3/config:123" not found
  - referenced from config "branch:123/component:keboola.snowflake-transformation/config:789"
  - input mapping "in.c-keboola-ex-aws-s3-123.accounts"
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logger.WarnAndErrorMessages())

	// No change in the content
	configContent := json.MustEncodeString(object.Content, false)
	assert.Equal(t, `{"parameters":{},"storage":{"input":{"tables":[{"source":"in.c-keboola-ex-aws-s3-123.accounts","destination":"accounts"}]}}}`, configContent)
}

func TestDefaultBucketMapper_MapBeforeLocalSave_Row(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Branch
	branchKey := model.BranchKey{BranchId: 123}
	state.MustAdd(&model.Branch{BranchKey: branchKey})

	// Config referenced by the default bucket
	sourceConfigKey := model.ConfigKey{BranchKey: branchKey, ComponentId: `keboola.ex-aws-s3`, ConfigId: `123`}
	state.MustAdd(&model.Config{ConfigKey: sourceConfigKey})
	state.NamingRegistry().MustAttach(
		sourceConfigKey,
		model.NewAbsPath("branch", "extractor/keboola.ex-aws-s3/test"),
	)

	// Parent config
	configKey := model.ConfigKey{BranchKey: branchKey, ComponentId: `keboola.snowflake-transformation`, ConfigId: `789`}
	config := &model.Config{ConfigKey: configKey}
	state.MustAdd(config)

	// Row with the input mapping
	rowKey := model.ConfigRowKey{ConfigKey: configKey, ConfigRowId: `456`}
	rowPath := model.NewAbsPath("branch/transformation/keboola.snowflake-transformation/test", "rows/row")
	state.NamingRegistry().MustAttach(rowKey, rowPath)
	row := &model.ConfigRow{ConfigRowKey: rowKey, Content: orderedmap.New()}
	json.MustDecodeString(localSaveConfigContentSample, row.Content)
	state.MustAdd(row)

	// Invoke
	object := deepcopy.Copy(row).(*model.ConfigRow)
	_, err := state.Mapper().MapBeforeLocalSave(context.Background(), object, nil)
	assert.NoError(t, err)
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Default bucket is replaced with the placeholder
	configContent := json.MustEncodeString(object.Content, false)
	assert.Equal(t, `{"parameters":{},"storage":{"input":{"tables":[{"source":"{{:default-bucket:extractor/keboola.ex-aws-s3/test}}.accounts","destination":"accounts"}]}}}`, configContent)
}

func TestDefaultBucketMapper_MapBeforeLocalSave_Row_Missing(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Branch
	branchKey := model.BranchKey{BranchId: 123}
	state.MustAdd(&model.Branch{BranchKey: branchKey})

	// Parent config
	configKey := model.ConfigKey{BranchKey: branchKey, ComponentId: `keboola.snowflake-transformation`, ConfigId: `789`}
	config := &model.Config{ConfigKey: configKey}
	state.MustAdd(config)

	// Row with the input mapping
	rowKey := model.ConfigRowKey{ConfigKey: configKey, ConfigRowId: `456`}
	rowPath := model.NewAbsPath("branch/transformation/keboola.snowflake-transformation/test", "rows/row")
	state.NamingRegistry().MustAttach(rowKey, rowPath)
	row := &model.ConfigRow{ConfigRowKey: rowKey, Content: orderedmap.New()}
	json.MustDecodeString(localSaveConfigContentSample, row.Content)
	state.MustAdd(row)

	// Invoke
	object := deepcopy.Copy(row).(*model.ConfigRow)
	_, err := state.Mapper().MapBeforeLocalSave(context.Background(), object, nil)
	assert.NoError(t, err)

	// Check warning of missing default bucket config
	expectedWarnings := `
WARN  Warning: - config "branch:123/component:keboola.ex-aws-s3/config:123" not found
  - referenced from config row "branch:123/component:keboola.snowflake-transformation/config:789/row:456"
  - input mapping "in.c-keboola-ex-aws-s3-123.accounts"
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logger.WarnAndErrorMessages())

	// No change in the content
	configContent := json.MustEncodeString(object.Content, false)
	assert.Equal(t, `{"parameters":{},"storage":{"input":{"tables":[{"source":"in.c-keboola-ex-aws-s3-123.accounts","destination":"accounts"}]}}}`, configContent)
}
