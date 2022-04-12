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
          "source": "{{:default-bucket:extractor/keboola.ex-aws-s3/test}}.accounts",
          "destination": "accounts"
        }
      ]
    }
  }
}`

func TestDefaultBucketMapper_AfterLocalOperation_Load_Config(t *testing.T) {
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
	config := &model.Config{ConfigKey: configKey, Content: orderedmap.New()}
	json.MustDecodeString(localLoadConfigContentSample, config.Content)

	// Invoke
	assert.NoError(t, state.Mapper().AfterLocalOperation(model.NewChanges().AddLoaded(config)))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Placeholder is replaced with the default bucket
	configContent := json.MustEncodeString(config.Content, false)
	assert.Equal(t, `{"parameters":{},"storage":{"input":{"tables":[{"source":"in.c-keboola-ex-aws-s3-123.accounts","destination":"accounts"}]}}}`, configContent)
}

func TestDefaultBucketMapper_AfterLocalOperation_Load_Config_Missing(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Branch
	branchKey := model.BranchKey{BranchId: 123}
	state.MustAdd(&model.Branch{BranchKey: branchKey})

	// Config with the input mapping
	configKey := model.ConfigKey{BranchKey: branchKey, ComponentId: `keboola.snowflake-transformation`, ConfigId: `789`}
	config := &model.Config{ConfigKey: configKey, Content: orderedmap.New()}
	json.MustDecodeString(localLoadConfigContentSample, config.Content)

	// Invoke
	assert.NoError(t, state.Mapper().AfterLocalOperation(model.NewChanges().AddLoaded(config)))

	// Check warning of missing default bucket config
	expectedWarnings := `
WARN  Warning:
  - config "branch:123/component:keboola.snowflake-transformation/config:789" contains table "{{:default-bucket:extractor/keboola.ex-aws-s3/test}}.accounts" in input mapping referencing to a non-existing configuration
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logger.WarnAndErrorMessages())

	// No change in the content
	configContent := json.MustEncodeString(config.Content, false)
	assert.Equal(t, `{"parameters":{},"storage":{"input":{"tables":[{"source":"{{:default-bucket:extractor/keboola.ex-aws-s3/test}}.accounts","destination":"accounts"}]}}}`, configContent)
}

func TestDefaultBucketMapper_AfterLocalOperation_Load_Row(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Branch
	state.MustAdd(&model.Branch{BranchKey: model.BranchKey{BranchId: 123}})

	// Config referenced by the default bucket
	branchKey := model.BranchKey{BranchId: 123}
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
	row := &model.ConfigRow{ConfigRowKey: rowKey, Content: orderedmap.New()}
	json.MustDecodeString(localLoadConfigContentSample, row.Content)

	// Invoke
	assert.NoError(t, state.Mapper().AfterLocalOperation(model.NewChanges().AddLoaded(row)))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Placeholder is replaced with the default bucket
	rowContent := json.MustEncodeString(row.Content, false)
	assert.Equal(t, `{"parameters":{},"storage":{"input":{"tables":[{"source":"in.c-keboola-ex-aws-s3-123.accounts","destination":"accounts"}]}}}`, rowContent)
}

func TestDefaultBucketMapper_AfterLocalOperation_Load_Row_Missing(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Branch
	branchKey := model.BranchKey{BranchId: 123}
	state.MustAdd(&model.Branch{BranchKey: model.BranchKey{BranchId: 123}})

	// Parent config
	configKey := model.ConfigKey{BranchKey: branchKey, ComponentId: `keboola.snowflake-transformation`, ConfigId: `789`}
	config := &model.Config{ConfigKey: configKey}
	state.MustAdd(config)

	// Row with the input mapping
	rowKey := model.ConfigRowKey{ConfigKey: configKey, ConfigRowId: `456`}
	row := &model.ConfigRow{ConfigRowKey: rowKey, Content: orderedmap.New()}
	json.MustDecodeString(localLoadConfigContentSample, row.Content)

	// Invoke
	assert.NoError(t, state.Mapper().AfterLocalOperation(model.NewChanges().AddLoaded(row)))

	// Check warning of missing default bucket config
	expectedWarnings := `
WARN  Warning:
  - config row "branch:123/component:keboola.snowflake-transformation/config:789/row:456" contains table "{{:default-bucket:extractor/keboola.ex-aws-s3/test}}.accounts" in input mapping referencing to a non-existing configuration
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logger.WarnAndErrorMessages())

	// No change in the content
	rowContent := json.MustEncodeString(row.Content, false)
	assert.Equal(t, `{"parameters":{},"storage":{"input":{"tables":[{"source":"{{:default-bucket:extractor/keboola.ex-aws-s3/test}}.accounts","destination":"accounts"}]}}}`, rowContent)
}
