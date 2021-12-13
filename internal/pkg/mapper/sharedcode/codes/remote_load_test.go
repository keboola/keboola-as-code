package codes_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/codes"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeRemoteLoad(t *testing.T) {
	t.Parallel()
	context, logs, configState, rowState := createRemoteSharedCode(t)
	rowState.Remote.Content.Set(model.SharedCodeContentKey, []interface{}{
		"SELECT 1;",
		"SELECT 2;",
	})

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(configState)
	changes.AddLoaded(rowState)
	assert.NoError(t, NewMapper(context).OnRemoteChange(changes))
	assert.Empty(t, logs.String())

	// Check config
	assert.Equal(t, &model.SharedCodeConfig{
		Target: `keboola.snowflake-transformation`,
	}, configState.Remote.SharedCode)

	// Check row
	assert.Equal(t, &model.SharedCodeRow{
		Target: `keboola.snowflake-transformation`,
		Scripts: model.Scripts{
			model.StaticScript{
				Value: "SELECT 1;",
			},
			model.StaticScript{
				Value: "SELECT 2;",
			},
		},
	}, rowState.Remote.SharedCode)
}

func TestSharedCodeRemoteLoad_Legacy(t *testing.T) {
	t.Parallel()
	context, logs, configState, rowState := createRemoteSharedCode(t)
	rowState.Remote.Content.Set(model.SharedCodeContentKey, "SELECT 1; \n  SELECT 2; \n ")

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(configState)
	changes.AddLoaded(rowState)
	assert.NoError(t, NewMapper(context).OnRemoteChange(changes))
	assert.Empty(t, logs.String())

	// Check config
	assert.Equal(t, &model.SharedCodeConfig{
		Target: `keboola.snowflake-transformation`,
	}, configState.Remote.SharedCode)

	// Check row
	assert.Equal(t, &model.SharedCodeRow{
		Target: `keboola.snowflake-transformation`,
		Scripts: model.Scripts{
			model.StaticScript{
				Value: "SELECT 1;",
			},
			model.StaticScript{
				Value: "SELECT 2;",
			},
		},
	}, rowState.Remote.SharedCode)
}

func TestSharedCodeRemoteLoad_UnexpectedTypeInConfig(t *testing.T) {
	t.Parallel()
	context, logs, configState, rowState := createRemoteSharedCode(t)
	configState.Remote.Content.Set(model.ShareCodeTargetComponentKey, 123)

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(configState)
	changes.AddLoaded(rowState)
	assert.NoError(t, NewMapper(context).OnRemoteChange(changes))

	// Check logs
	expectedLogs := `
WARN  Warning:
  - invalid config "branch:789/component:keboola.shared-code/config:123":
    - key "componentId" should be string, found "int"
`
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logs.String())

	// Check config and row
	assert.Empty(t, configState.Remote.SharedCode)
	assert.Empty(t, rowState.Remote.SharedCode)
}

func TestSharedCodeRemoteLoad_UnexpectedTypeInRow(t *testing.T) {
	t.Parallel()
	context, logs, configState, rowState := createRemoteSharedCode(t)
	rowState.Remote.Content.Set(model.SharedCodeContentKey, 123)

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(configState)
	changes.AddLoaded(rowState)
	assert.NoError(t, NewMapper(context).OnRemoteChange(changes))

	// Check logs
	expectedLogs := `
WARN  Warning:
  - invalid config row "branch:789/component:keboola.shared-code/config:123/row:456":
    - key "code_content" should be string or array, found "int"
`
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logs.String())

	// Check config and row
	assert.Equal(t, &model.SharedCodeConfig{
		Target: `keboola.snowflake-transformation`,
	}, configState.Remote.SharedCode)
	assert.Empty(t, rowState.Remote.SharedCode)
}
