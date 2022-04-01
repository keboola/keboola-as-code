package codes_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeRemoteLoad(t *testing.T) {
	t.Parallel()

	state, d := createStateWithRemoteMapper(t)
	logger := d.DebugLogger()
	targetComponentId := model.ComponentId(`keboola.snowflake-transformation`)
	config, row := createSharedCode(t, targetComponentId, state, true)

	row.Content.Set(model.SharedCodeContentKey, []interface{}{
		"SELECT 1;",
		"SELECT 2;",
	})

	// Invoke
	assert.NoError(t, state.Mapper().AfterRemoteOperation(model.NewChanges().AddLoaded(config, row)))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Check config
	assert.Equal(t, &model.SharedCodeConfig{
		Target: targetComponentId,
	}, config.SharedCode)

	// Check row
	assert.Equal(t, &model.SharedCodeRow{
		Target: targetComponentId,
		Scripts: model.Scripts{
			model.StaticScript{
				Value: "SELECT 1;",
			},
			model.StaticScript{
				Value: "SELECT 2;",
			},
		},
	}, row.SharedCode)
}

func TestSharedCodeRemoteLoad_Legacy(t *testing.T) {
	t.Parallel()

	state, d := createStateWithRemoteMapper(t)
	logger := d.DebugLogger()
	targetComponentId := model.ComponentId(`keboola.snowflake-transformation`)
	config, row := createSharedCode(t, targetComponentId, state, true)

	row.Content.Set(model.SharedCodeContentKey, "SELECT 1; \n  SELECT 2; \n ")

	// Invoke
	assert.NoError(t, state.Mapper().AfterRemoteOperation(model.NewChanges().AddLoaded(config, row)))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Check config
	assert.Equal(t, &model.SharedCodeConfig{
		Target: targetComponentId,
	}, config.SharedCode)

	// Check row
	assert.Equal(t, &model.SharedCodeRow{
		Target: targetComponentId,
		Scripts: model.Scripts{
			model.StaticScript{
				Value: "SELECT 1;",
			},
			model.StaticScript{
				Value: "SELECT 2;",
			},
		},
	}, row.SharedCode)
}

func TestSharedCodeRemoteLoad_UnexpectedTypeInConfig(t *testing.T) {
	t.Parallel()

	state, d := createStateWithRemoteMapper(t)
	logger := d.DebugLogger()
	targetComponentId := model.ComponentId(`keboola.snowflake-transformation`)
	config, row := createSharedCode(t, targetComponentId, state, true)

	config.Content.Set(model.ShareCodeTargetComponentKey, 123)

	// Invoke
	assert.NoError(t, state.Mapper().AfterRemoteOperation(model.NewChanges().AddLoaded(config, row)))

	// Check logs
	expectedLogs := `
WARN  Warning:
  - invalid config "branch:123/component:keboola.shared-code/config:123":
    - key "componentId" should be string, found "int"
`
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logger.AllMessages())

	// Check config and row
	assert.Empty(t, config.SharedCode)
	assert.Empty(t, row.SharedCode)
}

func TestSharedCodeRemoteLoad_UnexpectedTypeInRow(t *testing.T) {
	t.Parallel()

	state, d := createStateWithRemoteMapper(t)
	logger := d.DebugLogger()
	targetComponentId := model.ComponentId(`keboola.snowflake-transformation`)
	config, row := createSharedCode(t, targetComponentId, state, true)

	row.Content.Set(model.SharedCodeContentKey, 123)

	// Invoke
	assert.NoError(t, state.Mapper().AfterRemoteOperation(model.NewChanges().AddLoaded(config, row)))

	// Check logs
	expectedLogs := `
WARN  Warning:
  - invalid config row "branch:123/component:keboola.shared-code/config:123/row:456":
    - key "code_content" should be string or array, found "int"
`
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logger.AllMessages())

	// Check config and row
	assert.Equal(t, &model.SharedCodeConfig{
		Target: targetComponentId,
	}, config.SharedCode)
	assert.Empty(t, row.SharedCode)
}
