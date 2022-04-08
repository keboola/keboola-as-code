package codes_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeLocalLoad(t *testing.T) {
	t.Parallel()

	state, d := createStateWithLocalMapper(t)
	logger := d.DebugLogger()
	fs := d.Fs()
	configPath := model.NewAbsPath("branch", "config")
	rowPath := model.NewAbsPath("branch/config", "row")
	targetComponentId := model.ComponentId(`keboola.python-transformation-v2`)
	config, row := createSharedCode(t, targetComponentId, state, false)

	// Write file
	codeFilePath := filesystem.Join(state.NamingGenerator().SharedCodeFilePath(rowPath, targetComponentId))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(codeFilePath, `foo bar`)))
	logger.Truncate()

	// Load config
	configRecipe := model.NewLocalLoadRecipe(d.FileLoader(), configPath, config)
	assert.NoError(t, state.Mapper().MapAfterLocalLoad(configRecipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Load row
	state.MustAdd(config)
	rowRecipe := model.NewLocalLoadRecipe(d.FileLoader(), rowPath, row)
	assert.NoError(t, state.Mapper().MapAfterLocalLoad(rowRecipe))
	assert.Equal(t, "DEBUG  Loaded \"branch/config/row/code.py\"\n", logger.AllMessages())

	// Structs are set
	assert.Equal(t, &model.SharedCodeConfig{
		Target: "keboola.python-transformation-v2",
	}, config.SharedCode)
	assert.Equal(t, &model.SharedCodeRow{
		Target: "keboola.python-transformation-v2",
		Scripts: model.Scripts{
			model.StaticScript{Value: `foo bar`},
		},
	}, row.SharedCode)

	// Shared code is loaded
	sharedCodeFile := rowRecipe.Files.GetOneByTag(model.FileKindNativeSharedCode)
	assert.NotNil(t, sharedCodeFile)
}

func TestSharedCodeLocalLoad_MissingCodeFile(t *testing.T) {
	t.Parallel()

	state, d := createStateWithLocalMapper(t)
	logger := d.DebugLogger()
	configPath := model.NewAbsPath("branch", "config")
	rowPath := model.NewAbsPath("branch/config", "row")
	targetComponentId := model.ComponentId(`keboola.python-transformation-v2`)
	config, row := createSharedCode(t, targetComponentId, state, false)

	// Load config
	configRecipe := model.NewLocalLoadRecipe(d.FileLoader(), configPath, config)
	assert.NoError(t, state.Mapper().MapAfterLocalLoad(configRecipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Load row
	state.MustAdd(config)
	rowRecipe := model.NewLocalLoadRecipe(d.FileLoader(), rowPath, row)
	err := state.Mapper().MapAfterLocalLoad(rowRecipe)
	assert.Error(t, err)
	assert.Equal(t, `missing shared code file "branch/config/row/code.py"`, err.Error())
	assert.Empty(t, logger.WarnAndErrorMessages())
}
