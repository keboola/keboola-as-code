package codes_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/codes"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeLocalLoad(t *testing.T) {
	t.Parallel()
	targetComponentId := model.ComponentId(`keboola.python-transformation-v2`)
	context, logs, configState, rowState := createLocalSharedCode(t, targetComponentId)

	// Write file
	codeFilePath := filesystem.Join(context.NamingGenerator.SharedCodeFilePath(rowState.ConfigRowManifest.Path(), targetComponentId))
	assert.NoError(t, context.Fs.WriteFile(filesystem.NewFile(codeFilePath, `foo bar`)))
	logs.Truncate()

	// Load config
	configRecipe := fixtures.NewLocalLoadRecipe(configState.Manifest(), configState.Local)
	err := NewMapper(context).MapAfterLocalLoad(configRecipe)
	assert.NoError(t, err)
	assert.Empty(t, logs.AllMsgs())

	// Load row
	rowRecipe := fixtures.NewLocalLoadRecipe(rowState.Manifest(), rowState.Local)
	err = NewMapper(context).MapAfterLocalLoad(rowRecipe)
	assert.NoError(t, err)
	assert.Equal(t, "DEBUG  Loaded \"branch/config/row/code.py\"\n", logs.AllMsgs())

	// Structs are set
	assert.Equal(t, &model.SharedCodeConfig{
		Target: "keboola.python-transformation-v2",
	}, configState.Local.SharedCode)
	assert.Equal(t, &model.SharedCodeRow{
		Target: "keboola.python-transformation-v2",
		Scripts: model.Scripts{
			model.StaticScript{Value: `foo bar`},
		},
	}, rowState.Local.SharedCode)

	// Shared code is loaded
	sharedCodeFile := rowRecipe.Files.GetOneByTag(model.FileKindNativeSharedCode)
	assert.NotNil(t, sharedCodeFile)
}

func TestSharedCodeLocalLoad_MissingCodeFile(t *testing.T) {
	t.Parallel()
	targetComponentId := model.ComponentId(`keboola.python-transformation-v2`)
	context, logs, configState, rowState := createLocalSharedCode(t, targetComponentId)

	// Load config
	configRecipe := fixtures.NewLocalLoadRecipe(configState.Manifest(), configState.Local)
	err := NewMapper(context).MapAfterLocalLoad(configRecipe)
	assert.NoError(t, err)
	assert.Empty(t, logs.AllMsgs())

	// Load row
	rowRecipe := fixtures.NewLocalLoadRecipe(rowState.Manifest(), rowState.Local)
	err = NewMapper(context).MapAfterLocalLoad(rowRecipe)
	assert.Error(t, err)
	assert.Equal(t, `missing shared code file "branch/config/row/code.py"`, err.Error())
	assert.Empty(t, logs.AllMsgs())
}
