package cli

import (
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/utils"
	"testing"
)

func TestInitCmdExecute(t *testing.T) {
	writer, _ := utils.NewBufferWriter()
	root := NewRootCommand(writer, writer)
	root.cmd.SetArgs([]string{"init", "--api-url", "foo", "--token", "bar"})
	err := root.cmd.Execute()
	assert.Error(t, err)
	assert.Equal(t, "TODO", err.Error())
}

func TestMissingParams(t *testing.T) {
	writer, buffer := utils.NewBufferWriter()
	root := NewRootCommand(writer, writer)
	root.cmd.SetArgs([]string{"init"})
	err := root.cmd.Execute()
	assert.NoError(t, writer.Flush())

	// Assert
	assert.Error(t, err)
	assert.Equal(t, "invalid parameters, see output above", err.Error())
	assert.Contains(t, buffer.String(), "Missing API URL.")
	assert.Contains(t, buffer.String(), "Missing API token.")
}
