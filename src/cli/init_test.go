package cli

import (
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/utils"
	"testing"
)

func TestInitCmdExecute(t *testing.T) {
	in, _ := utils.NewBufferReader()
	out, _ := utils.NewBufferWriter()
	root := NewRootCommand(in, out, out)
	root.cmd.SetArgs([]string{"init", "--storage-api-url", "foo", "--storage-api-token", "bar"})
	err := root.cmd.Execute()
	assert.Error(t, err)
	assert.Equal(t, "TODO", err.Error())
}

func TestMissingParams(t *testing.T) {
	in, _ := utils.NewBufferReader()
	out, buffer := utils.NewBufferWriter()
	root := NewRootCommand(in, out, out)
	root.cmd.SetArgs([]string{"init"})
	err := root.cmd.Execute()
	assert.NoError(t, out.Flush())

	// Assert
	assert.Error(t, err)
	assert.Equal(t, "invalid parameters, see output above", err.Error())
	assert.Contains(t, buffer.String(), "Missing api url.")
	assert.Contains(t, buffer.String(), "Missing api token.")
}
