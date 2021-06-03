package cli

import (
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/utils"
	"testing"
)

func TestInitCmdExecute(t *testing.T) {
	writer, _ := utils.NewBufferWriter()
	root := NewRootCommand(writer, writer)
	root.cmd.SetArgs([]string{"init"})
	err := root.cmd.Execute()
	assert.Error(t, err)
	assert.Equal(t, "TODO", err.Error())
}
