package cli

import (
	"errors"
	"github.com/ActiveState/vt10x"
	"github.com/Netflix/go-expect"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/utils"
	"sync"
	"testing"
)

func TestInitCmdExecute(t *testing.T) {
	in := utils.NewBufferReader()
	out := utils.NewBufferWriter()
	root := NewRootCommand(in, out, out, NewPrompt(in, out, out))
	root.cmd.SetArgs([]string{"init", "--storage-api-host", "foo", "--storage-api-token", "bar"})
	err := root.cmd.Execute()
	assert.Error(t, err)
	assert.Equal(t, "TODO", err.Error())
}

func TestMissingParams(t *testing.T) {
	in := utils.NewBufferReader()
	out := utils.NewBufferWriter()
	root := NewRootCommand(in, out, out, NewPrompt(in, out, out))
	root.cmd.SetArgs([]string{"init"})
	err := root.cmd.Execute()
	assert.NoError(t, out.Flush())

	// Assert
	assert.Error(t, err)
	assert.Equal(t, "invalid parameters, see output above", err.Error())
	assert.Contains(t, out.Buffer.String(), "Missing api host.")
	assert.Contains(t, out.Buffer.String(), "Missing api token.")
}

func TestInteractive(t *testing.T) {
	// Create virtual console
	c, state, err := vt10x.NewVT10XConsole()
	assert.NoError(t, err)
	defer func() {
		err := c.Close()
		assert.NoError(t, err)
		t.Logf("Console output:\n%s", expect.StripTrailingEmptyLines(state.String()))
	}()

	// Init prompt and cmd
	prompt := NewPrompt(c.Tty(), c.Tty(), c.Tty())
	prompt.Interactive = true
	root := NewRootCommand(c.Tty(), c.Tty(), c.Tty(), prompt)
	root.cmd.SetArgs([]string{"init"})

	// Run cmd in background
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		assert.Equal(t, root.cmd.Execute(), errors.New("TODO"))
		assert.NoError(t, c.Tty().Close())
		wg.Done()
	}()

	// Interaction
	_, err = c.ExpectString("Please enter Keboola Storage API host, eg. \"keboola.connection.com\".")
	assert.NoError(t, err)
	_, err = c.ExpectString("API host")
	assert.NoError(t, err)
	_, err = c.SendLine("keboola.connection.com")
	assert.NoError(t, err)
	_, err = c.ExpectString("Please enter Keboola Storage API token. The value will be hidden.")
	assert.NoError(t, err)
	_, err = c.ExpectString("API token")
	assert.NoError(t, err)
	_, err = c.SendLine("mytoken")
	assert.NoError(t, err)
	_, err = c.ExpectEOF()
	assert.NoError(t, err)
	wg.Wait()

	// Assert output
	out := expect.StripTrailingEmptyLines(state.String())
	assert.Contains(t, out, "? API host")
	assert.Contains(t, out, "? API token")
	assert.Contains(t, out, "Error: TODO")
}

func TestApiHostValidator(t *testing.T) {
	assert.NoError(t, apiHostValidator("keboola.connection.com"))
	assert.NoError(t, apiHostValidator("keboola.connection.com/"))
	assert.NoError(t, apiHostValidator("https://keboola.connection.com"))
	assert.NoError(t, apiHostValidator("https://keboola.connection.com/"))
	assert.Equal(t, errors.New("value is required"), apiHostValidator(""))
	assert.Equal(t, errors.New("invalid host"), apiHostValidator("@#$$%^&%#$&"))
}
