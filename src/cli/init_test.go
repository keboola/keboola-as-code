package cli

import (
	"errors"
	"github.com/ActiveState/vt10x"
	"github.com/Netflix/go-expect"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/ask"
	"keboola-as-code/src/tests"
	"keboola-as-code/src/utils"
	"os"
	"sync"
	"testing"
)

func TestMissingParams(t *testing.T) {
	tempDir := t.TempDir()
	assert.NoError(t, os.Chdir(tempDir))
	in := utils.NewBufferReader()
	out := utils.NewBufferWriter()
	root := NewRootCommand(in, out, out, ask.NewPrompt(in, out, out))
	root.cmd.SetArgs([]string{"init"})
	err := root.cmd.Execute()

	// Assert
	assert.Error(t, err)
	assert.Equal(t, "invalid parameters, see output above", err.Error())
	assert.Contains(t, out.String(), "Missing api host.")
	assert.Contains(t, out.String(), "Missing api token.")
}

func TestInteractive(t *testing.T) {
	tempDir := t.TempDir()
	assert.NoError(t, os.Chdir(tempDir))

	// Create virtual console
	c, state, err := vt10x.NewVT10XConsole()
	assert.NoError(t, err)
	defer func() {
		err := c.Close()
		assert.NoError(t, err)
		t.Logf("Console output:\n%s", expect.StripTrailingEmptyLines(state.String()))
	}()

	// Init prompt and cmd
	prompt := ask.NewPrompt(c.Tty(), c.Tty(), c.Tty())
	prompt.Interactive = true
	root := NewRootCommand(c.Tty(), c.Tty(), c.Tty(), prompt)
	root.cmd.SetArgs([]string{"init"})

	// Run cmd in background
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		assert.Equal(t, root.cmd.Execute(), errors.New("TODO FIRST PULL"))
		assert.NoError(t, c.Tty().Close())
		wg.Done()
	}()

	// Interaction
	_, err = c.ExpectString("Please enter Keboola Storage API host, eg. \"keboola.connection.com\".")
	assert.NoError(t, err)
	_, err = c.ExpectString("API host")
	assert.NoError(t, err)
	_, err = c.SendLine(tests.TestApiHost())
	assert.NoError(t, err)
	_, err = c.ExpectString("Please enter Keboola Storage API token. The value will be hidden.")
	assert.NoError(t, err)
	_, err = c.ExpectString("API token")
	assert.NoError(t, err)
	_, err = c.SendLine(tests.TestToken())
	assert.NoError(t, err)
	_, err = c.ExpectEOF()
	assert.NoError(t, err)
	wg.Wait()

	// Assert output
	out := expect.StripTrailingEmptyLines(state.String())
	assert.Contains(t, out, "? API host")
	assert.Contains(t, out, "? API token")
	assert.Contains(t, out, "Error: TODO FIRST PULL")
}
