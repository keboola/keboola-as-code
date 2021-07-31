package cli

import (
	"io"
	"keboola-as-code/src/interaction"
	"keboola-as-code/src/utils"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ActiveState/vt10x"
	"github.com/Netflix/go-expect"
	"github.com/stretchr/testify/assert"
)

func TestMissingParams(t *testing.T) {
	tempDir := t.TempDir()
	assert.NoError(t, os.Chdir(tempDir))
	in := utils.NewBufferReader()
	out := utils.NewBufferWriter()
	root := NewRootCommand(in, out, out, interaction.NewPrompt(in, out, out))
	root.cmd.SetArgs([]string{"init"})
	err := root.cmd.Execute()

	// Assert
	assert.Error(t, err)
	assert.Equal(t, "invalid parameters, see output above", err.Error())
	assert.Contains(t, out.String(), "Missing api host.")
	assert.Contains(t, out.String(), "Missing api token.")
}

func TestInteractiveInit(t *testing.T) {
	tempDir := t.TempDir()
	assert.NoError(t, os.Chdir(tempDir))

	// Create virtual console
	var stdout io.Writer
	if utils.TestIsVerbose() {
		stdout = os.Stdout
	} else {
		stdout = io.Discard
	}
	c, state, err := vt10x.NewVT10XConsole(expect.WithStdout(stdout), expect.WithDefaultTimeout(10*time.Second))
	assert.NoError(t, err)

	// Init prompt and cmd
	prompt := interaction.NewPrompt(c.Tty(), c.Tty(), c.Tty())
	prompt.Interactive = true
	root := NewRootCommand(c.Tty(), c.Tty(), c.Tty(), prompt)
	root.cmd.SetArgs([]string{"init"})

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err = c.ExpectString("Please enter Keboola Storage API host, eg. \"connection.keboola.com\".")
		assert.NoError(t, err)
		_, err = c.ExpectString("API host ")
		assert.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
		_, err = c.SendLine(utils.TestApiHost())
		assert.NoError(t, err)
		_, err = c.ExpectString("Please enter Keboola Storage API token. The value will be hidden.")
		assert.NoError(t, err)
		_, err = c.ExpectString("API token ")
		assert.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
		_, err = c.SendLine(utils.TestTokenMaster())
		assert.NoError(t, err)
		_, err = c.ExpectString("Allowed branches")
		assert.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
		_, err = c.SendLine("\n") // enter, first option "only main branch"
		assert.NoError(t, err)
		_, err = c.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run cmd
	assert.NoError(t, root.cmd.Execute())
	assert.NoError(t, c.Tty().Close())
	wg.Wait()
	assert.NoError(t, c.Close())

	// Assert output
	out := expect.StripTrailingEmptyLines(state.String())
	assert.Contains(t, out, "Pull done.")
}
