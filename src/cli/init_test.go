package cli

import (
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ActiveState/vt10x"
	"github.com/Netflix/go-expect"
	"github.com/stretchr/testify/assert"

	"keboola-as-code/src/interaction"
	"keboola-as-code/src/utils"
)

const Enter = "\n"

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
	c, state, err := vt10x.NewVT10XConsole(expect.WithStdout(stdout), expect.WithDefaultTimeout(15*time.Second))
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
		time.Sleep(20 * time.Millisecond)
		_, err = c.SendLine(utils.TestApiHost())
		assert.NoError(t, err)
		_, err = c.ExpectString("Please enter Keboola Storage API token. The value will be hidden.")
		assert.NoError(t, err)
		_, err = c.ExpectString("API token ")
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = c.SendLine(utils.TestTokenMaster())
		assert.NoError(t, err)
		_, err = c.ExpectString("Allowed project's branches:")
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = c.SendLine(Enter) // enter, first option "only main branch"
		assert.NoError(t, err)
		_, err = c.ExpectString(`Generate workflows files for GitHub Actions?`)
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = c.SendLine(Enter) // enter - yes
		assert.NoError(t, err)
		_, err = c.ExpectString(`Please confirm GitHub Actions you want to generate.`)
		assert.NoError(t, err)
		_, err = c.ExpectString(`Generate "validate" workflow?`)
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = c.SendLine(Enter) // enter - yes
		assert.NoError(t, err)
		_, err = c.ExpectString(`Generate "push" workflow?`)
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = c.SendLine(Enter) // enter - yes
		assert.NoError(t, err)
		_, err = c.ExpectString(`Generate "pull" workflow?`)
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = c.SendLine(Enter) // enter - yes
		assert.NoError(t, err)
		_, err = c.ExpectString(`Please select the main GitHub branch name:`)
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = c.SendLine(Enter) // enter - main
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
	assert.Contains(t, out, "CI workflows have been generated.")
}
