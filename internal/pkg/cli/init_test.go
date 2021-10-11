package cli

import (
	"sync"
	"testing"
	"time"

	"github.com/Netflix/go-expect"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
)

func TestMissingParams(t *testing.T) {
	root, out := newTestRootCommand()
	root.cmd.SetArgs([]string{"init"})
	err := root.cmd.Execute()

	// Assert
	assert.Error(t, err)
	assert.Equal(t, "invalid parameters, see output above", err.Error())
	logStr := out.String()
	assert.Contains(t, logStr, "Missing api host.")
	assert.Contains(t, logStr, "Missing api token.")
}

func TestInteractiveInit(t *testing.T) {
	// Create virtual console
	c, state, err := testhelper.NewVirtualTerminal(expect.WithStdout(testhelper.VerboseStdout()), expect.WithDefaultTimeout(15*time.Second))
	assert.NoError(t, err)

	// Init prompt and cmd
	root := newTestRootCommandWithTty(c.Tty())
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
		_, err = c.SendLine(testhelper.TestApiHost())
		assert.NoError(t, err)
		_, err = c.ExpectString("Please enter Keboola Storage API token. The value will be hidden.")
		assert.NoError(t, err)
		_, err = c.ExpectString("API token ")
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = c.SendLine(testhelper.TestToken())
		assert.NoError(t, err)
		_, err = c.ExpectString("Allowed project's branches:")
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = c.Send(testhelper.Enter) // enter, first option "only main branch"
		assert.NoError(t, err)
		_, err = c.ExpectString(`Generate workflows files for GitHub Actions?`)
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = c.Send(testhelper.Enter) // enter - yes
		assert.NoError(t, err)
		_, err = c.ExpectString(`Please confirm GitHub Actions you want to generate.`)
		assert.NoError(t, err)
		_, err = c.ExpectString(`Generate "validate" workflow?`)
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = c.Send(testhelper.Enter) // enter - yes
		assert.NoError(t, err)
		_, err = c.ExpectString(`Generate "push" workflow?`)
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = c.Send(testhelper.Enter) // enter - yes
		assert.NoError(t, err)
		_, err = c.ExpectString(`Generate "pull" workflow?`)
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = c.Send(testhelper.Enter) // enter - yes
		assert.NoError(t, err)
		_, err = c.ExpectString(`Please select the main GitHub branch name:`)
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = c.Send(testhelper.Enter) // enter - main
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
