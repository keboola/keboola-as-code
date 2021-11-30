package sync_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Netflix/go-expect"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/testcli"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/testproject"
)

func TestMissingParams(t *testing.T) {
	t.Parallel()
	root, out := testcli.NewTestRootCommand(testhelper.NewMemoryFs())
	root.SetArgs([]string{"init"})
	exitCode := root.Execute()
	assert.Equal(t, 1, exitCode)

	// Assert
	expectedOut := `
Error:
  - missing Storage Api host, please use "--storage-api-host" flag or ENV variable "KBC_STORAGE_API_HOST"
  - missing Storage Api token, please use "--storage-api-token" flag or ENV variable "KBC_STORAGE_API_TOKEN"
`
	assert.Equal(t, strings.TrimLeft(expectedOut, "\n"), out.String())
}

func TestInteractiveInit(t *testing.T) {
	t.Parallel()

	// Create virtual console
	c, state, err := testhelper.NewVirtualTerminal(t, expect.WithStdout(testhelper.VerboseStdout()), expect.WithDefaultTimeout(15*time.Second))
	assert.NoError(t, err)

	// Test project
	project := testproject.GetTestProject(t, env.Empty())
	project.SetState(`empty.json`)

	// Init prompt and cmd
	root := testcli.NewTestRootCommandWithTty(c.Tty(), testhelper.NewMemoryFs())
	root.SetArgs([]string{"init"})

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
		_, err = c.SendLine(project.StorageApiHost())
		assert.NoError(t, err)
		_, err = c.ExpectString("Please enter Keboola Storage API token. The value will be hidden.")
		assert.NoError(t, err)
		_, err = c.ExpectString("API token ")
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = c.SendLine(project.Token())
		assert.NoError(t, err)
		_, err = c.ExpectString("Allowed project's branches:")
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = c.Send(testhelper.Enter) // enter, first option "only main branch"
		assert.NoError(t, err)
		_, err = c.ExpectString(`Do you want to include object IDs in directory structure?`)
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = c.SendLine(`Y`) // yes
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
	assert.NoError(t, root.Cmd.Execute())
	assert.NoError(t, c.Tty().Close())
	wg.Wait()
	assert.NoError(t, c.Close())

	// Assert output
	out := expect.StripTrailingEmptyLines(state.String())
	assert.Contains(t, out, "CI workflows have been generated.")
}
