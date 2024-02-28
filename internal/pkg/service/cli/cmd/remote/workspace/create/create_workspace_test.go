package create

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/terminal"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace/create"
)

func TestAskCreateWorkspace(t *testing.T) {
	t.Parallel()

	// options
	o := options.New()

	// terminal
	console, err := terminal.New(t)
	require.NoError(t, err)

	p := cli.NewPrompt(console.Tty(), console.Tty(), console.Tty(), false)

	// dialog
	d := dialog.New(p, o)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("Enter a name for the new workspace"))

		assert.NoError(t, console.SendLine("foo"))

		assert.NoError(t, console.ExpectString("Select a type for the new workspace"))

		assert.NoError(t, console.SendDownArrow())
		assert.NoError(t, console.SendEnter()) // python

		assert.NoError(t, console.ExpectString("Select a size for the new workspace"))

		assert.NoError(t, console.SendEnter()) // small

		assert.NoError(t, console.ExpectEOF())
	}()

	// Run
	opts, err := AskCreateWorkspace(d, Flags{})
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, create.CreateOptions{
		Name: "foo",
		Type: "python",
		Size: "small",
	}, opts)
}
