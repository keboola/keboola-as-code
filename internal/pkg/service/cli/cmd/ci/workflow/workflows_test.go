package workflow

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/terminal"
	genWorkflows "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
)

func TestAskWorkflowsOptionsInteractive(t *testing.T) {
	t.Parallel()

	// options
	o := options.New()

	// terminal
	console, err := terminal.New(t)
	require.NoError(t, err)

	p := cli.NewPrompt(console.Tty(), console.Tty(), console.Tty(), false)

	// dialog
	d := dialog.New(p, o)

	f := DefaultFlags()

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString(`Generate "validate" workflow?`))

		assert.NoError(t, console.SendLine(`n`)) // no

		assert.NoError(t, console.ExpectString(`Generate "push" workflow?`))

		assert.NoError(t, console.SendEnter()) // enter - yes

		assert.NoError(t, console.ExpectString(`Generate "pull" workflow?`))

		assert.NoError(t, console.SendLine(`n`)) // no

		assert.NoError(t, console.ExpectString(`Please select the main GitHub branch name:`))

		assert.NoError(t, console.SendEnter()) // enter - main

		assert.NoError(t, console.ExpectEOF())
	}()

	// Run
	out := AskWorkflowsOptions(f, d)
	assert.Equal(t, genWorkflows.Options{
		Validate:   false,
		Push:       true,
		Pull:       false,
		MainBranch: `main`,
	}, out)

	// Close terminal
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())
}

func TestAskWorkflowsOptionsByFlag(t *testing.T) {
	t.Parallel()

	// options
	o := options.New()

	// terminal
	console, err := terminal.New(t)
	require.NoError(t, err)

	p := cli.NewPrompt(console.Tty(), console.Tty(), console.Tty(), false)

	// dialog
	d := dialog.New(p, o)

	f := DefaultFlags()
	f.CIValidate = configmap.NewValueWithOrigin(false, configmap.SetByFlag)
	f.CIPull = configmap.NewValueWithOrigin(false, configmap.SetByFlag)
	f.CIMainBranch = configmap.NewValueWithOrigin("main", configmap.SetByFlag)
	f.CIPush = configmap.NewValueWithOrigin(true, configmap.SetByFlag)

	// Run
	out := AskWorkflowsOptions(f, d)
	assert.Equal(t, genWorkflows.Options{
		Validate:   false,
		Push:       true,
		Pull:       false,
		MainBranch: `main`,
	}, out)
}
