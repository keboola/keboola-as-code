package dialog_test

import (
	"sync"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/cmd/ci"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	genWorkflows "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
)

func TestAskWorkflowsOptionsInteractive(t *testing.T) {
	t.Parallel()

	dialog, console := createDialogs(t, true)
	o := options.New()

	// Default values are defined by options
	flags := pflag.NewFlagSet(``, pflag.ExitOnError)
	ci.WorkflowsCmdFlags(flags)
	assert.NoError(t, o.BindPFlags(flags))

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err := console.ExpectString(`Generate "validate" workflow?`)
		assert.NoError(t, err)

		_, err = console.SendLine(`n`) // no
		assert.NoError(t, err)

		_, err = console.ExpectString(`Generate "push" workflow?`)
		assert.NoError(t, err)

		_, err = console.Send(testhelper.Enter) // enter - yes
		assert.NoError(t, err)

		_, err = console.ExpectString(`Generate "pull" workflow?`)
		assert.NoError(t, err)

		_, err = console.SendLine(`n`) // no
		assert.NoError(t, err)

		_, err = console.ExpectString(`Please select the main GitHub branch name:`)
		assert.NoError(t, err)

		_, err = console.Send(testhelper.Enter) // enter - main
		assert.NoError(t, err)

		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	out := dialog.AskWorkflowsOptions(o)
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

	dialog, _ := createDialogs(t, true)
	o := options.New()
	o.Set(`ci-validate`, `false`)
	o.Set(`ci-push`, `true`)
	o.Set(`ci-pull`, `false`)
	o.Set(`ci-main-branch`, `main`)

	// Default values are defined by options
	flags := pflag.NewFlagSet(``, pflag.ExitOnError)
	ci.WorkflowsCmdFlags(flags)
	assert.NoError(t, o.BindPFlags(flags))

	// Run
	out := dialog.AskWorkflowsOptions(o)
	assert.Equal(t, genWorkflows.Options{
		Validate:   false,
		Push:       true,
		Pull:       false,
		MainBranch: `main`,
	}, out)
}
