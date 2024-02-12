package dialog_test

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/ci/workflow"
	"sync"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"

	genWorkflows "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
)

func TestAskWorkflowsOptionsInteractive(t *testing.T) {
	t.Parallel()

	dialog, _, console := createDialogs(t, true)
	flags := workflow.DefaultFlags()
	// Default values are defined by options
	//flags := pflag.NewFlagSet(``, pflag.ExitOnError)
	//workflow.WorkflowsCmdFlags(flags)
	//assert.NoError(t, o.BindPFlags(flags))

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
	out := workflow.AskWorkflowsOptions(*flags, dialog)
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

	dialog, o, _ := createDialogs(t, true)
	o.Set(`ci-validate`, `false`)
	o.Set(`ci-push`, `true`)
	o.Set(`ci-pull`, `false`)
	o.Set(`ci-main-branch`, `main`)

	// Default values are defined by options
	flags := pflag.NewFlagSet(``, pflag.ExitOnError)
	workflow.WorkflowsCmdFlags(flags)
	assert.NoError(t, o.BindPFlags(flags))

	// Run
	out := workflow.AskWorkflowsOptions(workflow.Flags{}, dialog)
	assert.Equal(t, genWorkflows.Options{
		Validate:   false,
		Push:       true,
		Pull:       false,
		MainBranch: `main`,
	}, out)
}
