package dialog_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/ci/workflow"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	genWorkflows "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
)

func TestAskWorkflowsOptionsInteractive(t *testing.T) {
	t.Parallel()

	dialog, console := createDialogs(t, true)

	f := workflow.DefaultFlags()

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
	out := workflow.AskWorkflowsOptions(f, dialog)
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

	f := workflow.DefaultFlags()
	f.CIValidate = configmap.NewValueWithOrigin(false, configmap.SetByFlag)
	f.CIPull = configmap.NewValueWithOrigin(false, configmap.SetByFlag)
	f.CIMainBranch = configmap.NewValueWithOrigin("main", configmap.SetByFlag)
	f.CIPush = configmap.NewValueWithOrigin(true, configmap.SetByFlag)

	// Run
	out := workflow.AskWorkflowsOptions(f, dialog)
	assert.Equal(t, genWorkflows.Options{
		Validate:   false,
		Push:       true,
		Pull:       false,
		MainBranch: `main`,
	}, out)
}
