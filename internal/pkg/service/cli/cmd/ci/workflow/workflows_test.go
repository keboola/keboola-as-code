package workflow

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	genWorkflows "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
)

func TestAskWorkflowsOptionsInteractive(t *testing.T) {
	t.Parallel()

	d, console := dialog.NewForTest(t, true)

	f := DefaultFlags()

	// Interaction
	wg := sync.WaitGroup{}
	wg.Go(func() {

		require.NoError(t, console.ExpectString(`Generate "validate" workflow?`))

		require.NoError(t, console.SendLine(`n`)) // no

		require.NoError(t, console.ExpectString(`Generate "push" workflow?`))

		require.NoError(t, console.SendEnter()) // enter - yes

		require.NoError(t, console.ExpectString(`Generate "pull" workflow?`))

		require.NoError(t, console.SendLine(`n`)) // no

		require.NoError(t, console.ExpectString(`Please select the main GitHub branch name:`))

		require.NoError(t, console.SendEnter()) // enter - main

		require.NoError(t, console.ExpectEOF())
	})

	// Run
	out := AskWorkflowsOptions(f, d)
	assert.Equal(t, genWorkflows.Options{
		Validate:   false,
		Push:       true,
		Pull:       false,
		MainBranch: `main`,
	}, out)

	// Close terminal
	require.NoError(t, console.Tty().Close())
	wg.Wait()
	require.NoError(t, console.Close())
}

func TestAskWorkflowsOptionsByFlag(t *testing.T) {
	t.Parallel()

	d, _ := dialog.NewForTest(t, true)

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
