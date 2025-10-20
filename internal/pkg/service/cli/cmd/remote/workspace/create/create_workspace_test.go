package create

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace/create"
)

func TestAskCreateWorkspace(t *testing.T) {
	t.Parallel()

	d, console := dialog.NewForTest(t, true)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Go(func() {
		require.NoError(t, console.ExpectString("Enter a name for the new workspace"))

		require.NoError(t, console.SendLine("foo"))

		require.NoError(t, console.ExpectString("Select a type for the new workspace"))

		require.NoError(t, console.SendDownArrow())
		require.NoError(t, console.SendEnter()) // python

		require.NoError(t, console.ExpectString("Select a size for the new workspace"))

		require.NoError(t, console.SendEnter()) // small

		require.NoError(t, console.ExpectEOF())
	})

	// Run
	opts, err := AskCreateWorkspace(d, Flags{})
	require.NoError(t, err)
	require.NoError(t, console.Tty().Close())
	wg.Wait()
	require.NoError(t, console.Close())

	// Assert
	assert.Equal(t, create.CreateOptions{
		Name: "foo",
		Type: "python",
		Size: "small",
	}, opts)
}
