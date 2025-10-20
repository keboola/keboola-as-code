package dialog_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAskStorageApiToken(t *testing.T) {
	t.Parallel()

	dialog, console := createDialogs(t, true)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Go(func() {
		require.NoError(t, console.ExpectString("API token: "))

		require.NoError(t, console.SendLine(`my-secret`))

		require.NoError(t, console.ExpectEOF())
	})

	// Run
	out := dialog.AskStorageAPIToken()
	assert.Equal(t, `my-secret`, out)

	// Close terminal
	require.NoError(t, console.Tty().Close())
	wg.Wait()
	require.NoError(t, console.Close())
}
