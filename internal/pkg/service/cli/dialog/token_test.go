package dialog_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAskStorageApiToken(t *testing.T) {
	t.Parallel()

	dialog, _, console := createDialogs(t, true)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("API token: "))

		assert.NoError(t, console.SendLine(`my-secret`))

		assert.NoError(t, console.ExpectEOF())
	}()

	// Run
	out := dialog.AskStorageAPIToken()
	assert.Equal(t, `my-secret`, out)

	// Close terminal
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())
}
