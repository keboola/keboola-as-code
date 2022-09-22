package dialog_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
)

func TestAskStorageApiTokenInteractive(t *testing.T) {
	t.Parallel()

	dialog, console := createDialogs(t, true)
	o := options.New()

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
	out, err := dialog.AskStorageApiToken(o)
	assert.Equal(t, `my-secret`, out)
	assert.NoError(t, err)

	// Close terminal
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())
}

func TestAskStorageApiTokenByFlag(t *testing.T) {
	t.Parallel()

	dialog, _ := createDialogs(t, true)
	o := options.New()
	o.Set(`storage-api-token`, `my-secret`)

	// Run
	out, err := dialog.AskStorageApiToken(o)
	assert.Equal(t, `my-secret`, out)
	assert.NoError(t, err)
}

func TestAskStorageApiTokenMissing(t *testing.T) {
	t.Parallel()

	dialog, _ := createDialogs(t, false)
	o := options.New()

	// Run
	out, err := dialog.AskStorageApiToken(o)
	assert.Empty(t, out)
	assert.Error(t, err)
	assert.Equal(t, `missing Storage API token`, err.Error())
}
