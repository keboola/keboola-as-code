package dialog_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
)

func TestAskStorageAPIHost_HTTPS(t *testing.T) {
	t.Parallel()

	dialog, console := createDialogs(t, true)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("API host: "))

		assert.NoError(t, console.SendLine(`https://foo.bar.com/`))

		assert.NoError(t, console.ExpectEOF())
	}()

	// Run
	out := dialog.AskStorageAPIHost()
	assert.Equal(t, `https://foo.bar.com/`, out)

	// Close terminal
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())
}

func TestAskStorageAPIHost_HTTP(t *testing.T) {
	t.Parallel()

	dialog, console := createDialogs(t, true)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("API host: "))

		assert.NoError(t, console.SendLine(`http://foo.bar.com/`))

		assert.NoError(t, console.ExpectEOF())
	}()

	// Run
	out := dialog.AskStorageAPIHost()
	assert.Equal(t, `http://foo.bar.com/`, out)

	// Close terminal
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())
}

func TestAPIHostValidator(t *testing.T) {
	t.Parallel()
	assert.NoError(t, StorageAPIHostValidator("connection.keboola.com"))
	assert.NoError(t, StorageAPIHostValidator("connection.keboola.com/"))
	assert.NoError(t, StorageAPIHostValidator("https://connection.keboola.com"))
	assert.NoError(t, StorageAPIHostValidator("https://connection.keboola.com/"))

	err := StorageAPIHostValidator("")
	assert.Error(t, err)
	assert.Equal(t, "value is required", err.Error())

	err = StorageAPIHostValidator("@#$$%^&%#$&")
	assert.Error(t, err)
	assert.Equal(t, "invalid host", err.Error())
}
