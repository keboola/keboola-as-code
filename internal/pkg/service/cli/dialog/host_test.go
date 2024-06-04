package dialog_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

		require.NoError(t, console.ExpectString("API host: "))

		require.NoError(t, console.SendLine(`https://foo.bar.com/`))

		require.NoError(t, console.ExpectEOF())
	}()

	// Run
	out := dialog.AskStorageAPIHost()
	assert.Equal(t, `https://foo.bar.com/`, out)

	// Close terminal
	require.NoError(t, console.Tty().Close())
	wg.Wait()
	require.NoError(t, console.Close())
}

func TestAskStorageAPIHost_HTTP(t *testing.T) {
	t.Parallel()

	dialog, console := createDialogs(t, true)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		require.NoError(t, console.ExpectString("API host: "))

		require.NoError(t, console.SendLine(`http://foo.bar.com/`))

		require.NoError(t, console.ExpectEOF())
	}()

	// Run
	out := dialog.AskStorageAPIHost()
	assert.Equal(t, `http://foo.bar.com/`, out)

	// Close terminal
	require.NoError(t, console.Tty().Close())
	wg.Wait()
	require.NoError(t, console.Close())
}

func TestAPIHostValidator(t *testing.T) {
	t.Parallel()
	require.NoError(t, StorageAPIHostValidator("connection.keboola.com"))
	require.NoError(t, StorageAPIHostValidator("connection.keboola.com/"))
	require.NoError(t, StorageAPIHostValidator("https://connection.keboola.com"))
	require.NoError(t, StorageAPIHostValidator("https://connection.keboola.com/"))

	err := StorageAPIHostValidator("")
	require.Error(t, err)
	assert.Equal(t, "value is required", err.Error())

	err = StorageAPIHostValidator("@#$$%^&%#$&")
	require.Error(t, err)
	assert.Equal(t, "invalid host", err.Error())
}
