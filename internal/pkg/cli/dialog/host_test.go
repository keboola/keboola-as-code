package dialog_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
)

func TestAskStorageApiHostInteractive(t *testing.T) {
	t.Parallel()

	dialog, console := createDialogs(t, true)
	o := options.New()

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
	out, err := dialog.AskStorageApiHost(o)
	assert.Equal(t, `foo.bar.com`, out)
	assert.NoError(t, err)

	// Close terminal
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())
}

func TestAskStorageApiHostByFlag(t *testing.T) {
	t.Parallel()

	dialog, _ := createDialogs(t, true)
	o := options.New()
	o.Set(`storage-api-host`, `foo.bar.com`)

	// Run
	out, err := dialog.AskStorageApiHost(o)
	assert.Equal(t, `foo.bar.com`, out)
	assert.NoError(t, err)
}

func TestAskStorageApiHostMissing(t *testing.T) {
	t.Parallel()

	dialog, _ := createDialogs(t, false)
	o := options.New()

	// Run
	out, err := dialog.AskStorageApiHost(o)
	assert.Empty(t, out)
	assert.Error(t, err)
	assert.Equal(t, `missing Storage API host`, err.Error())
}

func TestApiHostValidator(t *testing.T) {
	t.Parallel()
	assert.NoError(t, StorageApiHostValidator("connection.keboola.com"))
	assert.NoError(t, StorageApiHostValidator("connection.keboola.com/"))
	assert.NoError(t, StorageApiHostValidator("https://connection.keboola.com"))
	assert.NoError(t, StorageApiHostValidator("https://connection.keboola.com/"))
	assert.Equal(t, errors.New("value is required"), StorageApiHostValidator(""))
	assert.Equal(t, errors.New("invalid host"), StorageApiHostValidator("@#$$%^&%#$&"))
}
