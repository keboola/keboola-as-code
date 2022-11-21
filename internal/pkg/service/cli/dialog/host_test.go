package dialog_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestAskStorageApiHostInteractive(t *testing.T) {
	t.Parallel()

	dialog, console := createDialogs(t, true)
	d := dependencies.NewMockedDeps()

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
	out, err := dialog.AskStorageApiHost(d)
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
	d := dependencies.NewMockedDeps()
	opts := d.Options()
	opts.Set(`storage-api-host`, `foo.bar.com`)

	// Run
	out, err := dialog.AskStorageApiHost(d)
	assert.Equal(t, `foo.bar.com`, out)
	assert.NoError(t, err)
}

func TestAskStorageApiHostMissing(t *testing.T) {
	t.Parallel()

	dialog, _ := createDialogs(t, false)
	d := dependencies.NewMockedDeps()

	// Run
	out, err := dialog.AskStorageApiHost(d)
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

	err := StorageApiHostValidator("")
	assert.Error(t, err)
	assert.Equal(t, "value is required", err.Error())

	err = StorageApiHostValidator("@#$$%^&%#$&")
	assert.Error(t, err)
	assert.Equal(t, "invalid host", err.Error())
}
