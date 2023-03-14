package dependencies

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
)

func TestStorageAPIHost_NotSet(t *testing.T) {
	t.Parallel()
	opts := options.New()
	out, err := storageAPIHost(aferofs.NewMemoryFs(), opts)
	assert.NoError(t, err)
	assert.Equal(t, "https://connection.keboola.com", out) // default fallback
}

func TestStorageAPIHost_Empty(t *testing.T) {
	t.Parallel()
	in := ""
	opts := options.New()
	opts.Set("storage-api-host", in)
	out, err := storageAPIHost(aferofs.NewMemoryFs(), opts)
	assert.NoError(t, err)
	assert.Equal(t, "https://connection.keboola.com", out) // default fallback
}

func TestStorageAPIHost_NoProtocol(t *testing.T) {
	t.Parallel()
	in := "connection.keboola.local"
	opts := options.New()
	opts.Set("storage-api-host", in)
	out, err := storageAPIHost(aferofs.NewMemoryFs(), opts)
	assert.NoError(t, err)
	assert.Equal(t, "https://connection.keboola.local", out)
}

func TestStorageAPIHost_HTTP_Protocol(t *testing.T) {
	t.Parallel()
	in := "http://connection.keboola.local"
	opts := options.New()
	opts.Set("storage-api-host", in)
	out, err := storageAPIHost(aferofs.NewMemoryFs(), opts)
	assert.NoError(t, err)
	assert.Equal(t, "http://connection.keboola.local", out)
}

func TestStorageAPIHost_HTTPS_Protocol(t *testing.T) {
	t.Parallel()
	in := "https://connection.keboola.local"
	opts := options.New()
	opts.Set("storage-api-host", in)
	out, err := storageAPIHost(aferofs.NewMemoryFs(), opts)
	assert.NoError(t, err)
	assert.Equal(t, "https://connection.keboola.local", out)
}

func TestStorageAPIHost_Invalid_Protocol(t *testing.T) {
	t.Parallel()
	in := "foo://connection.keboola.local"
	opts := options.New()
	opts.Set("storage-api-host", in)
	out, err := storageAPIHost(aferofs.NewMemoryFs(), opts)
	assert.NoError(t, err)
	assert.Equal(t, "https://foo://connection.keboola.local", out)
}
