package dependencies

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/nop"
)

func TestStorageAPIHost_NotSet(t *testing.T) {
	t.Parallel()
	o := options.New()
	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), options: o, dialogs: dialog.New(nopPrompt.New())}
	_, err := storageAPIHost(context.Background(), baseScp, "")
	if assert.Error(t, err) {
		assert.Equal(t, ErrMissingStorageAPIHost, err)
	}
}

func TestStorageAPIHost_NotSet_Fallback(t *testing.T) {
	t.Parallel()
	o := options.New()
	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), options: o, dialogs: dialog.New(nopPrompt.New())}
	out, err := storageAPIHost(context.Background(), baseScp, "https://connection.keboola.com")
	assert.NoError(t, err)
	assert.Equal(t, "https://connection.keboola.com", out) // fallback
}

func TestStorageAPIHost_Empty(t *testing.T) {
	t.Parallel()
	o := options.New()
	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), options: o, dialogs: dialog.New(nopPrompt.New())}
	in := ""
	o.Set("storage-api-host", in)
	_, err := storageAPIHost(context.Background(), baseScp, "")
	if assert.Error(t, err) {
		assert.Equal(t, ErrMissingStorageAPIHost, err)
	}
}

func TestStorageAPIHost_Empty_Fallback(t *testing.T) {
	t.Parallel()
	o := options.New()
	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), options: o, dialogs: dialog.New(nopPrompt.New())}
	in := ""
	o.Set("storage-api-host", in)
	out, err := storageAPIHost(context.Background(), baseScp, "https://connection.keboola.com")
	assert.NoError(t, err)
	assert.Equal(t, "https://connection.keboola.com", out) // fallback
}

func TestStorageAPIHost_NoProtocol(t *testing.T) {
	t.Parallel()
	o := options.New()
	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), options: o, dialogs: dialog.New(nopPrompt.New())}
	in := "connection.keboola.local"
	o.Set("storage-api-host", in)
	out, err := storageAPIHost(context.Background(), baseScp, "")
	assert.NoError(t, err)
	assert.Equal(t, "https://connection.keboola.local", out)
}

func TestStorageAPIHost_HTTP_Protocol(t *testing.T) {
	t.Parallel()
	o := options.New()
	d := &baseScope{fs: aferofs.NewMemoryFs(), options: o, dialogs: dialog.New(nopPrompt.New())}
	in := "http://connection.keboola.local"
	o.Set("storage-api-host", in)
	out, err := storageAPIHost(context.Background(), d, "")
	assert.NoError(t, err)
	assert.Equal(t, "http://connection.keboola.local", out)
}

func TestStorageAPIHost_HTTPS_Protocol(t *testing.T) {
	t.Parallel()
	o := options.New()
	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), options: o, dialogs: dialog.New(nopPrompt.New())}
	in := "https://connection.keboola.local"
	o.Set("storage-api-host", in)
	out, err := storageAPIHost(context.Background(), baseScp, "")
	assert.NoError(t, err)
	assert.Equal(t, "https://connection.keboola.local", out)
}

func TestStorageAPIHost_Invalid_Protocol(t *testing.T) {
	t.Parallel()
	o := options.New()
	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), options: o, dialogs: dialog.New(nopPrompt.New())}
	in := "foo://connection.keboola.local"
	o.Set("storage-api-host", in)
	out, err := storageAPIHost(context.Background(), baseScp, "")
	assert.NoError(t, err)
	assert.Equal(t, "https://foo://connection.keboola.local", out)
}
