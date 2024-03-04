package dependencies

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/flag"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

func TestStorageAPIHost_NotSet(t *testing.T) {
	t.Parallel()
	o := options.New()
	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), options: o, dialogs: dialog.New(nopPrompt.New(), o)}
	_, err := storageAPIHost(context.Background(), baseScp, "", configmap.NewValueWithOrigin("", configmap.SetByDefault))
	if assert.Error(t, err) {
		assert.Equal(t, ErrMissingStorageAPIHost, err)
	}
}

func TestStorageAPIHost_NotSet_Fallback(t *testing.T) {
	t.Parallel()
	o := options.New()
	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), options: o, dialogs: dialog.New(nopPrompt.New(), o)}
	out, err := storageAPIHost(context.Background(), baseScp, "https://connection.keboola.com", configmap.NewValueWithOrigin("", configmap.SetByFlag))
	assert.NoError(t, err)
	assert.Equal(t, "https://connection.keboola.com", out) // fallback
}

func TestStorageAPIHost_Empty(t *testing.T) {
	t.Parallel()
	o := options.New()
	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), options: o, dialogs: dialog.New(nopPrompt.New(), o)}
	in := ""
	o.Set("storage-api-host", in)
	_, err := storageAPIHost(context.Background(), baseScp, "", configmap.NewValueWithOrigin("", configmap.SetByDefault))
	if assert.Error(t, err) {
		assert.Equal(t, ErrMissingStorageAPIHost, err)
	}
}

func TestStorageAPIHost_Empty_Fallback(t *testing.T) {
	t.Parallel()
	o := options.New()
	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), options: o, globalsFlags: flag.DefaultGlobalFlags(), dialogs: dialog.New(nopPrompt.New(), o)}

	out, err := storageAPIHost(context.Background(), baseScp, "https://connection.keboola.com", configmap.NewValueWithOrigin("", configmap.SetByFlag))
	assert.NoError(t, err)
	assert.Equal(t, "https://connection.keboola.com", out) // fallback
}

func TestStorageAPIHost_NoProtocol(t *testing.T) {
	t.Parallel()
	o := options.New()

	f := flag.DefaultGlobalFlags()
	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), options: o, globalsFlags: f, dialogs: dialog.New(nopPrompt.New(), o)}

	out, err := storageAPIHost(context.Background(), baseScp, "", configmap.NewValueWithOrigin("connection.keboola.local", configmap.SetByFlag))
	assert.NoError(t, err)
	assert.Equal(t, "https://connection.keboola.local", out)
}

func TestStorageAPIHost_HTTP_Protocol(t *testing.T) {
	t.Parallel()
	f := flag.DefaultGlobalFlags()

	o := options.New()
	d := &baseScope{fs: aferofs.NewMemoryFs(), options: o, globalsFlags: f, dialogs: dialog.New(nopPrompt.New(), o)}

	out, err := storageAPIHost(context.Background(), d, "", configmap.NewValueWithOrigin("http://connection.keboola.local", configmap.SetByFlag))
	assert.NoError(t, err)
	assert.Equal(t, "http://connection.keboola.local", out)
}

func TestStorageAPIHost_HTTPS_Protocol(t *testing.T) {
	t.Parallel()
	o := options.New()
	f := flag.DefaultGlobalFlags()

	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), options: o, globalsFlags: f, dialogs: dialog.New(nopPrompt.New(), o)}

	out, err := storageAPIHost(context.Background(), baseScp, "", configmap.NewValueWithOrigin("https://connection.keboola.local", configmap.SetByFlag))
	assert.NoError(t, err)
	assert.Equal(t, "https://connection.keboola.local", out)
}

func TestStorageAPIHost_Invalid_Protocol(t *testing.T) {
	t.Parallel()
	o := options.New()

	f := flag.DefaultGlobalFlags()

	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), options: o, globalsFlags: f, dialogs: dialog.New(nopPrompt.New(), o)}

	out, err := storageAPIHost(context.Background(), baseScp, "", configmap.NewValueWithOrigin("foo://connection.keboola.local", configmap.SetByFlag))
	assert.NoError(t, err)
	assert.Equal(t, "https://foo://connection.keboola.local", out)
}
