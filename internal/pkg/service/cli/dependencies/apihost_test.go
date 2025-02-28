package dependencies

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/flag"
	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

func TestStorageAPIHost_NotSet(t *testing.T) {
	t.Parallel()

	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), dialogs: dialog.New(nopPrompt.New())}
	_, err := storageAPIHost(t.Context(), baseScp, "", configmap.NewValueWithOrigin("", configmap.SetByDefault))
	if assert.Error(t, err) {
		assert.Equal(t, ErrMissingStorageAPIHost, err)
	}
}

func TestStorageAPIHost_NotSet_Fallback(t *testing.T) {
	t.Parallel()

	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), dialogs: dialog.New(nopPrompt.New())}
	out, err := storageAPIHost(t.Context(), baseScp, "https://connection.keboola.com", configmap.NewValueWithOrigin("", configmap.SetByFlag))
	require.NoError(t, err)
	assert.Equal(t, "https://connection.keboola.com", out) // fallback
}

func TestStorageAPIHost_Empty(t *testing.T) {
	t.Parallel()

	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), dialogs: dialog.New(nopPrompt.New())}

	_, err := storageAPIHost(t.Context(), baseScp, "", configmap.NewValueWithOrigin("", configmap.SetByDefault))
	if assert.Error(t, err) {
		assert.Equal(t, ErrMissingStorageAPIHost, err)
	}
}

func TestStorageAPIHost_Empty_Fallback(t *testing.T) {
	t.Parallel()

	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), globalsFlags: flag.DefaultGlobalFlags(), dialogs: dialog.New(nopPrompt.New())}

	out, err := storageAPIHost(t.Context(), baseScp, "https://connection.keboola.com", configmap.NewValueWithOrigin("", configmap.SetByFlag))
	require.NoError(t, err)
	assert.Equal(t, "https://connection.keboola.com", out) // fallback
}

func TestStorageAPIHost_NoProtocol(t *testing.T) {
	t.Parallel()

	f := flag.DefaultGlobalFlags()
	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), globalsFlags: f, dialogs: dialog.New(nopPrompt.New())}

	out, err := storageAPIHost(t.Context(), baseScp, "", configmap.NewValueWithOrigin("connection.keboola.local", configmap.SetByFlag))
	require.NoError(t, err)
	assert.Equal(t, "https://connection.keboola.local", out)
}

func TestStorageAPIHost_HTTP_Protocol(t *testing.T) {
	t.Parallel()
	f := flag.DefaultGlobalFlags()

	d := &baseScope{fs: aferofs.NewMemoryFs(), globalsFlags: f, dialogs: dialog.New(nopPrompt.New())}

	out, err := storageAPIHost(t.Context(), d, "", configmap.NewValueWithOrigin("http://connection.keboola.local", configmap.SetByFlag))
	require.NoError(t, err)
	assert.Equal(t, "http://connection.keboola.local", out)
}

func TestStorageAPIHost_HTTPS_Protocol(t *testing.T) {
	t.Parallel()

	f := flag.DefaultGlobalFlags()

	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), globalsFlags: f, dialogs: dialog.New(nopPrompt.New())}

	out, err := storageAPIHost(t.Context(), baseScp, "", configmap.NewValueWithOrigin("https://connection.keboola.local", configmap.SetByFlag))
	require.NoError(t, err)
	assert.Equal(t, "https://connection.keboola.local", out)
}

func TestStorageAPIHost_Invalid_Protocol(t *testing.T) {
	t.Parallel()

	f := flag.DefaultGlobalFlags()

	baseScp := &baseScope{fs: aferofs.NewMemoryFs(), globalsFlags: f, dialogs: dialog.New(nopPrompt.New())}

	out, err := storageAPIHost(t.Context(), baseScp, "", configmap.NewValueWithOrigin("foo://connection.keboola.local", configmap.SetByFlag))
	require.NoError(t, err)
	assert.Equal(t, "https://foo://connection.keboola.local", out)
}
