package version

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func TestCheckManifestVersion_ValidVersion(t *testing.T) {
	t.Parallel()
	fs := aferofs.NewMemoryFs()
	require.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(`foo.json`, `{"version": 2}`)))
	err := CheckManifestVersion(context.Background(), log.NewNopLogger(), fs, `foo.json`)
	require.NoError(t, err)
}

func TestCheckManifestVersion_InvalidVersion(t *testing.T) {
	t.Parallel()
	fs := aferofs.NewMemoryFs()
	require.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(`foo.json`, `{"version": 123}`)))
	err := CheckManifestVersion(context.Background(), log.NewNopLogger(), fs, `foo.json`)
	require.Error(t, err)
	assert.Equal(t, `unknown version "123" found in "foo.json"`, err.Error())
}

func TestCheckManifestVersion_MissingVersion(t *testing.T) {
	t.Parallel()
	fs := aferofs.NewMemoryFs()
	require.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(`foo.json`, `{}`)))
	err := CheckManifestVersion(context.Background(), log.NewNopLogger(), fs, `foo.json`)
	require.Error(t, err)
	assert.Equal(t, `version field not found in "foo.json"`, err.Error())
}
