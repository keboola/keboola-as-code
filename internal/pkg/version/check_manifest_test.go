package version

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
)

func TestCheckManifestVersion_ValidVersion(t *testing.T) {
	t.Parallel()
	fs := testfs.NewMemoryFs()
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(`foo.json`, `{"version": 2}`)))
	err := CheckManifestVersion(log.NewNopLogger(), fs, `foo.json`)
	assert.NoError(t, err)
}

func TestCheckManifestVersion_InvalidVersion(t *testing.T) {
	t.Parallel()
	fs := testfs.NewMemoryFs()
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(`foo.json`, `{"version": 123}`)))
	err := CheckManifestVersion(log.NewNopLogger(), fs, `foo.json`)
	assert.Error(t, err)
	assert.Equal(t, `unknown version "123" found in "foo.json"`, err.Error())
}

func TestCheckManifestVersion_MissingVersion(t *testing.T) {
	t.Parallel()
	fs := testfs.NewMemoryFs()
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(`foo.json`, `{}`)))
	err := CheckManifestVersion(log.NewNopLogger(), fs, `foo.json`)
	assert.Error(t, err)
	assert.Equal(t, `version field not found in "foo.json"`, err.Error())
}
