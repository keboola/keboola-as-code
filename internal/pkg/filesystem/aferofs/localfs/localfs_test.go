// nolint: forbidigo
package localfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func TestNewLocalFs(t *testing.T) {
	t.Parallel()
	projectDir := t.TempDir()
	fs, err := New(projectDir)
	assert.NoError(t, err)
	assert.Equal(t, projectDir, fs.BasePath())
}

func TestFindProjectDir(t *testing.T) {
	t.Parallel()
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, filesystem.MetadataDir)
	workingDir := filepath.Join(projectDir, `foo`, `bar`)
	assert.NoError(t, os.MkdirAll(metadataDir, 0o755))
	assert.NoError(t, os.MkdirAll(workingDir, 0o755))

	dir, err := FindKeboolaDir(log.NewNopLogger(), workingDir)
	assert.NoError(t, err)
	assert.Equal(t, projectDir, dir)
}

func TestFindProjectDirNotFound(t *testing.T) {
	t.Parallel()
	workingDir := t.TempDir()
	dir, err := FindKeboolaDir(log.NewNopLogger(), workingDir)
	assert.NoError(t, err)
	assert.Equal(t, workingDir, dir)
}
