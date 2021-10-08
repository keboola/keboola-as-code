// nolint: forbidigo
package localfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

func TestNewLocalFs(t *testing.T) {
	projectDir := t.TempDir()
	fs := New(projectDir)
	assert.Equal(t, projectDir, fs.BasePath())
}

func TestFindProjectDir(t *testing.T) {
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, filesystem.MetadataDir)
	workingDir := filepath.Join(projectDir, `foo`, `bar`)
	assert.NoError(t, os.MkdirAll(metadataDir, 0o755))
	assert.NoError(t, os.MkdirAll(workingDir, 0o755))

	dir, err := FindProjectDir(zap.NewNop().Sugar(), workingDir)
	assert.NoError(t, err)
	assert.Equal(t, projectDir, dir)
}

func TestFindProjectDirNotFound(t *testing.T) {
	workingDir := t.TempDir()
	dir, err := FindProjectDir(zap.NewNop().Sugar(), workingDir)
	assert.NoError(t, err)
	assert.Equal(t, workingDir, dir)
}
