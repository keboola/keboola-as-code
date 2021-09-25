package localfs

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewLocalFs(t *testing.T) {
	projectDir := t.TempDir()
	fs := New(projectDir)
	assert.Equal(t, projectDir, fs.BasePath())
}

func TestFindProjectDir(t *testing.T) {
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, model.MetadataDir)
	workingDir := filepath.Join(projectDir, `foo`, `bar`)
	assert.NoError(t, os.MkdirAll(metadataDir, 0755))
	assert.NoError(t, os.MkdirAll(workingDir, 0755))

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

