package aferofs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

func TestCopyFs2FsRootToRoot(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	localFs, err := NewLocalFs(zap.NewNop().Sugar(), tempDir, `/`)
	assert.NoError(t, err)
	memoryFs, err := NewMemoryFs(zap.NewNop().Sugar(), `/`)
	assert.NoError(t, err)

	// Create files
	assert.NoError(t, localFs.WriteFile(filesystem.NewFile(`foo.txt`, `content1`)))
	assert.NoError(t, localFs.WriteFile(filesystem.NewFile(filesystem.Join(`my-dir`, `bar.txt`), `content2`)))

	// Copy
	assert.NoError(t, CopyFs2Fs(localFs, ``, memoryFs, ``))

	// Assert
	file1, err := memoryFs.ReadFile(`foo.txt`, ``)
	assert.NoError(t, err)
	assert.Equal(t, `content1`, file1.Content)
	file2, err := memoryFs.ReadFile(filesystem.Join(`my-dir`, `bar.txt`), ``)
	assert.NoError(t, err)
	assert.Equal(t, `content2`, file2.Content)
}

func TestCopyFs2FsDirToDir(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	localFs, err := NewLocalFs(zap.NewNop().Sugar(), tempDir, `/`)
	assert.NoError(t, err)
	memoryFs, err := NewMemoryFs(zap.NewNop().Sugar(), `/`)
	assert.NoError(t, err)

	// Create files
	assert.NoError(t, localFs.WriteFile(filesystem.NewFile(filesystem.Join(`my-dir`, `bar.txt`), `content`)))

	// Copy
	assert.NoError(t, CopyFs2Fs(localFs, `my-dir`, memoryFs, `my-dir-2`))

	// Assert
	file, err := memoryFs.ReadFile(filesystem.Join(`my-dir-2`, `bar.txt`), ``)
	assert.NoError(t, err)
	assert.Equal(t, `content`, file.Content)
}
