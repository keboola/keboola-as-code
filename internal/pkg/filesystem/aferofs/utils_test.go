package aferofs

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func TestCopyFs2FsRootToRoot_LocalToMemory(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	localFs, err := NewLocalFs(log.NewNopLogger(), tempDir, "/")
	assert.NoError(t, err)
	memoryFs, err := NewMemoryFs(log.NewNopLogger(), "/")
	assert.NoError(t, err)

	// Create files
	assert.NoError(t, localFs.WriteFile(filesystem.NewRawFile("foo.txt", "content1")))
	assert.NoError(t, localFs.WriteFile(filesystem.NewRawFile(filesystem.Join("my-dir", "bar.txt"), "content2")))

	// Copy
	assert.NoError(t, CopyFs2Fs(localFs, "", memoryFs, ""))

	// Assert
	file1, err := memoryFs.ReadFile(filesystem.NewFileDef("foo.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "content1", file1.Content)
	file2, err := memoryFs.ReadFile(filesystem.NewFileDef(filesystem.Join("my-dir", "bar.txt")))
	assert.NoError(t, err)
	assert.Equal(t, "content2", file2.Content)
}

func TestCopyFs2FsRootToRoot_LocalToLocal(t *testing.T) {
	t.Parallel()
	tempDir1 := t.TempDir()
	localFs1, err := NewLocalFs(log.NewNopLogger(), tempDir1, "/")
	assert.NoError(t, err)
	tempDir2 := t.TempDir()
	localFs2, err := NewLocalFs(log.NewNopLogger(), tempDir2, "/")
	assert.NoError(t, err)

	// Create files
	assert.NoError(t, localFs1.WriteFile(filesystem.NewRawFile("foo.txt", "content1")))
	assert.NoError(t, localFs1.WriteFile(filesystem.NewRawFile(filesystem.Join("my-dir", "bar.txt"), "content2")))

	// Copy
	assert.NoError(t, CopyFs2Fs(localFs1, "", localFs2, ""))

	// Assert
	file1, err := localFs2.ReadFile(filesystem.NewFileDef("foo.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "content1", file1.Content)
	file2, err := localFs2.ReadFile(filesystem.NewFileDef(filesystem.Join("my-dir", "bar.txt")))
	assert.NoError(t, err)
	assert.Equal(t, "content2", file2.Content)
}

func TestCopyFs2FsRootToRoot_BaseToBase(t *testing.T) {
	t.Parallel()
	tempDir1 := t.TempDir()
	localFs1, err := NewLocalFs(log.NewNopLogger(), tempDir1, "/")
	assert.NoError(t, err)
	subDir1 := filesystem.Join("sub", "dir", "1")
	assert.NoError(t, localFs1.Mkdir(subDir1))
	base1, err := localFs1.SubDirFs(subDir1)
	assert.NoError(t, err)

	tempDir2 := t.TempDir()
	localFs2, err := NewLocalFs(log.NewNopLogger(), tempDir2, "/")
	assert.NoError(t, err)
	subDir2 := filesystem.Join("sub", "dir", "2")
	assert.NoError(t, localFs2.Mkdir(subDir2))
	base2, err := localFs2.SubDirFs(subDir2)
	assert.NoError(t, err)

	// Create files
	assert.NoError(t, base1.WriteFile(filesystem.NewRawFile("foo.txt", "content1")))
	assert.NoError(t, base1.WriteFile(filesystem.NewRawFile(filesystem.Join("my-dir", "bar.txt"), "content2")))

	// Copy
	assert.NoError(t, CopyFs2Fs(base1, "", base2, ""))

	// Assert
	file1, err := localFs2.ReadFile(filesystem.NewFileDef(filesystem.Join(subDir2, "foo.txt")))
	assert.NoError(t, err)
	assert.Equal(t, "content1", file1.Content)
	file2, err := localFs2.ReadFile(filesystem.NewFileDef(filesystem.Join(subDir2, "my-dir", "bar.txt")))
	assert.NoError(t, err)
	assert.Equal(t, "content2", file2.Content)
}

func TestCopyFs2FsRootToRoot_MemoryToLocal(t *testing.T) {
	t.Parallel()
	memoryFs, err := NewMemoryFs(log.NewNopLogger(), "")
	assert.NoError(t, err)
	tempDir := t.TempDir()
	localFs, err := NewLocalFs(log.NewNopLogger(), tempDir, "")
	assert.NoError(t, err)

	// Create files
	assert.NoError(t, memoryFs.WriteFile(filesystem.NewRawFile("foo.txt", "content1")))
	assert.NoError(t, memoryFs.WriteFile(filesystem.NewRawFile(filesystem.Join("my-dir", "bar.txt"), "content2")))

	// Copy
	assert.NoError(t, CopyFs2Fs(memoryFs, "/", localFs, "/"))

	// Assert
	file1, err := localFs.ReadFile(filesystem.NewFileDef("foo.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "content1", file1.Content)
	file2, err := localFs.ReadFile(filesystem.NewFileDef(filesystem.Join("my-dir", "bar.txt")))
	assert.NoError(t, err)
	assert.Equal(t, "content2", file2.Content)
}

func TestCopyFs2FsRootToRoot_MemoryToMemory(t *testing.T) {
	t.Parallel()
	memoryFs1, err := NewMemoryFs(log.NewNopLogger(), "")
	assert.NoError(t, err)
	memoryFs2, err := NewMemoryFs(log.NewNopLogger(), "")
	assert.NoError(t, err)

	// Create files
	assert.NoError(t, memoryFs1.WriteFile(filesystem.NewRawFile("foo.txt", "content1")))
	assert.NoError(t, memoryFs1.WriteFile(filesystem.NewRawFile(filesystem.Join("my-dir", "bar.txt"), "content2")))

	// Copy
	assert.NoError(t, CopyFs2Fs(memoryFs1, "/", memoryFs2, "/"))

	// Assert
	file1, err := memoryFs2.ReadFile(filesystem.NewFileDef("foo.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "content1", file1.Content)
	file2, err := memoryFs2.ReadFile(filesystem.NewFileDef(filesystem.Join("my-dir", "bar.txt")))
	assert.NoError(t, err)
	assert.Equal(t, "content2", file2.Content)
}

func TestCopyFs2FsDirToDir(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	localFs, err := NewLocalFs(log.NewNopLogger(), tempDir, "/")
	assert.NoError(t, err)
	memoryFs, err := NewMemoryFs(log.NewNopLogger(), "/")
	assert.NoError(t, err)

	// Create files
	assert.NoError(t, localFs.WriteFile(filesystem.NewRawFile(filesystem.Join("my-dir", "bar.txt"), "content")))

	// Copy
	assert.NoError(t, CopyFs2Fs(localFs, "my-dir", memoryFs, "my-dir-2"))

	// Assert
	file, err := memoryFs.ReadFile(filesystem.NewFileDef(filesystem.Join("my-dir-2", "bar.txt")))
	assert.NoError(t, err)
	assert.Equal(t, "content", file.Content)
}
