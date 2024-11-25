package aferofs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

func TestCopyFs2FsRootToRoot_LocalToMemory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tempDir := t.TempDir()
	localFs, err := NewLocalFs(tempDir)
	require.NoError(t, err)
	memoryFs := NewMemoryFs()

	// Create files
	require.NoError(t, localFs.WriteFile(ctx, filesystem.NewRawFile("foo.txt", "content1")))
	require.NoError(t, localFs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join("my-dir", "bar.txt"), "content2")))

	// Copy
	require.NoError(t, CopyFs2Fs(localFs, "", memoryFs, ""))

	// Assert
	file1, err := memoryFs.ReadFile(ctx, filesystem.NewFileDef("foo.txt"))
	require.NoError(t, err)
	assert.Equal(t, "content1", file1.Content)
	file2, err := memoryFs.ReadFile(ctx, filesystem.NewFileDef(filesystem.Join("my-dir", "bar.txt")))
	require.NoError(t, err)
	assert.Equal(t, "content2", file2.Content)
}

func TestCopyFs2FsRootToRoot_LocalToLocal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tempDir1 := t.TempDir()
	localFs1, err := NewLocalFs(tempDir1)
	require.NoError(t, err)
	tempDir2 := t.TempDir()
	localFs2, err := NewLocalFs(tempDir2)
	require.NoError(t, err)

	// Create files
	require.NoError(t, localFs1.WriteFile(ctx, filesystem.NewRawFile("foo.txt", "content1")))
	require.NoError(t, localFs1.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join("my-dir", "bar.txt"), "content2")))

	// Copy
	require.NoError(t, CopyFs2Fs(localFs1, "", localFs2, ""))

	// Assert
	file1, err := localFs2.ReadFile(ctx, filesystem.NewFileDef("foo.txt"))
	require.NoError(t, err)
	assert.Equal(t, "content1", file1.Content)
	file2, err := localFs2.ReadFile(ctx, filesystem.NewFileDef(filesystem.Join("my-dir", "bar.txt")))
	require.NoError(t, err)
	assert.Equal(t, "content2", file2.Content)
}

func TestCopyFs2FsRootToRoot_BaseToBase(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tempDir1 := t.TempDir()
	localFs1, err := NewLocalFs(tempDir1)
	require.NoError(t, err)
	subDir1 := filesystem.Join("sub", "dir", "1")
	require.NoError(t, localFs1.Mkdir(ctx, subDir1))
	base1, err := localFs1.SubDirFs(subDir1)
	require.NoError(t, err)

	tempDir2 := t.TempDir()
	localFs2, err := NewLocalFs(tempDir2)
	require.NoError(t, err)
	subDir2 := filesystem.Join("sub", "dir", "2")
	require.NoError(t, localFs2.Mkdir(ctx, subDir2))
	base2, err := localFs2.SubDirFs(subDir2)
	require.NoError(t, err)

	// Create files
	require.NoError(t, base1.WriteFile(ctx, filesystem.NewRawFile("foo.txt", "content1")))
	require.NoError(t, base1.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join("my-dir", "bar.txt"), "content2")))

	// Copy
	require.NoError(t, CopyFs2Fs(base1, "", base2, ""))

	// Assert
	file1, err := localFs2.ReadFile(ctx, filesystem.NewFileDef(filesystem.Join(subDir2, "foo.txt")))
	require.NoError(t, err)
	assert.Equal(t, "content1", file1.Content)
	file2, err := localFs2.ReadFile(ctx, filesystem.NewFileDef(filesystem.Join(subDir2, "my-dir", "bar.txt")))
	require.NoError(t, err)
	assert.Equal(t, "content2", file2.Content)
}

func TestCopyFs2FsRootToRoot_MemoryToLocal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	memoryFs := NewMemoryFs()
	tempDir := t.TempDir()
	localFs, err := NewLocalFs(tempDir)
	require.NoError(t, err)

	// Create files
	require.NoError(t, memoryFs.WriteFile(ctx, filesystem.NewRawFile("foo.txt", "content1")))
	require.NoError(t, memoryFs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join("my-dir", "bar.txt"), "content2")))

	// Copy
	require.NoError(t, CopyFs2Fs(memoryFs, "/", localFs, "/"))

	// Assert
	file1, err := localFs.ReadFile(ctx, filesystem.NewFileDef("foo.txt"))
	require.NoError(t, err)
	assert.Equal(t, "content1", file1.Content)
	file2, err := localFs.ReadFile(ctx, filesystem.NewFileDef(filesystem.Join("my-dir", "bar.txt")))
	require.NoError(t, err)
	assert.Equal(t, "content2", file2.Content)
}

func TestCopyFs2FsRootToRoot_MemoryToMemory(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	memoryFs1 := NewMemoryFs()
	memoryFs2 := NewMemoryFs()

	// Create files
	require.NoError(t, memoryFs1.WriteFile(ctx, filesystem.NewRawFile("foo.txt", "content1")))
	require.NoError(t, memoryFs1.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join("my-dir", "bar.txt"), "content2")))

	// Copy
	require.NoError(t, CopyFs2Fs(memoryFs1, "/", memoryFs2, "/"))

	// Assert
	file1, err := memoryFs2.ReadFile(ctx, filesystem.NewFileDef("foo.txt"))
	require.NoError(t, err)
	assert.Equal(t, "content1", file1.Content)
	file2, err := memoryFs2.ReadFile(ctx, filesystem.NewFileDef(filesystem.Join("my-dir", "bar.txt")))
	require.NoError(t, err)
	assert.Equal(t, "content2", file2.Content)
}

func TestCopyFs2FsDirToDir(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tempDir := t.TempDir()
	localFs, err := NewLocalFs(tempDir)
	require.NoError(t, err)
	memoryFs := NewMemoryFs()

	// Create files
	require.NoError(t, localFs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join("my-dir", "bar.txt"), "content")))

	// Copy
	require.NoError(t, CopyFs2Fs(localFs, "my-dir", memoryFs, "my-dir-2"))

	// Assert
	file, err := memoryFs.ReadFile(ctx, filesystem.NewFileDef(filesystem.Join("my-dir-2", "bar.txt")))
	require.NoError(t, err)
	assert.Equal(t, "content", file.Content)
}
