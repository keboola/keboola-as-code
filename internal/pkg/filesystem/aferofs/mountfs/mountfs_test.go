package mountfs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/abstract"
	. "github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/mountfs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
)

func TestNewMountFs(t *testing.T) {
	t.Parallel()
	fs := New(testfs.NewMemoryFs().(abstract.BackendProvider).Backend(), "base/path")
	assert.Equal(t, "base/path", fs.BasePath())
}

func TestMountFs_Rename(t *testing.T) {
	t.Parallel()

	// Create FS
	root := testfs.NewMemoryFs()
	dir1 := testfs.NewMemoryFs()
	dir2 := testfs.NewMemoryFs()
	fs, err := aferofs.NewMountFs(
		root,
		NewMountPoint(filesystem.Join("/sub/dir1"), dir1),
		NewMountPoint(filesystem.Join("/sub/dir1/dir2"), dir2),
	)
	assert.NoError(t, err)

	// Create file
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile("/sub/dir1/foo", "abc")))

	// Move file within mount point - no error
	assert.NoError(t, fs.Move("/sub/dir1/foo", "/sub/dir1/bar"))
	assert.False(t, fs.IsFile("/sub/dir1/foo"))
	assert.True(t, fs.IsFile("/sub/dir1/bar"))

	// Move file outside mount point - error
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile("/sub/dir1/foo", "abc")))
	err = fs.Move("/sub/dir1/foo", "/bar")
	assert.Error(t, err)
	assert.Equal(t, `path "/sub/dir1/foo" cannot be moved outside mount dir "/sub/dir1" to "/bar"`, err.Error())
}
