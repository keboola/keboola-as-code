package mountfs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/abstract"
	. "github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/mountfs"
)

func TestNewMountFs(t *testing.T) {
	t.Parallel()
	fs := New(aferofs.NewMemoryFs().(abstract.BackendProvider).Backend(), "base/path")
	assert.Equal(t, "base/path", fs.BasePath())
}

func TestMountFs_Rename(t *testing.T) {
	t.Parallel()

	// Create FS
	ctx := context.Background()
	root := aferofs.NewMemoryFs()
	dir1 := aferofs.NewMemoryFs()
	dir2 := aferofs.NewMemoryFs()
	fs, err := aferofs.NewMountFs(
		root,
		[]MountPoint{
			NewMountPoint(filesystem.Join("/sub/dir1"), dir1),
			NewMountPoint(filesystem.Join("/sub/dir1/dir2"), dir2),
		},
	)
	require.NoError(t, err)

	// Create file
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile("/sub/dir1/foo", "abc")))

	// Move file within mount point - no error
	require.NoError(t, fs.Move(ctx, "/sub/dir1/foo", "/sub/dir1/bar"))
	assert.False(t, fs.IsFile(ctx, "/sub/dir1/foo"))
	assert.True(t, fs.IsFile(ctx, "/sub/dir1/bar"))

	// Move file outside mount point - error
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile("/sub/dir1/foo", "abc")))
	err = fs.Move(ctx, "/sub/dir1/foo", "/bar")
	require.Error(t, err)
	assert.Equal(t, `path "/sub/dir1/foo" cannot be moved outside mount dir "/sub/dir1" to "/bar"`, err.Error())
}
