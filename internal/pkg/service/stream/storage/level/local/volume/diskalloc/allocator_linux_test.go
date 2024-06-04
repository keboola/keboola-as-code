//go:build linux

package diskalloc

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllocate(t *testing.T) {
	t.Parallel()

	expectedSize := 10 * datasize.KB

	// Create empty file
	path := filepath.Join(t.TempDir(), "file")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	require.NoError(t, err)

	// Write some data
	n, err := file.WriteString("1234567890")
	assert.Equal(t, 10, n)
	require.NoError(t, err)

	// Check file size before allocation
	allocated, err := Allocated(path)
	require.NoError(t, err)
	assert.Less(t, allocated, expectedSize)

	// Allocate disk space
	allocator := Default()
	ok, err := allocator.Allocate(file, expectedSize)
	assert.True(t, ok)
	require.NoError(t, err)

	// Check file size after allocation
	// The size is rounded to whole blocks, so we check:
	// EXPECTED <= ACTUAL SIZE < 2*EXPECTED
	allocated, err = Allocated(path)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, allocated, expectedSize)
	assert.Less(t, allocated, 2*expectedSize)

	// Check file content
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "1234567890", string(content))
}

func TestAllocate_Error(t *testing.T) {
	t.Parallel()

	allocator := Default()
	ok, err := allocator.Allocate(os.Stdout, 10*datasize.KB)
	assert.False(t, ok)
	require.Error(t, err)
}

func TestAllocated_Error(t *testing.T) {
	t.Parallel()
	_, err := Allocated("/missing/file")
	require.Error(t, err)
}
