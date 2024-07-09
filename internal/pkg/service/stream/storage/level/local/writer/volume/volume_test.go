package volume_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/gofrs/flock"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	model "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestOpen_NonExistentPath(t *testing.T) {
	t.Parallel()
	tc := test.NewWriterVolumeTestCase(t)
	tc.VolumePath = filepath.Join("non-existent", "path")

	_, err := tc.OpenVolume()
	if assert.Error(t, err) {
		assert.True(t, errors.Is(err, os.ErrNotExist))
	}
}

func TestOpen_Error_DirPermissions(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("permissions work different on Windows")
	}

	tc := test.NewWriterVolumeTestCase(t)

	// Volume directory is readonly
	assert.NoError(t, os.Chmod(tc.VolumePath, 0o440))

	_, err := tc.OpenVolume()
	if assert.Error(t, err) {
		assert.True(t, errors.Is(err, os.ErrPermission))
	}
}

func TestOpen_Error_VolumeFilePermissions(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("permissions work different on Windows")
	}

	tc := test.NewWriterVolumeTestCase(t)

	// Volume ID file is not readable
	path := filepath.Join(tc.VolumePath, model.IDFile)
	assert.NoError(t, os.WriteFile(path, []byte("abc"), 0o640))
	assert.NoError(t, os.Chmod(path, 0o110))

	_, err := tc.OpenVolume()
	if assert.Error(t, err) {
		assert.True(t, errors.Is(err, os.ErrPermission))
	}
}

// TestOpen_GenerateVolumeID tests that the file with the volume ID is generated if not exists.
func TestOpen_GenerateVolumeID(t *testing.T) {
	t.Parallel()
	tc := test.NewWriterVolumeTestCase(t)

	// Open volume - it generates the file
	vol, err := tc.OpenVolume()
	require.NoError(t, err)

	// Read the volume ID file and check content length
	idFilePath := filepath.Join(tc.VolumePath, model.IDFile)
	if assert.FileExists(t, idFilePath) {
		content, err := os.ReadFile(idFilePath)
		assert.NoError(t, err)
		assert.Len(t, content, model.IDLength)

		// Volume ID reported by the volume instance match the file content
		assert.Equal(t, model.ID(content), vol.ID())
	}

	// Lock is locked by the volume
	lock := flock.New(filepath.Join(tc.VolumePath, volume.LockFile))
	locked, err := lock.TryLock()
	assert.False(t, locked)
	assert.NoError(t, err)

	// Lock is release by Close method
	assert.NoError(t, vol.Close(context.Background()))
	assert.NoFileExists(t, lock.Path())
	locked, err = lock.TryLock()
	assert.True(t, locked)
	assert.NoError(t, err)
	assert.NoError(t, lock.Unlock())

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"opening volume","volume.path":"%s"}
{"level":"info","message":"generated volume ID","volume.id":"%s"}
{"level":"info","message":"opened volume","volume.id":"%s","volume.path":"%s","volume.type":"hdd","volume.label":"1"}
{"level":"info","message":"closing volume","volume.id":"%s"}
{"level":"info","message":"closed volume","volume.id":"%s"}
`)
}

// TestOpen_LoadVolumeID tests that the volume ID is loaded from the file if it exists.
func TestOpen_LoadVolumeID(t *testing.T) {
	t.Parallel()
	tc := test.NewWriterVolumeTestCase(t)

	// Write volume ID file
	idFilePath := filepath.Join(tc.VolumePath, model.IDFile)
	writeContent := []byte("  123456789  ")
	require.NoError(t, os.WriteFile(idFilePath, writeContent, 0o0640))

	// Open volume - it loads the file
	vol, err := tc.OpenVolume()
	require.NoError(t, err)

	// Volume ID reported by the volume instance match the file content
	assert.Equal(t, model.ID("123456789"), vol.ID())

	// File content remains same
	if assert.FileExists(t, idFilePath) {
		content, err := os.ReadFile(idFilePath)
		assert.NoError(t, err)
		assert.Equal(t, writeContent, content)
	}

	// Lock is locked by the volume
	lock := flock.New(filepath.Join(tc.VolumePath, volume.LockFile))
	locked, err := lock.TryLock()
	assert.False(t, locked)
	assert.NoError(t, err)

	// Lock is release by Close method
	assert.NoError(t, vol.Close(context.Background()))
	assert.NoFileExists(t, lock.Path())
	locked, err = lock.TryLock()
	assert.True(t, locked)
	assert.NoError(t, err)
	assert.NoError(t, lock.Unlock())

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"opening volume","volume.path":"%s"}
{"level":"info","message":"opened volume","volume.id":"123456789","volume.path":"%s","volume.type":"hdd","volume.label":"1"}
{"level":"info","message":"closing volume","volume.id":"123456789"}
{"level":"info","message":"closed volume","volume.id":"123456789"}
`)
}

// TestOpen_VolumeLock tests that only one volume instance can be active at a time.
func TestOpen_VolumeLock(t *testing.T) {
	t.Parallel()
	tc := test.NewWriterVolumeTestCase(t)

	// Open volume - first instance - ok
	vol, err := tc.OpenVolume()
	assert.NoError(t, err)

	// Open volume - second instance - error
	_, err = tc.OpenVolume()
	if assert.Error(t, err) {
		wildcards.Assert(t, `cannot acquire writer lock "%s": already locked`, err.Error())
	}

	// Close volume
	assert.NoError(t, vol.Close(context.Background()))
}

func TestVolume_Close_Errors(t *testing.T) {
	t.Parallel()

	tc := test.NewWriterVolumeTestCase(t)

	// Open volume, replace file opener
	vol, err := tc.OpenVolume(volume.WithFileOpener(func(filePath string) (volume.File, error) {
		f := test.NewWriterTestFile(t, filePath)
		f.CloseError = errors.New("some close error")
		return f, nil
	}))
	require.NoError(t, err)

	// Open two writers
	_, err = vol.OpenWriter(test.NewSliceOpenedAt("2000-01-01T20:00:00.000Z"))
	require.NoError(t, err)
	_, err = vol.OpenWriter(test.NewSliceOpenedAt("2000-01-01T21:00:00.000Z"))
	require.NoError(t, err)

	// Close volume, expect close errors from the writers
	err = vol.Close(context.Background())
	if assert.Error(t, err) {
		// Order of the errors is random, writers are closed in parallel
		wildcards.Assert(t, strings.TrimSpace(`
- cannot close writer for slice "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T%s":
  - chain close error:
    - cannot close file: some close error
- cannot close writer for slice "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T%s":
  - chain close error:
    - cannot close file: some close error
`), err.Error())
	}
}
