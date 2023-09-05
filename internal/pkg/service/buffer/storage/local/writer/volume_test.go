package writer

import (
	"context"
	"fmt"
	"github.com/benbjohnson/clock"
	"github.com/gofrs/flock"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/base"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOpenVolume_NonExistentPath(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)
	tc.VolumePath = filesystem.Join("non-existent", "path")

	_, err := tc.OpenVolume()
	if assert.Error(t, err) {
		assert.True(t, errors.Is(err, os.ErrNotExist))
	}
}

func TestOpenVolume_FileNotDir(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)
	tc.VolumePath = filesystem.Join(t.TempDir(), "file")

	// Create file
	assert.NoError(t, os.WriteFile(tc.VolumePath, []byte("foo"), 0o640))

	_, err := tc.OpenVolume()
	if assert.Error(t, err) {
		assert.Equal(t, fmt.Sprintf(`cannot open volume "%s": the path is not directory`, tc.VolumePath), err.Error())
	}
}

func TestOpenVolume_Error_DirPermissions(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	// Volume directory is readonly
	assert.NoError(t, os.Chmod(tc.VolumePath, 0o440))

	_, err := tc.OpenVolume()
	if assert.Error(t, err) {
		assert.True(t, errors.Is(err, os.ErrPermission))
	}
}

func TestOpenVolume_Error_VolumeFilePermissions(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	// Volume ID file is not readable
	path := filesystem.Join(tc.VolumePath, local.VolumeIDFile)
	assert.NoError(t, os.WriteFile(path, []byte("abc"), 0o640))
	assert.NoError(t, os.Chmod(path, 0o110))

	_, err := tc.OpenVolume()
	if assert.Error(t, err) {
		assert.True(t, errors.Is(err, os.ErrPermission))
	}
}

// TestOpenVolume_DrainFile tests that the volume can be blocked for writing by a drain file.
func TestOpenVolume_DrainFile(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	// Create an empty drain file
	assert.NoError(t, os.WriteFile(filepath.Join(tc.VolumePath, drainFile), nil, 0o640))

	// Type open volume
	_, err := tc.OpenVolume()
	if assert.Error(t, err) {
		assert.Equal(t, `cannot open volume for writing: found "drain" file`, err.Error())
	}
}

// TestOpenVolume_GenerateVolumeID tests that the file with the volume ID is generated if not exists.
func TestOpenVolume_GenerateVolumeID(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	// Open volume - it generates the file
	volume, err := tc.OpenVolume()
	require.NoError(t, err)

	// Read the volume ID file and check content length
	idFilePath := filepath.Join(tc.VolumePath, local.VolumeIDFile)
	if assert.FileExists(t, idFilePath) {
		content, err := os.ReadFile(idFilePath)
		assert.NoError(t, err)
		assert.Len(t, content, storage.VolumeIDLength)

		// Volume ID reported by the volume instance match the file content
		assert.Equal(t, storage.VolumeID(content), volume.VolumeID())
	}

	// Lock is locked by the volume
	lock := flock.New(filepath.Join(tc.VolumePath, lockFile))
	locked, err := lock.TryLock()
	assert.False(t, locked)
	assert.NoError(t, err)

	// Lock is release by Close method
	assert.NoError(t, volume.Close())
	assert.NoFileExists(t, lock.Path())
	locked, err = lock.TryLock()
	assert.True(t, locked)
	assert.NoError(t, err)
	assert.NoError(t, lock.Unlock())

	// Check logs
	tc.AssertLogs(`
INFO  opening volume "%s"
INFO  generated volume ID "%s"
INFO  opened volume
INFO  closing volume
INFO  closed volume
`)
}

// TestOpenVolume_LoadVolumeID tests that the volume ID is loaded from the file if it exists.
func TestOpenVolume_LoadVolumeID(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	// Write volume ID file
	idFilePath := filepath.Join(tc.VolumePath, local.VolumeIDFile)
	writeContent := []byte("  123456789  ")
	require.NoError(t, os.WriteFile(idFilePath, writeContent, 0o0640))

	// Open volume - it loads the file
	volume, err := tc.OpenVolume()
	require.NoError(t, err)

	// Volume ID reported by the volume instance match the file content
	assert.Equal(t, storage.VolumeID("123456789"), volume.VolumeID())

	// File content remains same
	if assert.FileExists(t, idFilePath) {
		content, err := os.ReadFile(idFilePath)
		assert.NoError(t, err)
		assert.Equal(t, writeContent, content)
	}

	// Lock is locked by the volume
	lock := flock.New(filepath.Join(tc.VolumePath, lockFile))
	locked, err := lock.TryLock()
	assert.False(t, locked)
	assert.NoError(t, err)

	// Lock is release by Close method
	assert.NoError(t, volume.Close())
	assert.NoFileExists(t, lock.Path())
	locked, err = lock.TryLock()
	assert.True(t, locked)
	assert.NoError(t, err)
	assert.NoError(t, lock.Unlock())

	// Check logs
	tc.AssertLogs(`
INFO  opening volume "%s"
INFO  opened volume
INFO  closing volume
INFO  closed volume
`)
}

// TestOpenVolume_VolumeLock tests that only one volume instance can be active at a time.
func TestOpenVolume_VolumeLock(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	// Open volume - first instance - ok
	_, err := tc.OpenVolume()
	assert.NoError(t, err)

	// Open volume - second instance - error
	_, err = tc.OpenVolume()
	if assert.Error(t, err) {
		wildcards.Assert(t, `cannot acquire writer lock "%s": already locked`, err.Error())
	}
}

func TestVolume_Close_Errors(t *testing.T) {
	t.Parallel()

	tc := newVolumeTestCase(t)

	// Open volume, replace file opener
	volume, err := tc.OpenVolume(WithFileOpener(func(filePath string) (File, error) {
		f := newTestFile(t, filePath)
		f.CloseError = errors.New("some close error")
		return f, nil
	}))
	require.NoError(t, err)

	// Open two writers
	_, err = volume.NewWriterFor(newTestSliceOpenedAt("2000-01-01T20:00:00.000Z"))
	require.NoError(t, err)
	_, err = volume.NewWriterFor(newTestSliceOpenedAt("2000-01-01T21:00:00.000Z"))
	require.NoError(t, err)

	// Close volume, expect close errors from the writers
	err = volume.Close()
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
- cannot close writer for slice "123/my-receiver/my-export/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T20:00:00.000Z": chain close error: cannot close file: some close error
- cannot close writer for slice "123/my-receiver/my-export/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T21:00:00.000Z": chain close error: cannot close file: some close error
`), err.Error())
	}
}

type volumeTestCase struct {
	T          testing.TB
	Ctx        context.Context
	Logger     log.DebugLogger
	Clock      *clock.Mock
	Allocator  *testAllocator
	VolumePath string
}

func newVolumeTestCase(t testing.TB) *volumeTestCase {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(func() {
		cancel()
	})

	logger := log.NewDebugLogger()
	tmpDir := t.TempDir()

	return &volumeTestCase{
		T:          t,
		Ctx:        ctx,
		Logger:     logger,
		Clock:      clock.NewMock(),
		Allocator:  &testAllocator{},
		VolumePath: tmpDir,
	}
}

func (tc *volumeTestCase) OpenVolume(opts ...Option) (*Volume, error) {
	opts = append([]Option{
		WithAllocator(tc.Allocator),
		WithWriterFactory(func(w *base.Writer) (SliceWriter, error) {
			return test.NewSliceWriter(w), nil
		}),
	}, opts...)

	return OpenVolume(tc.Ctx, tc.Logger, tc.Clock, tc.VolumePath, opts...)
}

func (tc *volumeTestCase) AssertLogs(expected string) bool {
	return wildcards.Assert(tc.T, strings.TrimSpace(expected), strings.TrimSpace(tc.Logger.AllMessages()))
}
