package diskwriter_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/gofrs/flock"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	volumeModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestSpace(t *testing.T) {
	t.Parallel()

	tc := newVolumeTestCase(t)
	vol, err := tc.OpenVolume()
	require.NoError(t, err)

	used, err := vol.UsedSpace()
	require.NoError(t, err)
	assert.NotEqual(t, 0, used)

	total, err := vol.TotalSpace()
	require.NoError(t, err)
	assert.NotEqual(t, 0, total)

	assert.Less(t, used, total)

	// Lock is release by Close method
	assert.NoError(t, vol.Close(context.Background()))
}

func TestOpen_NonExistentPath(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)
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

	tc := newVolumeTestCase(t)

	// Volume directory is readonly
	require.NoError(t, os.Chmod(tc.VolumePath, 0o440))

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

	tc := newVolumeTestCase(t)

	// Volume ID file is not readable
	path := filepath.Join(tc.VolumePath, volumeModel.IDFile)
	require.NoError(t, os.WriteFile(path, []byte("abc"), 0o640))
	require.NoError(t, os.Chmod(path, 0o110))

	_, err := tc.OpenVolume()
	if assert.Error(t, err) {
		assert.True(t, errors.Is(err, os.ErrPermission))
	}
}

// TestOpen_GenerateVolumeID tests that the file with the volume ID is generated if not exists.
func TestOpen_GenerateVolumeID(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	// Open volume - it generates the file
	vol, err := tc.OpenVolume()
	require.NoError(t, err)

	// Read the volume ID file and check content length
	idFilePath := filepath.Join(tc.VolumePath, volumeModel.IDFile)
	if assert.FileExists(t, idFilePath) {
		content, err := os.ReadFile(idFilePath)
		require.NoError(t, err)
		assert.Len(t, content, volumeModel.IDLength)

		// Volume ID reported by the volume instance match the file content
		assert.Equal(t, volumeModel.ID(content), vol.ID())
	}

	// Lock is locked by the volume
	lock := flock.New(filepath.Join(tc.VolumePath, diskwriter.LockFile))
	locked, err := lock.TryLock()
	assert.False(t, locked)
	require.NoError(t, err)

	// Lock is release by Close method
	require.NoError(t, vol.Close(context.Background()))
	assert.NoFileExists(t, lock.Path())
	locked, err = lock.TryLock()
	assert.True(t, locked)
	require.NoError(t, err)
	require.NoError(t, lock.Unlock())

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
	tc := newVolumeTestCase(t)

	// Write volume ID file
	idFilePath := filepath.Join(tc.VolumePath, volumeModel.IDFile)
	writeContent := []byte("  123456789  ")
	require.NoError(t, os.WriteFile(idFilePath, writeContent, 0o0640))

	// Open volume - it loads the file
	vol, err := tc.OpenVolume()
	require.NoError(t, err)

	// Volume ID reported by the volume instance match the file content
	assert.Equal(t, volumeModel.ID("123456789"), vol.ID())

	// File content remains same
	if assert.FileExists(t, idFilePath) {
		content, err := os.ReadFile(idFilePath)
		require.NoError(t, err)
		assert.Equal(t, writeContent, content)
	}

	// Lock is locked by the volume
	lock := flock.New(filepath.Join(tc.VolumePath, diskwriter.LockFile))
	locked, err := lock.TryLock()
	assert.False(t, locked)
	require.NoError(t, err)

	// Lock is release by Close method
	require.NoError(t, vol.Close(context.Background()))
	assert.NoFileExists(t, lock.Path())
	locked, err = lock.TryLock()
	assert.True(t, locked)
	require.NoError(t, err)
	require.NoError(t, lock.Unlock())

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
	tc := newVolumeTestCase(t)

	// Open volume - first instance - ok
	vol, err := tc.OpenVolume()
	require.NoError(t, err)

	// Open volume - second instance - error
	_, err = tc.OpenVolume()
	if assert.Error(t, err) {
		wildcards.Assert(t, `cannot acquire writer lock "%s": already locked`, err.Error())
	}

	// Close volume
	require.NoError(t, vol.Close(context.Background()))
}

func TestVolume_Close_Errors(t *testing.T) {
	t.Parallel()

	tc := newVolumeTestCase(t)
	tc.Config.OverrideFileOpener = diskwriter.FileOpenerFn(func(filePath string) (diskwriter.File, error) {
		f := test.NewWriterTestFile(t, filePath)
		f.CloseError = errors.New("some close error")
		return f, nil
	})

	// Open volume, replace file opener
	vol, err := tc.OpenVolume()
	require.NoError(t, err)

	// Open two writers
	slice1 := test.NewSliceOpenedAt("2000-01-01T20:00:00.000Z")
	slice2 := test.NewSliceOpenedAt("2000-01-01T21:00:00.000Z")
	_, err = vol.OpenWriter("my-source-node", slice1.SliceKey, slice1.LocalStorage)
	require.NoError(t, err)
	_, err = vol.OpenWriter("my-source-node", slice2.SliceKey, slice2.LocalStorage)
	require.NoError(t, err)

	// Close volume, expect close errors from the writers
	err = vol.Close(context.Background())
	if assert.Error(t, err) {
		// Order of the errors is random, writers are closed in parallel
		wildcards.Assert(t, strings.TrimSpace(`
- cannot close writer for slice "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T%s":
  - cannot close file: some close error
- cannot close writer for slice "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T%s":
  - cannot close file: some close error
`), err.Error())
	}
}

// volumeTestCase is a helper to open disk writer volume in tests.
type volumeTestCase struct {
	TB                testing.TB
	Ctx               context.Context
	Logger            log.DebugLogger
	Clock             *clock.Mock
	Events            *events.Events[diskwriter.Writer]
	Allocator         *allocator
	Config            diskwriter.Config
	VolumeNodeID      string
	VolumeNodeAddress volumeModel.RemoteAddr
	VolumePath        string
	VolumeType        string
	VolumeLabel       string
}

func newVolumeTestCase(tb testing.TB) *volumeTestCase {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	tb.Cleanup(func() {
		cancel()
	})

	logger := log.NewDebugLogger()
	tmpDir := tb.TempDir()

	testAllocator := &allocator{}
	cfg := diskwriter.NewConfig()
	cfg.WatchDrainFile = false
	cfg.Allocation.OverrideAllocator = testAllocator

	return &volumeTestCase{
		TB:                tb,
		Ctx:               ctx,
		Logger:            logger,
		Clock:             clock.NewMock(),
		Events:            events.New[diskwriter.Writer](),
		Allocator:         testAllocator,
		Config:            cfg,
		VolumeNodeID:      "my-node",
		VolumeNodeAddress: "localhost:1234",
		VolumePath:        tmpDir,
		VolumeType:        "hdd",
		VolumeLabel:       "1",
	}
}

func (tc *volumeTestCase) OpenVolume() (*diskwriter.Volume, error) {
	spec := volumeModel.Spec{
		NodeID:      tc.VolumeNodeID,
		NodeAddress: tc.VolumeNodeAddress,
		Path:        tc.VolumePath,
		Type:        tc.VolumeType,
		Label:       tc.VolumeLabel,
	}

	return diskwriter.OpenVolume(tc.Ctx, tc.Logger, tc.Clock, tc.Config, spec, tc.Events)
}

func (tc *volumeTestCase) AssertLogs(expected string) bool {
	return tc.Logger.AssertJSONMessages(tc.TB, expected)
}

// allocator is dummy disk space allocator for tests.
type allocator struct {
	Ok    bool
	Error error
}

func (a *allocator) Allocate(_ diskalloc.File, _ datasize.ByteSize) (bool, error) {
	return a.Ok, a.Error
}
