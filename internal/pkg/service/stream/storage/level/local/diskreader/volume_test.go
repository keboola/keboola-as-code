package diskreader_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/flock"
	"github.com/jonboulle/clockwork"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	volumeModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

// TestOpenVolume_NonExistentPath tests that an error should occur if there is no access to the volume directory.
func TestOpenVolume_Error_DirPermissions(t *testing.T) {
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

// TestOpenVolume_NonExistentPath tests that an error should occur if there is no access to the volume ID file.
func TestOpenVolume_Error_VolumeIDFilePermissions(t *testing.T) {
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
		assert.True(t, errors.Is(err, os.ErrPermission), err.Error())
	}
}

// TestOpenVolume_Ok tests successful opening of the volume and filesystem locks.
func TestOpenVolume_Ok(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	// Create volume ID file
	require.NoError(t, os.WriteFile(filepath.Join(tc.VolumePath, volumeModel.IDFile), []byte("abcdef"), 0o640))

	vol, err := tc.OpenVolume()
	require.NoError(t, err)
	assert.Equal(t, volumeModel.ID("abcdef"), vol.ID())

	// Lock is locked by the volume
	lock := flock.New(filepath.Join(tc.VolumePath, diskreader.LockFile))
	locked, err := lock.TryLock()
	assert.False(t, locked)
	require.NoError(t, err)

	// Lock is release by Close method
	require.NoError(t, vol.Close(t.Context()))
	assert.NoFileExists(t, lock.Path())
	locked, err = lock.TryLock()
	assert.True(t, locked)
	require.NoError(t, err)
	require.NoError(t, lock.Unlock())

	// Check logs
	tc.Logger.AssertJSONMessages(tc.TB, `
{"level":"info","message":"opening volume","volume.path":"%s"}
{"level":"info","message":"opened volume","volume.id":"abcdef","volume.path":"%s","volume.type":"hdd","volume.label":"1"}
{"level":"info","message":"closing volume"}
{"level":"info","message":"closed volume"}
`)
}

// TestOpenVolume_WaitForVolumeIDFile_Ok tests that volume should wait for volume ID file.
func TestOpenVolume_WaitForVolumeIDFile_Ok(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	timeout := 5 * diskreader.WaitForVolumeIDInterval
	tc.Config.WaitForVolumeIDTimeout = timeout

	// Start opening the volume in background
	var vol *diskreader.Volume
	done := make(chan struct{})
	go func() {
		defer close(done)
		var err error
		vol, err = tc.OpenVolume()
		require.NoError(t, err)
		assert.Equal(t, volumeModel.ID("abcdef"), vol.ID())
	}()

	// Wait for 2 checks
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 1, strings.Count(tc.Logger.AllMessages(), "waiting for volume ID file"))
	}, 5*time.Second, 10*time.Millisecond)
	tc.Clock.Advance(diskreader.WaitForVolumeIDInterval)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 2, strings.Count(tc.Logger.AllMessages(), "waiting for volume ID file"))
	}, 5*time.Second, 10*time.Millisecond)

	// Create the volume ID file
	require.NoError(t, os.WriteFile(filepath.Join(tc.VolumePath, volumeModel.IDFile), []byte("abcdef"), 0o640))
	tc.Clock.Advance(diskreader.WaitForVolumeIDInterval)

	// Wait for the goroutine
	select {
	case <-done:
	case <-time.After(time.Second):
		assert.Fail(t, "timeout")
	}

	// Lock is locked by the volume
	lock := flock.New(filepath.Join(tc.VolumePath, diskreader.LockFile))
	locked, err := lock.TryLock()
	assert.False(t, locked)
	require.NoError(t, err)

	// Lock is release by Close method
	require.NoError(t, vol.Close(t.Context()))
	assert.NoFileExists(t, lock.Path())
	locked, err = lock.TryLock()
	assert.True(t, locked)
	require.NoError(t, err)
	require.NoError(t, lock.Unlock())

	// Check logs
	tc.Logger.AssertJSONMessages(tc.TB, `
{"level":"info","message":"opening volume"}
{"level":"info","message":"waiting for volume ID file"}
{"level":"info","message":"waiting for volume ID file"}
{"level":"info","message":"opened volume"}
{"level":"info","message":"closing volume"}
{"level":"info","message":"closed volume"}
`)
}

// TestOpenVolume_WaitForVolumeIDFile_Ok tests a timeout when waiting for volume ID file.
func TestOpenVolume_WaitForVolumeIDFile_Timeout(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	intervals := 4
	timeoutExtra := 1 * time.Millisecond
	timeout := time.Duration(intervals)*diskreader.WaitForVolumeIDInterval + timeoutExtra
	tc.Config.WaitForVolumeIDTimeout = timeout

	// Start opening the volume in background
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := tc.OpenVolume()
		if assert.Error(t, err) {
			wildcards.Assert(t, "cannot open volume ID file \"%s\":\n- %s", err.Error(), clockwork.ErrFakeClockDeadlineExceeded)
		}
	}()

	// Simulate multiple check attempts and then timeout
	for i := 1; i <= intervals; i++ {
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, i, strings.Count(tc.Logger.AllMessages(), "waiting for volume ID file"))
		}, time.Second, 5*time.Millisecond)
		tc.Clock.Advance(diskreader.WaitForVolumeIDInterval)
	}
	tc.Clock.Advance(timeoutExtra)

	// Wait for the goroutine
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		assert.Fail(t, "timeout")
	}

	// Check logs
	tc.Logger.AssertJSONMessages(tc.TB, `
{"level":"info","message":"opening volume"}
{"level":"info","message":"waiting for volume ID file"}
{"level":"info","message":"waiting for volume ID file"}
{"level":"info","message":"waiting for volume ID file"}
{"level":"info","message":"waiting for volume ID file"}
{"level":"info","message":"waiting for volume ID file"}
`)
}

// TestOpenVolume_VolumeLock tests that only one volume instance can be active at a time.
func TestOpenVolume_VolumeLock(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	// Create volume ID file
	require.NoError(t, os.WriteFile(filepath.Join(tc.VolumePath, volumeModel.IDFile), []byte("abcdef"), 0o640))

	// Open volume - first instance - ok
	vol, err := tc.OpenVolume()
	require.NoError(t, err)

	// Open volume - second instance - error
	_, err = tc.OpenVolume()
	if assert.Error(t, err) {
		wildcards.Assert(t, `cannot acquire reader lock "%s": already locked`, err.Error())
	}

	// Close volume
	require.NoError(t, vol.Close(t.Context()))
}

// TestVolume_Close_Errors tests propagation of readers close errors on Volume.Close().
func TestVolume_Close_Errors(t *testing.T) {
	t.Parallel()

	tc := newVolumeTestCase(t)
	tc.Config.OverrideFileOpener = diskreader.FileOpenerFn(func(filePath string) (diskreader.File, error) {
		f := newTestFile(strings.NewReader("foo bar"))
		f.CloseError = errors.New("some close error")
		return f, nil
	})

	// Create volume ID file
	require.NoError(t, os.WriteFile(filepath.Join(tc.VolumePath, volumeModel.IDFile), []byte("abcdef"), 0o640))
	slice1 := test.NewSliceOpenedAt("2000-01-01T20:00:00.000Z")
	slice2 := test.NewSliceOpenedAt("2000-01-01T21:00:00.000Z")
	require.NoError(tc.TB, os.MkdirAll(slice1.LocalStorage.DirName(tc.VolumePath), 0o750))
	require.NoError(tc.TB, os.MkdirAll(slice2.LocalStorage.DirName(tc.VolumePath), 0o750))
	require.NoError(tc.TB, os.WriteFile(slice1.LocalStorage.FileName(tc.VolumePath, "my-node"), []byte("abc"), 0o640))
	require.NoError(tc.TB, os.WriteFile(slice2.LocalStorage.FileName(tc.VolumePath, "my-node"), []byte("def"), 0o640))

	// Open volume, replace file opener
	vol, err := tc.OpenVolume()
	require.NoError(t, err)
	// Open two writers
	_, err = vol.OpenReader(slice1.SliceKey, slice1.LocalStorage, slice1.Encoding.Compression, slice1.StagingStorage.Compression)
	require.NoError(t, err)
	_, err = vol.OpenReader(slice2.SliceKey, slice2.LocalStorage, slice2.Encoding.Compression, slice2.StagingStorage.Compression)
	require.NoError(t, err)

	// Close volume, expect close errors from the writers
	require.NoError(t, vol.Close(t.Context()))
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		tc.Logger.AssertJSONMessages(collect, `
{"level":"error","message":"cannot copy to writer %sslice-my-node.csv\": io: read/write on closed pipe","volume.id":"abcdef","volume.id":"my-volume"}
{"level":"error","message":"cannot copy to writer %sslice-my-node.csv\": io: read/write on closed pipe","volume.id":"abcdef","volume.id":"my-volume"}
	`)
	}, 5*time.Second, 10*time.Millisecond)
}

// volumeTestCase is a helper to open disk reader volume in tests.
type volumeTestCase struct {
	TB                testing.TB
	Ctx               context.Context
	Logger            log.DebugLogger
	Clock             *clockwork.FakeClock
	Config            diskreader.Config
	VolumeNodeID      string
	VolumeNodeAddress volumeModel.RemoteAddr
	VolumePath        string
	VolumeType        string
	VolumeLabel       string
}

func newVolumeTestCase(tb testing.TB) *volumeTestCase {
	tb.Helper()

	ctx, cancel := context.WithTimeout(tb.Context(), 10*time.Second)
	tb.Cleanup(func() {
		cancel()
	})

	logger := log.NewDebugLogger()
	logger.ConnectTo(testhelper.VerboseStdout())
	tmpDir := tb.TempDir()

	return &volumeTestCase{
		TB:                tb,
		Ctx:               ctx,
		Logger:            logger,
		Clock:             clockwork.NewFakeClock(),
		Config:            diskreader.NewConfig(),
		VolumeNodeID:      "my-node",
		VolumeNodeAddress: "localhost:1234",
		VolumePath:        tmpDir,
		VolumeType:        "hdd",
		VolumeLabel:       "1",
	}
}

func (tc *volumeTestCase) OpenVolume() (*diskreader.Volume, error) {
	info := volumeModel.Spec{
		NodeID:      tc.VolumeNodeID,
		NodeAddress: tc.VolumeNodeAddress,
		Path:        tc.VolumePath,
		Type:        tc.VolumeType,
		Label:       tc.VolumeLabel,
	}
	return diskreader.OpenVolume(tc.Ctx, tc.Logger, tc.Clock, tc.Config, events.New[diskreader.Reader](), info)
}
