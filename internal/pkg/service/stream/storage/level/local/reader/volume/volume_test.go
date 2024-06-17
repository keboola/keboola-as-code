package volume

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/gofrs/flock"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// TestOpenVolume_NonExistentPath tests that an error should occur if there is no access to the volume directory.
func TestOpenVolume_Error_DirPermissions(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("permissions work different on Windows")
	}

	tc := newVolumeTestCase(t)

	// Volume directory is readonly
	assert.NoError(t, os.Chmod(tc.VolumePath, 0o440))

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
	path := filepath.Join(tc.VolumePath, volume.IDFile)
	assert.NoError(t, os.WriteFile(path, []byte("abc"), 0o640))
	assert.NoError(t, os.Chmod(path, 0o110))

	_, err := tc.OpenVolume()
	if assert.Error(t, err) {
		assert.True(t, errors.Is(err, os.ErrPermission), err.Error())
	}
}

// TestOpenVolume_NonExistentPath tests successful opening of the volume and filesystem locks.
func TestOpenVolume_Ok(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	// Create volume ID file
	assert.NoError(t, os.WriteFile(filepath.Join(tc.VolumePath, volume.IDFile), []byte("abcdef"), 0o640))

	vol, err := tc.OpenVolume()
	assert.NoError(t, err)
	assert.Equal(t, volume.ID("abcdef"), vol.ID())

	// Lock is locked by the volume
	lock := flock.New(filepath.Join(tc.VolumePath, lockFile))
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
{"level":"info","message":"opened volume","volume.id":"abcdef","volume.path":"%s","volume.type":"hdd","volume.label":"1"}
{"level":"info","message":"closing volume"}
{"level":"info","message":"closed volume"}
`)
}

// TestOpenVolume_WaitForVolumeIDFile_Ok tests that volume should wait for volume ID file.
func TestOpenVolume_WaitForVolumeIDFile_Ok(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	// Start opening the volume in background
	var vol *Volume
	done := make(chan struct{})
	go func() {
		defer close(done)
		var err error
		timeout := 5 * waitForVolumeIDInterval
		vol, err = tc.OpenVolume(WithWaitForVolumeIDTimeout(timeout))
		assert.NoError(t, err)
		assert.Equal(t, volume.ID("abcdef"), vol.ID())
	}()

	// Wait for 2 checks
	assert.Eventually(t, func() bool {
		return strings.Count(tc.Logger.AllMessages(), "waiting for volume ID file") == 1
	}, time.Second, 5*time.Millisecond)
	tc.Clock.Add(waitForVolumeIDInterval)
	assert.Eventually(t, func() bool {
		return strings.Count(tc.Logger.AllMessages(), "waiting for volume ID file") == 2
	}, time.Second, 5*time.Millisecond)

	// Create the volume ID file
	assert.NoError(t, os.WriteFile(filepath.Join(tc.VolumePath, volume.IDFile), []byte("abcdef"), 0o640))
	tc.Clock.Add(waitForVolumeIDInterval)

	// Wait for the goroutine
	select {
	case <-done:
	case <-time.After(time.Second):
		assert.Fail(t, "timeout")
	}

	// Lock is locked by the volume
	lock := flock.New(filepath.Join(tc.VolumePath, lockFile))
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
	timeout := time.Duration(intervals)*waitForVolumeIDInterval + timeoutExtra

	// Start opening the volume in background
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := tc.OpenVolume(WithWaitForVolumeIDTimeout(timeout))
		if assert.Error(t, err) {
			wildcards.Assert(t, `cannot open volume ID file "%s": context deadline exceeded`, err.Error())
		}
	}()

	// Simulate multiple check attempts and then timeout
	for i := 1; i <= intervals; i++ {
		assert.Eventually(t, func() bool {
			return strings.Count(tc.Logger.AllMessages(), "waiting for volume ID file") == i
		}, time.Second, 5*time.Millisecond)
		tc.Clock.Add(waitForVolumeIDInterval)
	}
	tc.Clock.Add(timeoutExtra)

	// Wait for the goroutine
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		assert.Fail(t, "timeout")
	}

	// Check logs
	tc.AssertLogs(`
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
	assert.NoError(t, os.WriteFile(filepath.Join(tc.VolumePath, volume.IDFile), []byte("abcdef"), 0o640))

	// Open volume - first instance - ok
	vol, err := tc.OpenVolume()
	assert.NoError(t, err)

	// Open volume - second instance - error
	_, err = tc.OpenVolume()
	if assert.Error(t, err) {
		wildcards.Assert(t, `cannot acquire reader lock "%s": already locked`, err.Error())
	}

	// Close volume
	assert.NoError(t, vol.Close(context.Background()))
}

// TestVolume_Close_Errors tests propagation of readers close errors on Volume.Close().
func TestVolume_Close_Errors(t *testing.T) {
	t.Parallel()

	tc := newVolumeTestCase(t)

	// Create volume ID file
	assert.NoError(t, os.WriteFile(filepath.Join(tc.VolumePath, volume.IDFile), []byte("abcdef"), 0o640))

	// Open volume, replace file opener
	vol, err := tc.OpenVolume(WithFileOpener(func(filePath string) (File, error) {
		f := newTestFile(strings.NewReader("foo bar"))
		f.CloseError = errors.New("some close error")
		return f, nil
	}))
	require.NoError(t, err)

	// Open two writers
	_, err = vol.OpenReader(test.NewSliceOpenedAt("2000-01-01T20:00:00.000Z"))
	require.NoError(t, err)
	_, err = vol.OpenReader(test.NewSliceOpenedAt("2000-01-01T21:00:00.000Z"))
	require.NoError(t, err)

	// Close volume, expect close errors from the writers
	err = vol.Close(context.Background())
	if assert.Error(t, err) {
		// Order of the errors is random, readers are closed in parallel
		wildcards.Assert(t, strings.TrimSpace(`
- cannot close reader for slice "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T%s": chain close error:
  - cannot close "*volume.testFile": some close error
- cannot close reader for slice "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T%s": chain close error:
  - cannot close "*volume.testFile": some close error
`), err.Error())
	}
}

type volumeTestCase struct {
	TB           testing.TB
	Ctx          context.Context
	Logger       log.DebugLogger
	Clock        *clock.Mock
	VolumeNodeID string
	VolumePath   string
	VolumeType   string
	VolumeLabel  string
}

func newVolumeTestCase(tb testing.TB) *volumeTestCase {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	tb.Cleanup(func() {
		cancel()
	})

	logger := log.NewDebugLogger()
	tmpDir := tb.TempDir()

	return &volumeTestCase{
		TB:           tb,
		Ctx:          ctx,
		Logger:       logger,
		Clock:        clock.NewMock(),
		VolumeNodeID: "my-node",
		VolumePath:   tmpDir,
		VolumeType:   "hdd",
		VolumeLabel:  "1",
	}
}

func (tc *volumeTestCase) OpenVolume(opts ...Option) (*Volume, error) {
	info := volume.Spec{NodeID: tc.VolumeNodeID, Path: tc.VolumePath, Type: tc.VolumeType, Label: tc.VolumeLabel}
	return Open(tc.Ctx, tc.Logger, tc.Clock, info, opts...)
}

func (tc *volumeTestCase) AssertLogs(expected string) bool {
	return tc.Logger.AssertJSONMessages(tc.TB, expected)
}
