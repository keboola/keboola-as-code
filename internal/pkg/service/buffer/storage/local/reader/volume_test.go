package reader

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
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/stretchr/testify/assert"
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
		assert.True(t, errors.Is(err, os.ErrPermission), err.Error())
	}
}

func TestOpenVolume_Ok(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	// Create volume ID file
	assert.NoError(t, os.WriteFile(filepath.Join(tc.VolumePath, local.VolumeIDFile), []byte("abcdef"), 0o640))

	volume, err := tc.OpenVolume()
	assert.NoError(t, err)
	assert.Equal(t, storage.VolumeID("abcdef"), volume.VolumeID())

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

func TestOpenVolume_WaitForVolumeIDFile_Ok(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	// Start opening the volume in background
	var volume *Volume
	done := make(chan struct{})
	go func() {
		defer close(done)
		var err error
		timeout := 5 * waitForVolumeIDInterval
		volume, err = tc.OpenVolume(WithWaitForVolumeIDTimeout(timeout))
		assert.NoError(t, err)
		assert.Equal(t, storage.VolumeID("abcdef"), volume.VolumeID())
	}()

	// Create the file after a while
	tc.Clock.Add(waitForVolumeIDInterval)
	tc.Clock.Add(waitForVolumeIDInterval)
	assert.NoError(t, os.WriteFile(filepath.Join(tc.VolumePath, local.VolumeIDFile), []byte("abcdef"), 0o640))
	tc.Clock.Add(waitForVolumeIDInterval)

	// Wait for the goroutine
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		assert.Fail(t, "timeout")
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
INFO  waiting for volume ID file
INFO  waiting for volume ID file
INFO  opened volume
INFO  closing volume
INFO  closed volume
`)
}

func TestOpenVolume_WaitForVolumeIDFile_Timeout(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	timeout := 5 * waitForVolumeIDInterval

	// Start opening the volume in background
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := tc.OpenVolume(WithWaitForVolumeIDTimeout(timeout))
		if assert.Error(t, err) {
			wildcards.Assert(t, `cannot open volume ID file "%s": waiting timeout after %s`, err.Error())
		}
	}()

	// Simulate timeout
	for elapsed := time.Duration(0); elapsed <= timeout; elapsed += waitForVolumeIDInterval {
		tc.Clock.Add(waitForVolumeIDInterval)
	}

	// Wait for the goroutine
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		assert.Fail(t, "timeout")
	}

	// Check logs
	// Note: waiting message is present 6x, because wait and timeout timers are triggered at the same time.
	// The clock is mocked, so the first registered timer = wait is unlocked first.
	tc.AssertLogs(`
INFO  opening volume "%s"
INFO  waiting for volume ID file
INFO  waiting for volume ID file
INFO  waiting for volume ID file
INFO  waiting for volume ID file
INFO  waiting for volume ID file
INFO  waiting for volume ID file
`)
}

// TestOpenVolume_VolumeLock tests that only one volume instance can be active at a time.
func TestOpenVolume_VolumeLock(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	// Create volume ID file
	assert.NoError(t, os.WriteFile(filepath.Join(tc.VolumePath, local.VolumeIDFile), []byte("abcdef"), 0o640))

	// Open volume - first instance - ok
	_, err := tc.OpenVolume()
	assert.NoError(t, err)

	// Open volume - second instance - error
	_, err = tc.OpenVolume()
	if assert.Error(t, err) {
		wildcards.Assert(t, `cannot acquire reader lock "%s": already locked`, err.Error())
	}
}

type volumeTestCase struct {
	T          testing.TB
	Ctx        context.Context
	Logger     log.DebugLogger
	Clock      *clock.Mock
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
		VolumePath: tmpDir,
	}
}

func (tc *volumeTestCase) OpenVolume(opts ...Option) (*Volume, error) {
	return OpenVolume(tc.Ctx, tc.Logger, tc.Clock, tc.VolumePath, opts...)
}

func (tc *volumeTestCase) AssertLogs(expected string) bool {
	return wildcards.Assert(tc.T, strings.TrimSpace(expected), strings.TrimSpace(tc.Logger.AllMessages()))
}
