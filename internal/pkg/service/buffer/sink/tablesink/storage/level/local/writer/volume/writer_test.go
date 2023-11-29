package volume

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/allocate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestVolume_NewWriterFor_Ok(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)

	w, err := tc.NewWriter()
	assert.NoError(t, err)
	assert.Len(t, tc.Volume.writers, 1)

	assert.NoError(t, w.Close())
	assert.Len(t, tc.Volume.writers, 0)
}

func TestVolume_NewWriterFor_Duplicate(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)

	// Create the writer first time - ok
	w, err := tc.NewWriter()
	assert.NoError(t, err)
	assert.Len(t, tc.Volume.writers, 1)

	// Create writer for the same slice again - error
	_, err = tc.NewWriter()
	if assert.Error(t, err) {
		assert.Equal(t, `writer for slice "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T20:00:00.000Z" already exists`, err.Error())
	}
	assert.Len(t, tc.Volume.writers, 1)

	assert.NoError(t, w.Close())
	assert.Len(t, tc.Volume.writers, 0)
}

func TestVolume_NewWriterFor_ClosedVolume(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)
	vol, err := tc.OpenVolume()
	require.NoError(t, err)

	assert.NoError(t, vol.Close())

	_, err = vol.NewWriterFor(test.NewSlice())
	assert.Error(t, err)
}

func TestVolume_Writer_OpenFile_Ok(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)

	w, err := tc.NewWriter()
	assert.NoError(t, err)
	assert.FileExists(t, w.FilePath())

	assert.NoError(t, w.Close())
	assert.FileExists(t, w.FilePath())
}

func TestVolume_Writer_OpenFile_MkdirError(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)

	_, err := tc.OpenVolume()
	require.NoError(t, err)

	// Block creating of the slice directory in the volume directory
	assert.NoError(t, os.Chmod(tc.VolumePath, 0o440))

	_, err = tc.NewWriter()
	if assert.Error(t, err) {
		assert.True(t, errors.Is(err, os.ErrPermission))
	}

	// Revert permission for cleanup
	assert.NoError(t, os.Chmod(tc.VolumePath, 0o750))
}

func TestVolume_Writer_OpenFile_FileError(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)

	// Create read only slice directory
	assert.NoError(t, os.Mkdir(filepath.Join(tc.VolumePath, tc.Slice.LocalStorage.Dir), 0o440))

	_, err := tc.NewWriter()
	if assert.Error(t, err) {
		assert.True(t, errors.Is(err, os.ErrPermission))
	}
}

func TestVolume_Writer_Sync_Enabled_Wait_ToDisk(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Slice.LocalStorage.DiskSync.Mode = disksync.ModeDisk
	tc.Slice.LocalStorage.DiskSync.Wait = true
	w, err := tc.NewWriter()
	assert.NoError(t, err)

	// Writes are BLOCKING, each write is waiting for the next sync

	// Write two rows and trigger sync
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"foo", "bar", 123}))
		tc.Logger.Infof("TEST: write unblocked")
	}()
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"foo", "bar", 123}))
		tc.Logger.Infof("TEST: write unblocked")
	}()
	tc.ExpectWritesCount(t, 2)
	tc.TriggerSync(t)
	wg.Wait()

	// Write one row and trigger sync
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"abc", "def", 456}))
		tc.Logger.Infof("TEST: write unblocked")
	}()
	tc.ExpectWritesCount(t, 1)
	tc.TriggerSync(t)
	wg.Wait()

	// Last write
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"ghi", "jkl", 789}))
	}()
	tc.ExpectWritesCount(t, 1)

	// Close writer and volume - it triggers the last sync
	assert.NoError(t, tc.Volume.Close())

	// Wait for goroutine
	wg.Wait()

	// Check file content
	AssertFileContent(t, w.FilePath(), `
foo,bar,123
foo,bar,123
abc,def,456
ghi,jkl,789
`)

	// Check logs
	tc.AssertLogs(`
INFO  opening volume "%s"
INFO  opened volume
DEBUG  opened file
DEBUG  disk space allocation is not supported
INFO  sync is enabled, mode=disk, sync each {count=500 or bytes=1MB or interval=50ms}, check each 1ms
DEBUG  starting sync to disk
DEBUG  syncing file
DEBUG  flushing writers
DEBUG  writers flushed
DEBUG  syncing file
DEBUG  file synced
DEBUG  sync to disk done
INFO  TEST: write unblocked
INFO  TEST: write unblocked
DEBUG  starting sync to disk
DEBUG  syncing file
DEBUG  flushing writers
DEBUG  writers flushed
DEBUG  syncing file
DEBUG  file synced
DEBUG  sync to disk done
INFO  TEST: write unblocked
INFO  closing volume
DEBUG  closing file
DEBUG  stopping syncer
DEBUG  starting sync to disk
DEBUG  syncing file
DEBUG  flushing writers
DEBUG  writers flushed
DEBUG  syncing file
DEBUG  file synced
DEBUG  sync to disk done
DEBUG  syncer stopped
DEBUG  closing chain
DEBUG  syncing file
DEBUG  file synced
DEBUG  chain closed
DEBUG  closed file
INFO  closed volume
`)
}

func TestVolume_Writer_Sync_Enabled_Wait_ToDiskCache(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Slice.LocalStorage.DiskSync.Mode = disksync.ModeCache
	tc.Slice.LocalStorage.DiskSync.Wait = true
	w, err := tc.NewWriter()
	assert.NoError(t, err)

	// Writes are BLOCKING, each write is waiting for the next sync

	// Write two rows and trigger sync
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"foo", "bar", 123}))
		tc.Logger.Infof("TEST: write unblocked")
	}()
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"foo", "bar", 123}))
		tc.Logger.Infof("TEST: write unblocked")
	}()
	tc.ExpectWritesCount(t, 2)
	tc.TriggerSync(t)
	wg.Wait()

	// Write one row and trigger sync
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"abc", "def", 456}))
		tc.Logger.Infof("TEST: write unblocked")
	}()
	tc.ExpectWritesCount(t, 1)
	tc.TriggerSync(t)
	wg.Wait()

	// Last write
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"ghi", "jkl", 789}))
	}()
	tc.ExpectWritesCount(t, 1)

	// Close writer and volume - it triggers the last sync
	assert.NoError(t, tc.Volume.Close())
	wg.Wait()

	// Check file content
	AssertFileContent(t, w.FilePath(), `
foo,bar,123
foo,bar,123
abc,def,456
ghi,jkl,789
`)

	// Check logs
	tc.AssertLogs(`
INFO  opening volume "%s"
INFO  opened volume
DEBUG  opened file
DEBUG  disk space allocation is not supported
INFO  sync is enabled, mode=cache, sync each {count=500 or bytes=1MB or interval=50ms}, check each 1ms
DEBUG  starting sync to cache
DEBUG  flushing writers
DEBUG  writers flushed
DEBUG  sync to cache done
INFO  TEST: write unblocked
INFO  TEST: write unblocked
DEBUG  starting sync to cache
DEBUG  flushing writers
DEBUG  writers flushed
DEBUG  sync to cache done
INFO  TEST: write unblocked
INFO  closing volume
DEBUG  closing file
DEBUG  stopping syncer
DEBUG  starting sync to cache
DEBUG  flushing writers
DEBUG  writers flushed
DEBUG  sync to cache done
DEBUG  syncer stopped
DEBUG  closing chain
DEBUG  syncing file
DEBUG  file synced
DEBUG  chain closed
DEBUG  closed file
INFO  closed volume
`)
}

func TestVolume_Writer_Sync_Enabled_NoWait_ToDisk(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Slice.LocalStorage.DiskSync.Mode = disksync.ModeDisk
	tc.Slice.LocalStorage.DiskSync.Wait = false
	w, err := tc.NewWriter()
	assert.NoError(t, err)

	// Writes are NOT BLOCKING, write doesn't wait for the next sync

	// Write two rows and trigger sync
	assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"foo", "bar", 123}))
	assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"foo", "bar", 123}))
	tc.ExpectWritesCount(t, 2)
	tc.TriggerSync(t)

	// Write one row and trigger sync
	assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"abc", "def", 456}))
	tc.ExpectWritesCount(t, 1)
	tc.TriggerSync(t)

	// Last write
	assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"ghi", "jkl", 789}))
	tc.ExpectWritesCount(t, 1)

	// Close writer and volume - it triggers the last sync
	assert.NoError(t, tc.Volume.Close())

	// Check file content
	AssertFileContent(t, w.FilePath(), `
foo,bar,123
foo,bar,123
abc,def,456
ghi,jkl,789
`)

	// Check logs
	tc.AssertLogs(`
INFO  opening volume "%s"
INFO  opened volume
DEBUG  opened file
DEBUG  disk space allocation is not supported
INFO  sync is enabled, mode=disk, sync each {count=500 or bytes=1MB or interval=50ms}, check each 1ms
DEBUG  starting sync to disk
DEBUG  syncing file
DEBUG  flushing writers
DEBUG  writers flushed
DEBUG  syncing file
DEBUG  file synced
DEBUG  sync to disk done
DEBUG  starting sync to disk
DEBUG  syncing file
DEBUG  flushing writers
DEBUG  writers flushed
DEBUG  syncing file
DEBUG  file synced
DEBUG  sync to disk done
INFO  closing volume
DEBUG  closing file
DEBUG  stopping syncer
DEBUG  starting sync to disk
DEBUG  syncing file
DEBUG  flushing writers
DEBUG  writers flushed
DEBUG  syncing file
DEBUG  file synced
DEBUG  sync to disk done
DEBUG  syncer stopped
DEBUG  closing chain
DEBUG  syncing file
DEBUG  file synced
DEBUG  chain closed
DEBUG  closed file
INFO  closed volume
`)
}

func TestVolume_Writer_Sync_Enabled_NoWait_ToDiskCache(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Slice.LocalStorage.DiskSync.Mode = disksync.ModeCache
	tc.Slice.LocalStorage.DiskSync.Wait = false
	w, err := tc.NewWriter()
	assert.NoError(t, err)

	// Writes are NOT BLOCKING, write doesn't wait for the next sync

	// Write two rows and trigger sync
	assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"foo", "bar", 123}))
	assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"foo", "bar", 123}))
	tc.ExpectWritesCount(t, 2)
	tc.TriggerSync(t)

	// Write one row and trigger sync
	assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"abc", "def", 456}))
	tc.ExpectWritesCount(t, 1)
	tc.TriggerSync(t)

	// Last write
	assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"ghi", "jkl", 789}))
	tc.ExpectWritesCount(t, 1)

	// Close writer and volume - it triggers the last sync
	assert.NoError(t, tc.Volume.Close())

	// Check file content
	AssertFileContent(t, w.FilePath(), `
foo,bar,123
foo,bar,123
abc,def,456
ghi,jkl,789
`)

	// Check logs
	tc.AssertLogs(`
INFO  opening volume "%s"
INFO  opened volume
DEBUG  opened file
DEBUG  disk space allocation is not supported
INFO  sync is enabled, mode=cache, sync each {count=500 or bytes=1MB or interval=50ms}, check each 1ms
DEBUG  starting sync to cache
DEBUG  flushing writers
DEBUG  writers flushed
DEBUG  sync to cache done
DEBUG  starting sync to cache
DEBUG  flushing writers
DEBUG  writers flushed
DEBUG  sync to cache done
INFO  closing volume
DEBUG  closing file
DEBUG  stopping syncer
DEBUG  starting sync to cache
DEBUG  flushing writers
DEBUG  writers flushed
DEBUG  sync to cache done
DEBUG  syncer stopped
DEBUG  closing chain
DEBUG  syncing file
DEBUG  file synced
DEBUG  chain closed
DEBUG  closed file
INFO  closed volume
`)
}

func TestVolume_Writer_Sync_Disabled(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Slice.LocalStorage.DiskSync = disksync.Config{Mode: disksync.ModeDisabled}
	w, err := tc.NewWriter()
	assert.NoError(t, err)

	// Writes are NOT BLOCKING, sync is disabled completely

	// Write two rows and trigger sync
	assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"foo", "bar", 123}))
	assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"foo", "bar", 123}))
	tc.ExpectWritesCount(t, 2)

	// Write one row and trigger sync
	assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"abc", "def", 456}))
	tc.ExpectWritesCount(t, 1)

	// Last write
	assert.NoError(t, w.WriteRow(tc.Clock.Now(), []any{"ghi", "jkl", 789}))
	tc.ExpectWritesCount(t, 1)

	// Close writer and volume
	assert.NoError(t, tc.Volume.Close())

	// Check file content
	AssertFileContent(t, w.FilePath(), `
foo,bar,123
foo,bar,123
abc,def,456
ghi,jkl,789
`)

	// Check logs
	tc.AssertLogs(`
INFO  opening volume "%s"
INFO  opened volume
DEBUG  opened file
DEBUG  disk space allocation is not supported
INFO  sync is disabled
INFO  closing volume
DEBUG  closing file
DEBUG  stopping syncer
DEBUG  syncer stopped
DEBUG  closing chain
DEBUG  syncing file
DEBUG  file synced
DEBUG  chain closed
DEBUG  closed file
INFO  closed volume
`)
}

func TestVolume_Writer_AllocateSpace_Error(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Allocator.Error = errors.New("some space allocation error")

	w, err := tc.NewWriter()
	assert.NoError(t, err)
	assert.FileExists(t, w.FilePath())

	// Close writer and volume
	assert.NoError(t, tc.Volume.Close())
	assert.FileExists(t, w.FilePath())

	// Check logs
	tc.AssertLogs(`
INFO  opening volume "%s"
INFO  opened volume
DEBUG  opened file
ERROR  cannot allocate disk space "10KB", allocation skipped: some space allocation error
INFO  sync is enabled, mode=disk, sync each {count=500 or bytes=1MB or interval=50ms}, check each 1ms
INFO  closing volume
DEBUG  closing file
%A
`)
}

func TestVolume_Writer_AllocateSpace_NotSupported(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Allocator.Ok = false

	w, err := tc.NewWriter()
	assert.NoError(t, err)
	assert.FileExists(t, w.FilePath())

	// Close writer and volume
	assert.NoError(t, tc.Volume.Close())
	assert.FileExists(t, w.FilePath())

	// Check logs
	tc.AssertLogs(`
INFO  opening volume "%s"
INFO  opened volume
DEBUG  opened file
DEBUG  disk space allocation is not supported
INFO  sync is enabled, mode=disk, sync each {count=500 or bytes=1MB or interval=50ms}, check each 1ms
INFO  closing volume
DEBUG  closing file
%A
`)
}

func TestVolume_Writer_AllocateSpace_Disabled(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Slice.LocalStorage.AllocatedDiskSpace = 0
	w, err := tc.NewWriter(WithAllocator(allocate.DefaultAllocator{}))
	assert.NoError(t, err)

	// Check file - no allocation
	allocated, err := allocate.Allocated(w.FilePath())
	require.NoError(t, err)
	assert.Less(t, allocated, datasize.KB)

	// Close writer and volume
	assert.NoError(t, tc.Volume.Close())

	// Check logs
	tc.AssertLogs(`
INFO  opening volume "%s"
INFO  opened volume
DEBUG  opened file
DEBUG  disk space allocation is disabled
%A
`)
}

type writerTestCase struct {
	*volumeTestCase
	Volume *Volume
	Slice  *storage.Slice
}

func newWriterTestCase(tb testing.TB) *writerTestCase {
	tb.Helper()
	tc := &writerTestCase{}
	tc.volumeTestCase = newVolumeTestCase(tb)
	tc.Slice = test.NewSlice()
	return tc
}

func (tc *writerTestCase) OpenVolume(opts ...Option) (*Volume, error) {
	vol, err := tc.volumeTestCase.OpenVolume(opts...)
	tc.Volume = vol
	return vol, err
}

func (tc *writerTestCase) NewWriter(opts ...Option) (writer.Writer, error) {
	if tc.Volume == nil {
		// Write file with the VolumeID
		require.NoError(tc.TB, os.WriteFile(filepath.Join(tc.VolumePath, volume.IDFile), []byte("my-volume"), 0o640))

		// Open volume
		_, err := tc.OpenVolume(opts...)
		require.NoError(tc.TB, err)
	}

	// Slice definition must be valid
	val := validator.New()
	require.NoError(tc.TB, val.Validate(context.Background(), tc.Slice))

	w, err := tc.Volume.NewWriterFor(tc.Slice)
	if err != nil {
		return nil, err
	}

	return w, nil
}

type testAllocator struct {
	Ok    bool
	Error error
}

func (a *testAllocator) Allocate(_ allocate.File, _ datasize.ByteSize) (bool, error) {
	return a.Ok, a.Error
}

func AssertFileContent(tb testing.TB, path, expected string) {
	tb.Helper()
	content, err := os.ReadFile(path)
	assert.NoError(tb, err)
	assert.Equal(tb, strings.TrimSpace(expected), strings.TrimSpace(string(content)))
}
