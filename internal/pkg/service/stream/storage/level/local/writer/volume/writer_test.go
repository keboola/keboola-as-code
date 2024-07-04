package volume

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/source/writesync"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestVolume_OpenWriter_Ok(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)

	w, err := tc.NewWriter()
	assert.NoError(t, err)
	assert.Len(t, tc.Volume.writers, 1)

	assert.NoError(t, w.Close(context.Background()))
	assert.Len(t, tc.Volume.writers, 0)
}

func TestVolume_OpenWriter_Duplicate(t *testing.T) {
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

	assert.NoError(t, w.Close(context.Background()))
	assert.Len(t, tc.Volume.writers, 0)
}

func TestVolume_OpenWriter_ClosedVolume(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)
	vol, err := tc.OpenVolume()
	require.NoError(t, err)

	assert.NoError(t, vol.Close(context.Background()))

	_, err = vol.OpenWriter(test.NewSlice())
	assert.Error(t, err)
}

func TestVolume_Writer_OpenFile_Ok(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)

	w, err := tc.NewWriter()
	assert.NoError(t, err)
	assert.FileExists(t, tc.FilePath())

	assert.NoError(t, w.Close(context.Background()))
	assert.FileExists(t, tc.FilePath())
}

func TestVolume_Writer_OpenFile_MkdirError(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("permissions work different on Windows")
	}

	tc := newWriterTestCase(t)

	// Open volume
	vol, err := tc.OpenVolume()
	require.NoError(t, err)

	// Block creating of the slice directory in the volume directory
	assert.NoError(t, os.Chmod(tc.VolumePath, 0o440))

	_, err = tc.NewWriter()
	if assert.Error(t, err) {
		assert.True(t, errors.Is(err, os.ErrPermission))
	}

	// Revert permission for cleanup
	assert.NoError(t, os.Chmod(tc.VolumePath, 0o750))

	// Close volume
	assert.NoError(t, vol.Close(context.Background()))
}

func TestVolume_Writer_OpenFile_FileError(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("permissions work different on Windows")
	}

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

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Slice.LocalStorage.DiskSync.Mode = writesync.ModeDisk
	tc.Slice.LocalStorage.DiskSync.Wait = true
	w, err := tc.NewWriter()
	assert.NoError(t, err)

	// Writes are BLOCKING, each write is waiting for the next sync

	// Write two rows and trigger sync
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"foo", "bar", 123}))
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"foo", "bar", 123}))
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	tc.ExpectWritesCount(t, 2)
	tc.TriggerSync(t)
	wg.Wait()

	// Write one row and trigger sync
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"abc", "def", 456}))
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	tc.ExpectWritesCount(t, 1)
	tc.TriggerSync(t)
	wg.Wait()

	// Last write
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"ghi", "jkl", 789}))
	}()
	tc.ExpectWritesCount(t, 1)

	// Close writer - it triggers the last sync
	assert.NoError(t, w.Close(ctx))

	// Wait for goroutine
	wg.Wait()

	// Check file content
	AssertFileContent(t, tc.FilePath(), `
foo,bar,123
foo,bar,123
abc,def,456
ghi,jkl,789
`)

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"opening volume"}               
{"level":"info","message":"opened volume"}                  
{"level":"debug","message":"opened file"}                   
{"level":"debug","message":"disk space allocation is not supported"}                        
{"level":"info","message":"sync is enabled, mode=disk, sync each {count=500 or uncompressed=10MB or compressed=1MB or interval=50ms}, check each 1ms"}
{"level":"debug","message":"starting sync to disk"}         
{"level":"debug","message":"syncing file"}                  
{"level":"debug","message":"flushing writers"}              
{"level":"debug","message":"writers flushed"}               
{"level":"debug","message":"syncing file"}                  
{"level":"debug","message":"file synced"}                   
{"level":"debug","message":"sync to disk done"}             
{"level":"info","message":"TEST: write unblocked"}          
{"level":"info","message":"TEST: write unblocked"}          
{"level":"debug","message":"starting sync to disk"}         
{"level":"debug","message":"syncing file"}                  
{"level":"debug","message":"flushing writers"}              
{"level":"debug","message":"writers flushed"}               
{"level":"debug","message":"syncing file"}                  
{"level":"debug","message":"file synced"}                   
{"level":"debug","message":"sync to disk done"}             
{"level":"info","message":"TEST: write unblocked"}           
{"level":"debug","message":"closing file"}                  
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to disk"}
{"level":"debug","message":"syncing file"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"syncing file"}
{"level":"debug","message":"file synced"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"syncer stopped"}
{"level":"debug","message":"closing chain"}
{"level":"debug","message":"syncing file"}
{"level":"debug","message":"file synced"}
{"level":"debug","message":"chain closed"}
{"level":"debug","message":"closed file"}
`)
}

func TestVolume_Writer_Sync_Enabled_Wait_ToDiskCache(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Slice.LocalStorage.DiskSync.Mode = writesync.ModeCache
	tc.Slice.LocalStorage.DiskSync.Wait = true
	w, err := tc.NewWriter()
	assert.NoError(t, err)

	// Writes are BLOCKING, each write is waiting for the next sync

	// Write two rows and trigger sync
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"foo", "bar", 123}))
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"foo", "bar", 123}))
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	tc.ExpectWritesCount(t, 2)
	tc.TriggerSync(t)
	wg.Wait()

	// Write one row and trigger sync
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"abc", "def", 456}))
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	tc.ExpectWritesCount(t, 1)
	tc.TriggerSync(t)
	wg.Wait()

	// Last write
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"ghi", "jkl", 789}))
	}()
	tc.ExpectWritesCount(t, 1)

	// Close writer - it triggers the last sync
	assert.NoError(t, w.Close(ctx))
	wg.Wait()

	// Check file content
	AssertFileContent(t, tc.FilePath(), `
foo,bar,123
foo,bar,123
abc,def,456
ghi,jkl,789
`)

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"opening volume"}
{"level":"info","message":"opened volume"}
{"level":"debug","message":"opened file"}
{"level":"debug","message":"disk space allocation is not supported"}
{"level":"info","message":"sync is enabled, mode=cache, sync each {count=500 or uncompressed=10MB or compressed=1MB or interval=50ms}, check each 1ms"}
{"level":"debug","message":"starting sync to cache"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to cache done"}
{"level":"info","message":"TEST: write unblocked"}
{"level":"info","message":"TEST: write unblocked"}
{"level":"debug","message":"starting sync to cache"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to cache done"}
{"level":"info","message":"TEST: write unblocked"}
{"level":"debug","message":"closing file"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to cache"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to cache done"}
{"level":"debug","message":"syncer stopped"}
{"level":"debug","message":"closing chain"}
{"level":"debug","message":"syncing file"}
{"level":"debug","message":"file synced"}
{"level":"debug","message":"chain closed"}
{"level":"debug","message":"closed file"}
`)
}

func TestVolume_Writer_Sync_Enabled_NoWait_ToDisk(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Slice.LocalStorage.DiskSync.Mode = writesync.ModeDisk
	tc.Slice.LocalStorage.DiskSync.Wait = false
	w, err := tc.NewWriter()
	assert.NoError(t, err)

	// Writes are NOT BLOCKING, write doesn't wait for the next sync

	// Write two rows and trigger sync
	assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"foo", "bar", 123}))
	assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"foo", "bar", 123}))
	tc.ExpectWritesCount(t, 2)
	tc.TriggerSync(t)

	// Write one row and trigger sync
	assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"abc", "def", 456}))
	tc.ExpectWritesCount(t, 1)
	tc.TriggerSync(t)

	// Last write
	assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"ghi", "jkl", 789}))
	tc.ExpectWritesCount(t, 1)

	// Close writer - it triggers the last sync
	assert.NoError(t, w.Close(ctx))

	// Check file content
	AssertFileContent(t, tc.FilePath(), `
foo,bar,123
foo,bar,123
abc,def,456
ghi,jkl,789
`)

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"opening volume"}
{"level":"info","message":"opened volume"}
{"level":"debug","message":"opened file"}
{"level":"debug","message":"disk space allocation is not supported"}
{"level":"info","message":"sync is enabled, mode=disk, sync each {count=500 or uncompressed=10MB or compressed=1MB or interval=50ms}, check each 1ms"}
{"level":"debug","message":"starting sync to disk"}
{"level":"debug","message":"syncing file"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"syncing file"}
{"level":"debug","message":"file synced"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"starting sync to disk"}
{"level":"debug","message":"syncing file"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"syncing file"}
{"level":"debug","message":"file synced"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"closing file"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to disk"}
{"level":"debug","message":"syncing file"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"syncing file"}
{"level":"debug","message":"file synced"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"syncer stopped"}
{"level":"debug","message":"closing chain"}
{"level":"debug","message":"syncing file"}
{"level":"debug","message":"file synced"}
{"level":"debug","message":"chain closed"}
{"level":"debug","message":"closed file"}
`)
}

func TestVolume_Writer_Sync_Enabled_NoWait_ToDiskCache(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Slice.LocalStorage.DiskSync.Mode = writesync.ModeCache
	tc.Slice.LocalStorage.DiskSync.Wait = false
	w, err := tc.NewWriter()
	assert.NoError(t, err)

	// Writes are NOT BLOCKING, write doesn't wait for the next sync

	// Write two rows and trigger sync
	assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"foo", "bar", 123}))
	assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"foo", "bar", 123}))
	tc.ExpectWritesCount(t, 2)
	tc.TriggerSync(t)

	// Write one row and trigger sync
	assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"abc", "def", 456}))
	tc.ExpectWritesCount(t, 1)
	tc.TriggerSync(t)

	// Last write
	assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"ghi", "jkl", 789}))
	tc.ExpectWritesCount(t, 1)

	// Close writer - it triggers the last sync
	assert.NoError(t, w.Close(ctx))

	// Check file content
	AssertFileContent(t, tc.FilePath(), `
foo,bar,123
foo,bar,123
abc,def,456
ghi,jkl,789
`)

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"opening volume"}
{"level":"info","message":"opened volume"}
{"level":"debug","message":"opened file"}
{"level":"debug","message":"disk space allocation is not supported"}
{"level":"info","message":"sync is enabled, mode=cache, sync each {count=500 or uncompressed=10MB or compressed=1MB or interval=50ms}, check each 1ms"}
{"level":"debug","message":"starting sync to cache"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to cache done"}
{"level":"debug","message":"starting sync to cache"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to cache done"}
{"level":"debug","message":"closing file"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to cache"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to cache done"}
{"level":"debug","message":"syncer stopped"}
{"level":"debug","message":"closing chain"}
{"level":"debug","message":"syncing file"}
{"level":"debug","message":"file synced"}
{"level":"debug","message":"chain closed"}
{"level":"debug","message":"closed file"}
`)
}

func TestVolume_Writer_Sync_Disabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Slice.LocalStorage.DiskSync = writesync.Config{Mode: writesync.ModeDisabled}
	w, err := tc.NewWriter()
	assert.NoError(t, err)

	// Writes are NOT BLOCKING, sync is disabled completely

	// Write two rows and trigger sync
	assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"foo", "bar", 123}))
	assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"foo", "bar", 123}))
	tc.ExpectWritesCount(t, 2)

	// Write one row and trigger sync
	assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"abc", "def", 456}))
	tc.ExpectWritesCount(t, 1)

	// Last write
	assert.NoError(t, w.WriteRecord(tc.Clock.Now(), []any{"ghi", "jkl", 789}))
	tc.ExpectWritesCount(t, 1)

	// Close writer
	assert.NoError(t, w.Close(ctx))

	// Check file content
	AssertFileContent(t, tc.FilePath(), `
foo,bar,123
foo,bar,123
abc,def,456
ghi,jkl,789
`)

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"opening volume"}
{"level":"info","message":"opened volume"}
{"level":"debug","message":"opened file","volume.id":"my-volume","file.path":"%s","projectId":"123","branchId":"456","sourceId":"my-source","sinkId":"my-sink","fileId":"2000-01-01T19:00:00.000Z","sliceId":"2000-01-01T20:00:00.000Z"}
{"level":"debug","message":"disk space allocation is not supported"}
{"level":"info","message":"sync is disabled"}
{"level":"debug","message":"closing file"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"syncer stopped"}
{"level":"debug","message":"closing chain"}
{"level":"debug","message":"syncing file"}
{"level":"debug","message":"file synced"}
{"level":"debug","message":"chain closed"}
{"level":"debug","message":"closed file"}
`)
}

func TestVolume_Writer_AllocateSpace_Error(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Allocator.Error = errors.New("some space allocation error")

	w, err := tc.NewWriter()
	assert.NoError(t, err)
	assert.FileExists(t, tc.FilePath())

	// Close writer
	assert.NoError(t, w.Close(ctx))
	assert.FileExists(t, tc.FilePath())

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"opening volume"}
{"level":"info","message":"opened volume"}
{"level":"debug","message":"opened file"}
{"level":"error","message":"cannot allocate disk space \"10KB\", allocation skipped: some space allocation error"}
{"level":"info","message":"sync is enabled, mode=disk, sync each {count=500 or uncompressed=10MB or compressed=1MB or interval=50ms}, check each 1ms"}
{"level":"debug","message":"closing file"}
`)
}

func TestVolume_Writer_AllocateSpace_NotSupported(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Allocator.Ok = false

	w, err := tc.NewWriter()
	assert.NoError(t, err)
	assert.FileExists(t, tc.FilePath())

	// Close writer
	assert.NoError(t, w.Close(ctx))
	assert.FileExists(t, tc.FilePath())

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"opening volume"}
{"level":"info","message":"opened volume"}
{"level":"debug","message":"opened file"}
{"level":"debug","message":"disk space allocation is not supported"}
{"level":"info","message":"sync is enabled, mode=disk, sync each {count=500 or uncompressed=10MB or compressed=1MB or interval=50ms}, check each 1ms"}
{"level":"debug","message":"closing file"}
`)
}

func TestVolume_Writer_AllocateSpace_Disabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Slice.LocalStorage.AllocatedDiskSpace = 0
	w, err := tc.NewWriter(WithAllocator(diskalloc.DefaultAllocator{}))
	assert.NoError(t, err)

	// Check file - no allocation
	allocated, err := diskalloc.Allocated(tc.FilePath())
	require.NoError(t, err)
	assert.Less(t, allocated, datasize.KB)

	// Close writer
	assert.NoError(t, w.Close(ctx))

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"opening volume"}
{"level":"info","message":"opened volume"}
{"level":"debug","message":"opened file"}
{"level":"debug","message":"disk space allocation is disabled"}
`)
}

type writerTestCase struct {
	*volumeTestCase
	Volume *Volume
	Slice  *model.Slice
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
		// Write file with the ID
		require.NoError(tc.TB, os.WriteFile(filepath.Join(tc.VolumePath, volume.IDFile), []byte("my-volume"), 0o640))

		// Open volume
		vol, err := tc.OpenVolume(opts...)
		require.NoError(tc.TB, err)

		// Close volume after the test
		tc.TB.Cleanup(func() {
			assert.NoError(tc.TB, vol.Close(context.Background()))
		})
	}

	// Slice definition must be valid
	val := validator.New()
	require.NoError(tc.TB, val.Validate(context.Background(), tc.Slice))

	w, err := tc.Volume.OpenWriter(tc.Slice)
	if err != nil {
		return nil, err
	}

	return w, nil
}

func (tc *writerTestCase) FilePath() string {
	return filepath.Join(tc.VolumePath, tc.Slice.LocalStorage.Dir, tc.Slice.LocalStorage.Filename)
}

type testAllocator struct {
	Ok    bool
	Error error
}

func (a *testAllocator) Allocate(_ diskalloc.File, _ datasize.ByteSize) (bool, error) {
	return a.Ok, a.Error
}

func AssertFileContent(tb testing.TB, path, expected string) {
	tb.Helper()
	content, err := os.ReadFile(path)
	assert.NoError(tb, err)
	assert.Equal(tb, strings.TrimSpace(expected), strings.TrimSpace(string(content)))
}
