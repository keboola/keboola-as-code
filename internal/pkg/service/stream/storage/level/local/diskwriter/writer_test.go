package diskwriter_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/source/format"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/source/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestWriter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := diskwriter.NewConfig()
	logger := log.NewDebugLogger()
	clk := clock.New()
	dirPath := t.TempDir()
	filePath := filepath.Join(dirPath, "file")
	slice := test.NewSlice()
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	assert.NoError(t, err)

	w, err := diskwriter.New(ctx, logger, clk, cfg, slice, file, writesync.NewSyncer, test.DummyWriterFactory, events.New[diskwriter.Writer]())
	require.NoError(t, err)

	// Test getters
	assert.Equal(t, slice.SliceKey, w.SliceKey())

	// Test write methods
	assert.NoError(t, w.WriteRecord(clk.Now(), []any{"123", "456", "789"}))
	assert.NoError(t, w.WriteRecord(clk.Now(), []any{"abc", "def", "ghj"}))

	// Test Close method
	assert.NoError(t, w.Close(ctx))

	// Try Close again
	err = w.Close(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "writer is already closed", err.Error())
	}

	// Check file content
	content, err := os.ReadFile(filePath)
	assert.NoError(t, err)
	assert.Equal(t, []byte("123,456,789\nabc,def,ghj\n"), content)
}

func TestWriter_FlushError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := diskwriter.NewConfig()
	logger := log.NewDebugLogger()
	clk := clock.NewMock()
	dirPath := t.TempDir()
	filePath := filepath.Join(dirPath, "file")
	slice := test.NewSlice()
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	assert.NoError(t, err)

	writerFactory := func(cfg format.Config, out io.Writer, slice *model.Slice) (format.Writer, error) {
		w := test.NewDummyWriter(cfg, out, slice)
		w.FlushError = errors.New("some error")
		return w, nil
	}

	w, err := diskwriter.New(ctx, logger, clk, cfg, slice, file, writesync.NewSyncer, writerFactory, events.New[diskwriter.Writer]())
	require.NoError(t, err)

	// Test Close method
	err = w.Close(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "chain sync error:\n- chain flush error:\n  - cannot flush \"*test.DummyWriter\": some error", err.Error())
	}
}

func TestWriter_CloseError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := diskwriter.NewConfig()
	logger := log.NewDebugLogger()
	clk := clock.NewMock()
	dirPath := t.TempDir()
	filePath := filepath.Join(dirPath, "file")
	slice := test.NewSlice()
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	assert.NoError(t, err)

	writerFactory := func(cfg format.Config, out io.Writer, slice *model.Slice) (format.Writer, error) {
		w := test.NewDummyWriter(cfg, out, slice)
		w.CloseError = errors.New("some error")
		return w, nil
	}

	w, err := diskwriter.New(ctx, logger, clk, cfg, slice, file, writesync.NewSyncer, writerFactory, events.New[diskwriter.Writer]())
	require.NoError(t, err)

	// Test Close method
	err = w.Close(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "chain close error:\n- cannot close \"*test.DummyWriter\": some error", err.Error())
	}
}

func TestVolume_OpenWriter_Ok(t *testing.T) {
	t.Parallel()
	tc := test.NewWriterTestCase(t)

	w, err := tc.NewWriter()
	assert.NoError(t, err)
	assert.Len(t, tc.Volume.Writers(), 1)

	assert.NoError(t, w.Close(context.Background()))
	assert.Len(t, tc.Volume.Writers(), 0)
}

func TestVolume_OpenWriter_Duplicate(t *testing.T) {
	t.Parallel()
	tc := test.NewWriterTestCase(t)

	// Create the writer first time - ok
	w, err := tc.NewWriter()
	assert.NoError(t, err)
	assert.Len(t, tc.Volume.Writers(), 1)

	// Create writer for the same slice again - error
	_, err = tc.NewWriter()
	if assert.Error(t, err) {
		assert.Equal(t, `writer for slice "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T20:00:00.000Z" already exists`, err.Error())
	}
	assert.Len(t, tc.Volume.Writers(), 1)

	assert.NoError(t, w.Close(context.Background()))
	assert.Len(t, tc.Volume.Writers(), 0)
}

func TestVolume_OpenWriter_ClosedVolume(t *testing.T) {
	t.Parallel()
	tc := test.NewWriterVolumeTestCase(t)
	vol, err := tc.OpenVolume()
	require.NoError(t, err)

	assert.NoError(t, vol.Close(context.Background()))

	_, err = vol.OpenWriter(test.NewSlice())
	assert.Error(t, err)
}

func TestVolume_Writer_OpenFile_Ok(t *testing.T) {
	t.Parallel()
	tc := test.NewWriterTestCase(t)

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

	tc := test.NewWriterTestCase(t)

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

	tc := test.NewWriterTestCase(t)

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
	tc := test.NewWriterTestCase(t)
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
{"level":"debug","message":"closing disk writer"}                  
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
{"level":"debug","message":"closed disk writer"}
`)
}

func TestVolume_Writer_Sync_Enabled_Wait_ToDiskCache(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := test.NewWriterTestCase(t)
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
{"level":"debug","message":"closing disk writer"}
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
{"level":"debug","message":"closed disk writer"}
`)
}

func TestVolume_Writer_Sync_Enabled_NoWait_ToDisk(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := test.NewWriterTestCase(t)
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
{"level":"debug","message":"closing disk writer"}
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
{"level":"debug","message":"closed disk writer"}
`)
}

func TestVolume_Writer_Sync_Enabled_NoWait_ToDiskCache(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := test.NewWriterTestCase(t)
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
{"level":"debug","message":"closing disk writer"}
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
{"level":"debug","message":"closed disk writer"}
`)
}

func TestVolume_Writer_Sync_Disabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := test.NewWriterTestCase(t)
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
{"level":"debug","message":"closing disk writer"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"syncer stopped"}
{"level":"debug","message":"closing chain"}
{"level":"debug","message":"syncing file"}
{"level":"debug","message":"file synced"}
{"level":"debug","message":"chain closed"}
{"level":"debug","message":"closed disk writer"}
`)
}

func TestVolume_Writer_AllocateSpace_Error(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := test.NewWriterTestCase(t)
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
{"level":"debug","message":"closing disk writer"}
`)
}

func TestVolume_Writer_AllocateSpace_NotSupported(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := test.NewWriterTestCase(t)
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
{"level":"debug","message":"closing disk writer"}
`)
}

func TestVolume_Writer_AllocateSpace_Disabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := test.NewWriterTestCase(t)
	tc.Slice.LocalStorage.AllocatedDiskSpace = 0
	w, err := tc.NewWriter(volume.WithAllocator(diskalloc.DefaultAllocator{}))
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

func AssertFileContent(tb testing.TB, path, expected string) {
	tb.Helper()
	content, err := os.ReadFile(path)
	assert.NoError(tb, err)
	assert.Equal(tb, strings.TrimSpace(expected), strings.TrimSpace(string(content)))
}
