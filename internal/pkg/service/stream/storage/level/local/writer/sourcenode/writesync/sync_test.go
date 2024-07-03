package writesync

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/sourcenode/count"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/sourcenode/size"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const (
	testWaitTimeout = 2 * time.Second
)

func TestNewSyncWriter_ModeDisabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeDisabled

	syncerWriter := tc.NewSyncerWriter()
	assert.Nil(t, syncerWriter.TriggerSync(ctx, false))
	assert.Nil(t, syncerWriter.TriggerSync(ctx, true))
	assert.NoError(t, syncerWriter.Stop(ctx))

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"sync is disabled"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"syncer stopped"}
`)
}

func TestNewSyncWriter_ModeInvalid(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Config.Mode = "invalid"
	assert.Panics(t, func() {
		tc.Config.IntervalTrigger = 0
		tc.NewSyncerWriter()
	})
}

// TestNewSyncWriter_NoCheckInterval tests that Config.CheckInterval field must be > 0.
func TestNewSyncWriter_NoCheckInterval(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	assert.Panics(t, func() {
		tc.Config.CheckInterval = 0
		tc.NewSyncerWriter()
	})
}

// TestNewSyncWriter_NoIntervalTrigger tests that Config.IntervalTrigger field must be > 0.
func TestNewSyncWriter_NoIntervalTrigger(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	assert.Panics(t, func() {
		tc.Config.IntervalTrigger = 0
		tc.NewSyncerWriter()
	})
}

// TestNewSyncWriter_NoUncompressedBytesTrigger tests that Config.BytesTrigger field must be > 0.
func TestNewSyncWriter_NoUncompressedBytesTrigger(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	assert.Panics(t, func() {
		tc.Config.UncompressedBytesTrigger = 0
		tc.NewSyncerWriter()
	})
}

// TestNewSyncWriter_NoCompressedBytesTriggertests that Config.BytesTrigger field must be > 0.
func TestNewSyncWriter_NoCompressedBytesTrigger(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	assert.Panics(t, func() {
		tc.Config.CompressedBytesTrigger = 0
		tc.NewSyncerWriter()
	})
}

func TestSyncWriter_StopStoppedSyncer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	syncerWriter := tc.NewSyncerWriter()

	// Stop the syncer
	assert.NoError(t, syncerWriter.Stop(ctx))

	// Try stop again
	err := syncerWriter.Stop(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "syncer is already stopped", err.Error())
	}
}

// TestSyncWriter_DoWithNotify_Wait tests wrapping of multiple write operations using DoWithNotify method.
func TestSyncWriter_Write_Wait(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeDisk
	tc.Config.Wait = true

	syncerWriter := tc.NewSyncerWriter()

	// Write data
	for i := 1; i <= 3; i++ {
		syncerWriter.MustWriteTestData(t, i)
	}
	notifier := syncerWriter.Notifier()

	// Wait for the notifier
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, notifier.WaitWithTimeout(testWaitTimeout))
		tc.Logger.Info(ctx, "TEST: sync wait unblocked")
	}()

	// Wait for sync
	assert.NoError(t, syncerWriter.TriggerSync(ctx, false).Wait())
	wg.Wait()

	// Check output
	assert.Equal(t, "data1data2data3", tc.Chain.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncerWriter.Stop(ctx))

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"sync is enabled, mode=disk, sync each {count=100 or uncompressed=128KB or compressed=128KB or interval=10ms}, check each 1ms"}
{"level":"info","message":"TEST: write \"data1\""}
{"level":"info","message":"TEST: write \"data2\""}
{"level":"info","message":"TEST: write \"data3\""}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"syncer stopped"}
`)
}

// TestSyncWriter_DoWithNotify_NoWait tests wrapping of multiple write operations using DoWithNotify method.
func TestSyncWriter_DoWithNotify_NoWait(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeDisk
	tc.Config.Wait = false

	syncerWriter := tc.NewSyncerWriter()

	// Write data
	for i := 1; i <= 3; i++ {
		syncerWriter.MustWriteTestData(t, i)
	}
	notifier := syncerWriter.Notifier()

	// Wait is disabled
	require.Nil(t, notifier)
	assert.NoError(t, notifier.WaitWithTimeout(testWaitTimeout))

	// Wait for sync
	assert.NoError(t, syncerWriter.TriggerSync(ctx, false).Wait())

	// Check output
	assert.Equal(t, "data1data2data3", tc.Chain.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncerWriter.Stop(ctx))

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"sync is enabled, mode=disk, sync each {count=100 or uncompressed=128KB or compressed=128KB or interval=10ms}, check each 1ms"}
{"level":"info","message":"TEST: write \"data1\""}
{"level":"info","message":"TEST: write \"data2\""}
{"level":"info","message":"TEST: write \"data3\""}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"syncer stopped"}
`)
}

func TestSyncWriter_SkipEmptySync(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Config.Wait = true

	syncerWriter := tc.NewSyncerWriter()

	// Wait for sync
	assert.NoError(t, syncerWriter.TriggerSync(ctx, false).Wait())

	// Check output
	assert.Equal(t, "", tc.Chain.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncerWriter.Stop(ctx))

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"sync is enabled, mode=disk, sync each {count=100 or uncompressed=128KB or compressed=128KB or interval=10ms}, check each 1ms"}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"syncer stopped"}
`)
}

// TestSyncWriter_SyncToDisk_Wait_Ok tests that w.Notifier().Wait() blocks if SyncConfig.SyncWait = true.
// Sync operation is successful.
func TestSyncWriter_SyncToDisk_Wait_Ok(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeDisk
	tc.Config.Wait = true

	syncerWriter := tc.NewSyncerWriter()

	// Write data
	wg := &sync.WaitGroup{}
	for i := 1; i <= 3; i++ {
		syncerWriter.MustWriteTestData(t, i)

		notifier := syncerWriter.Notifier()
		assert.NotNil(t, notifier)

		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, notifier.WaitWithTimeout(testWaitTimeout))
			tc.Logger.Info(ctx, "TEST: sync wait unblocked - part 1")
		}()
	}

	// Wait for sync 1
	syncerWriter.TriggerSync(ctx, false)
	wg.Wait()

	// Write data
	for i := 4; i <= 6; i++ {
		syncerWriter.MustWriteTestData(t, i)

		notifier := syncerWriter.Notifier()
		assert.NotNil(t, notifier)

		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, notifier.WaitWithTimeout(testWaitTimeout))
			tc.Logger.Info(ctx, "TEST: sync wait unblocked - part 2")
		}()
	}

	// Wait for sync 2
	syncerWriter.TriggerSync(ctx, false)
	wg.Wait()

	// Check output
	assert.Equal(t, "data1data2data3data4data5data6", tc.Chain.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncerWriter.Stop(ctx))

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"sync is enabled, mode=disk, sync each {count=100 or uncompressed=128KB or compressed=128KB or interval=10ms}, check each 1ms"}
{"level":"info","message":"TEST: write \"data1\""}
{"level":"info","message":"TEST: write \"data2\""}
{"level":"info","message":"TEST: write \"data3\""}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"info","message":"TEST: sync wait unblocked - part 1"}
{"level":"info","message":"TEST: sync wait unblocked - part 1"}
{"level":"info","message":"TEST: sync wait unblocked - part 1"}
{"level":"info","message":"TEST: write \"data4\""}
{"level":"info","message":"TEST: write \"data5\""}
{"level":"info","message":"TEST: write \"data6\""}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"info","message":"TEST: sync wait unblocked - part 2"}
{"level":"info","message":"TEST: sync wait unblocked - part 2"}
{"level":"info","message":"TEST: sync wait unblocked - part 2"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"syncer stopped"}
`)
}

// TestSyncWriter_SyncToDisk_Wait_Error tests that w.Notifier().Wait() blocks if SyncConfig.SyncWait = true.
// The sync error is returned by the Wait() method.
func TestSyncWriter_SyncToDisk_Wait_Error(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeDisk
	tc.Config.Wait = true
	tc.Chain.SyncError = errors.New("some sync error")

	syncerWriter := tc.NewSyncerWriter()

	// Write data
	wg := &sync.WaitGroup{}
	for i := 1; i <= 3; i++ {
		syncerWriter.MustWriteTestData(t, i)

		notifier := syncerWriter.Notifier()
		assert.NotNil(t, notifier)

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := notifier.WaitWithTimeout(testWaitTimeout); assert.Error(t, err) {
				assert.Equal(t, "some sync error", err.Error())
			}
			tc.Logger.Info(ctx, "TEST: sync wait unblocked")
		}()
	}

	// Wait for sync
	syncerWriter.TriggerSync(ctx, false)
	wg.Wait()

	// Check output
	assert.Equal(t, "data1data2data3", tc.Chain.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	err := syncerWriter.Stop(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "some sync error", err.Error())
	}

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"sync is enabled, mode=disk, sync each {count=100 or uncompressed=128KB or compressed=128KB or interval=10ms}, check each 1ms"}
{"level":"info","message":"TEST: write \"data1\""}
{"level":"info","message":"TEST: write \"data2\""}
{"level":"info","message":"TEST: write \"data3\""}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"error","message":"sync to disk failed: some sync error"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"error","message":"sync to disk failed: some sync error"}
{"level":"debug","message":"syncer stopped"}
`)
}

// TestSyncWriter_SyncToDisk_NoWait_Ok tests that w.Notifier().Wait() doesn't block if SyncConfig.SyncWait = false.
// The sync operation is successful.
func TestSyncWriter_SyncToDisk_NoWait_Ok(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeDisk
	tc.Config.Wait = false

	syncerWriter := tc.NewSyncerWriter()

	// Write data
	// Waiting for sync is disabled, so writes are not blocked
	for i := 1; i <= 3; i++ {
		syncerWriter.MustWriteTestData(t, i)

		notifier := syncerWriter.Notifier()
		assert.Nil(t, notifier)
		assert.NoError(t, notifier.WaitWithTimeout(testWaitTimeout))
	}

	// Sync
	assert.NoError(t, syncerWriter.TriggerSync(ctx, false).Wait())

	// Check output
	assert.Equal(t, "data1data2data3", tc.Chain.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncerWriter.Stop(ctx))

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"sync is enabled, mode=disk, sync each {count=100 or uncompressed=128KB or compressed=128KB or interval=10ms}, check each 1ms"}                            
{"level":"info","message":"TEST: write \"data1\""}
{"level":"info","message":"TEST: write \"data2\""}
{"level":"info","message":"TEST: write \"data3\""}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"syncer stopped"}
`)
}

// TestSyncWriter_SyncToDisk_NoWait_Error tests that w.Notifier().Wait() doesn't block if SyncConfig.SyncWait = false.
// The sync error is logged.
func TestSyncWriter_SyncToDisk_NoWait_Error(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeDisk
	tc.Config.Wait = false
	tc.Chain.SyncError = errors.New("some sync error")

	syncerWriter := tc.NewSyncerWriter()

	// Write data
	// Waiting for sync is disabled, so writes are not blocked
	for i := 1; i <= 3; i++ {
		syncerWriter.MustWriteTestData(t, i)

		notifier := syncerWriter.Notifier()
		assert.Nil(t, notifier)
		assert.NoError(t, notifier.WaitWithTimeout(testWaitTimeout))
	}

	// Sync
	err := syncerWriter.TriggerSync(ctx, false).Wait()
	if assert.Error(t, err) {
		assert.Equal(t, "some sync error", err.Error())
	}

	// Check output
	assert.Equal(t, "data1data2data3", tc.Chain.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	err = syncerWriter.Stop(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "some sync error", err.Error())
	}

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"sync is enabled, mode=disk, sync each {count=100 or uncompressed=128KB or compressed=128KB or interval=10ms}, check each 1ms"}
{"level":"info","message":"TEST: write \"data1\""}
{"level":"info","message":"TEST: write \"data2\""}
{"level":"info","message":"TEST: write \"data3\""}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"error","message":"sync to disk failed: some sync error"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"error","message":"sync to disk failed: some sync error"}
{"level":"debug","message":"syncer stopped"}
`)
}

// TestSyncWriter_SyncToCache_Wait_Ok tests that w.Notifier().Wait() blocks if SyncConfig.SyncWait = true.
// The flush operation is successful.
func TestSyncWriter_SyncToCache_Wait_Ok(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeCache
	tc.Config.Wait = true

	syncerWriter := tc.NewSyncerWriter()

	// Write data
	wg := &sync.WaitGroup{}
	for i := 1; i <= 3; i++ {
		syncerWriter.MustWriteTestData(t, i)

		notifier := syncerWriter.Notifier()
		assert.NotNil(t, notifier)

		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, notifier.WaitWithTimeout(testWaitTimeout))
			tc.Logger.Info(ctx, "TEST: sync wait unblocked - part 1")
		}()
	}

	// Wait for sync 1
	syncerWriter.TriggerSync(ctx, false)
	wg.Wait()

	// Write data
	for i := 4; i <= 6; i++ {
		syncerWriter.MustWriteTestData(t, i)

		notifier := syncerWriter.Notifier()
		assert.NotNil(t, notifier)

		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, notifier.WaitWithTimeout(testWaitTimeout))
			tc.Logger.Info(ctx, "TEST: sync wait unblocked - part 2")
		}()
	}

	// Wait for sync 2
	syncerWriter.TriggerSync(ctx, false)
	wg.Wait()

	// Check output
	assert.Equal(t, "data1data2data3data4data5data6", tc.Chain.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncerWriter.Stop(ctx))

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"sync is enabled, mode=cache, sync each {count=100 or uncompressed=128KB or compressed=128KB or interval=10ms}, check each 1ms"}
{"level":"info","message":"TEST: write \"data1\""}
{"level":"info","message":"TEST: write \"data2\""}
{"level":"info","message":"TEST: write \"data3\""}
{"level":"debug","message":"starting sync to cache"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to cache done"}
{"level":"info","message":"TEST: sync wait unblocked - part 1"}
{"level":"info","message":"TEST: sync wait unblocked - part 1"}
{"level":"info","message":"TEST: sync wait unblocked - part 1"}
{"level":"info","message":"TEST: write \"data4\""}
{"level":"info","message":"TEST: write \"data5\""}
{"level":"info","message":"TEST: write \"data6\""}
{"level":"debug","message":"starting sync to cache"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to cache done"}
{"level":"info","message":"TEST: sync wait unblocked - part 2"}
{"level":"info","message":"TEST: sync wait unblocked - part 2"}
{"level":"info","message":"TEST: sync wait unblocked - part 2"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to cache"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to cache done"}
{"level":"debug","message":"syncer stopped"}
`)
}

// TestSyncWriter_SyncToCache_Wait_Error tests that w.Notifier().Wait() blocks if SyncConfig.SyncWait = true.
// The flush error is returned by the Wait() method.
func TestSyncWriter_SyncToCache_Wait_Error(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeCache
	tc.Config.Wait = true
	tc.Chain.FlushError = errors.New("some flush error")

	syncerWriter := tc.NewSyncerWriter()

	// Write data
	wg := &sync.WaitGroup{}
	for i := 1; i <= 3; i++ {
		syncerWriter.MustWriteTestData(t, i)

		notifier := syncerWriter.Notifier()
		assert.NotNil(t, notifier)

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := notifier.WaitWithTimeout(testWaitTimeout); assert.Error(t, err) {
				assert.Equal(t, "some flush error", err.Error())
			}
			tc.Logger.Info(ctx, "TEST: sync wait unblocked")
		}()
	}

	// Wait for sync
	syncerWriter.TriggerSync(ctx, false)
	wg.Wait()

	// Check output
	assert.Equal(t, "data1data2data3", tc.Chain.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	err := syncerWriter.Stop(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "some flush error", err.Error())
	}

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"sync is enabled, mode=cache, sync each {count=100 or uncompressed=128KB or compressed=128KB or interval=10ms}, check each 1ms"}
{"level":"info","message":"TEST: write \"data1\""}
{"level":"info","message":"TEST: write \"data2\""}
{"level":"info","message":"TEST: write \"data3\""}
{"level":"debug","message":"starting sync to cache"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"error","message":"sync to cache failed: some flush error"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to cache"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"error","message":"sync to cache failed: some flush error"}
{"level":"debug","message":"syncer stopped"}
`)
}

// TestSyncWriter_SyncToCache_NoWait_Ok tests that w.Notifier().Wait() doesn't block if SyncConfig.SyncWait = false.
// The flush operation is successful.
func TestSyncWriter_SyncToCache_NoWait_Ok(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeCache
	tc.Config.Wait = false

	syncerWriter := tc.NewSyncerWriter()

	// Write data
	// Waiting for sync is disabled, so writes are not blocked
	for i := 1; i <= 3; i++ {
		syncerWriter.MustWriteTestData(t, i)

		notifier := syncerWriter.Notifier()
		assert.Nil(t, notifier)
		assert.NoError(t, notifier.WaitWithTimeout(testWaitTimeout))
	}

	// Sync
	assert.NoError(t, syncerWriter.TriggerSync(ctx, false).Wait())

	// Check output
	assert.Equal(t, "data1data2data3", tc.Chain.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncerWriter.Stop(ctx))

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"sync is enabled, mode=cache, sync each {count=100 or uncompressed=128KB or compressed=128KB or interval=10ms}, check each 1ms"}
{"level":"info","message":"TEST: write \"data1\""}
{"level":"info","message":"TEST: write \"data2\""}
{"level":"info","message":"TEST: write \"data3\""}
{"level":"debug","message":"starting sync to cache"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to cache done"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to cache"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to cache done"}
{"level":"debug","message":"syncer stopped"}
`)
}

// TestSyncWriter_SyncToCache_NoWait_Err tests that w.Notifier().Wait() doesn't block if SyncConfig.SyncWait = false.
// The flush error is logged.
func TestSyncWriter_SyncToCache_NoWait_Err(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeCache
	tc.Config.Wait = false
	tc.Chain.FlushError = errors.New("some flush error")

	syncerWriter := tc.NewSyncerWriter()

	// Write data
	// Waiting for sync is disabled, so writes are not blocked
	for i := 1; i <= 3; i++ {
		syncerWriter.MustWriteTestData(t, i)

		notifier := syncerWriter.Notifier()
		assert.Nil(t, notifier)
		assert.NoError(t, notifier.WaitWithTimeout(testWaitTimeout))
	}

	// Sync
	err := syncerWriter.TriggerSync(ctx, false).Wait()
	if assert.Error(t, err) {
		assert.Equal(t, "some flush error", err.Error())
	}

	// Check output
	assert.Equal(t, "data1data2data3", tc.Chain.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	err = syncerWriter.Stop(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "some flush error", err.Error())
	}

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"sync is enabled, mode=cache, sync each {count=100 or uncompressed=128KB or compressed=128KB or interval=10ms}, check each 1ms"}
{"level":"info","message":"TEST: write \"data1\""}
{"level":"info","message":"TEST: write \"data2\""}
{"level":"info","message":"TEST: write \"data3\""}
{"level":"debug","message":"starting sync to cache"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"error","message":"sync to cache failed: some flush error"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to cache"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"error","message":"sync to cache failed: some flush error"}
{"level":"debug","message":"syncer stopped"}
`)
}

// TestSyncWriter_WriteDuringSync tests write operations during sync in progress.
func TestSyncWriter_WriteDuringSync(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeDisk
	tc.Config.Wait = true

	syncerWriter := tc.NewSyncerWriter()

	// Write data
	wg := &sync.WaitGroup{}
	for i := 1; i <= 3; i++ {
		syncerWriter.MustWriteTestData(t, i)

		notifier := syncerWriter.Notifier()
		assert.NotNil(t, notifier)

		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, notifier.WaitWithTimeout(testWaitTimeout))
			tc.Logger.Info(ctx, "TEST: sync wait unblocked")
		}()
	}

	// Block sync completion
	tc.Chain.SyncLock.Lock()

	// Trigger sync
	syncerWriter.TriggerSync(ctx, false)

	// Wait for sync start
	assert.Eventually(t, func() bool {
		return tc.Logger.CompareJSONMessages(`{"level":"info","message":"TEST: sync started"}`) == nil
	}, time.Second, 10*time.Millisecond)

	// Write more data
	for i := 4; i <= 7; i++ {
		syncerWriter.MustWriteTestData(t, i)
	}

	// Unlock sync
	tc.Chain.SyncLock.Unlock()

	// Wait for log messages
	wg.Wait()

	// Check output
	assert.Equal(t, "data1data2data3data4data5data6data7", tc.Chain.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncerWriter.Stop(ctx))

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"sync is enabled, mode=disk, sync each {count=100 or uncompressed=128KB or compressed=128KB or interval=10ms}, check each 1ms"}
{"level":"info","message":"TEST: write \"data1\""}
{"level":"info","message":"TEST: write \"data2\""}
{"level":"info","message":"TEST: write \"data3\""}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: write \"data4\""}
{"level":"info","message":"TEST: write \"data5\""}
{"level":"info","message":"TEST: write \"data6\""}
{"level":"info","message":"TEST: write \"data7\""}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"syncer stopped"}
`)
}

// TestSyncWriter_OnlyOneRunningSync tests that sync runs only once, if it is triggered multiple times.
func TestSyncWriter_OnlyOneRunningSync(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Config.Wait = true

	syncerWriter := tc.NewSyncerWriter()

	// Write data
	wg := &sync.WaitGroup{}
	for i := 1; i <= 3; i++ {
		syncerWriter.MustWriteTestData(t, i)

		notifier := syncerWriter.Notifier()
		assert.NotNil(t, notifier)

		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, notifier.WaitWithTimeout(testWaitTimeout))
			tc.Logger.Info(ctx, "TEST: sync wait unblocked")
		}()
	}

	// Trigger sync multiple times, but it should run only once
	go func() {
		tc.Chain.SyncLock.Lock() // block sync completion
		for i := 0; i < 20; i++ {
			syncerWriter.TriggerSync(ctx, false)
		}
		tc.Chain.SyncLock.Unlock()
	}()

	// Wait for sync
	wg.Wait()

	// Check output
	assert.Equal(t, "data1data2data3", tc.Chain.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncerWriter.Stop(ctx))

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"sync is enabled, mode=disk, sync each {count=100 or uncompressed=128KB or compressed=128KB or interval=10ms}, check each 1ms"}
{"level":"info","message":"TEST: write \"data1\""}
{"level":"info","message":"TEST: write \"data2\""}
{"level":"info","message":"TEST: write \"data3\""}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"syncer stopped"}
`)
}

func TestSyncWriter_CountTrigger(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Config.Wait = true

	syncerWriter := tc.NewSyncerWriter()

	// Write data
	wg := &sync.WaitGroup{}
	for i := 1; i <= 3; i++ {
		syncerWriter.MustWriteTestData(t, i)

		notifier := syncerWriter.Notifier()
		assert.NotNil(t, notifier)

		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, notifier.WaitWithTimeout(testWaitTimeout))
			tc.Logger.Info(ctx, "TEST: sync wait unblocked")
		}()
	}

	// Trigger sync by the records count
	syncerWriter.Counter.Add(tc.Clock.Now(), uint64(tc.Config.CountTrigger)-syncerWriter.Counter.Count())
	tc.Clock.Add(tc.Config.CheckInterval.Duration())
	wg.Wait()

	// Check output
	assert.Equal(t, "data1data2data3", tc.Chain.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncerWriter.Stop(ctx))

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"sync is enabled, mode=disk, sync each {count=100 or uncompressed=128KB or compressed=128KB or interval=10ms}, check each 1ms"}
{"level":"info","message":"TEST: write \"data1\""}
{"level":"info","message":"TEST: write \"data2\""}
{"level":"info","message":"TEST: write \"data3\""}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"syncer stopped"}
`)
}

func TestSyncWriter_IntervalTrigger(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Config.Wait = true

	syncerWriter := tc.NewSyncerWriter()

	// Write data
	wg := &sync.WaitGroup{}
	for i := 1; i <= 3; i++ {
		syncerWriter.MustWriteTestData(t, i)

		notifier := syncerWriter.Notifier()
		assert.NotNil(t, notifier)

		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, notifier.WaitWithTimeout(testWaitTimeout))
			tc.Logger.Info(ctx, "TEST: sync wait unblocked")
		}()
	}

	// Wait for sync
	tc.Clock.Add(tc.Config.IntervalTrigger.Duration())
	wg.Wait()

	// Check output
	assert.Equal(t, "data1data2data3", tc.Chain.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncerWriter.Stop(ctx))

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"sync is enabled, mode=disk, sync each {count=100 or uncompressed=128KB or compressed=128KB or interval=10ms}, check each 1ms"}
{"level":"info","message":"TEST: write \"data1\""}
{"level":"info","message":"TEST: write \"data2\""}
{"level":"info","message":"TEST: write \"data3\""}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"syncer stopped"}
`)
}

func TestSyncWriter_BytesTrigger(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Config.Wait = true

	syncerWriter := tc.NewSyncerWriter()
	wg := &sync.WaitGroup{}

	// Write 10 bytes
	data1 := "1234567890"
	n1, err1 := syncerWriter.Write([]byte(data1))
	assert.Equal(t, 10, n1)

	notifier1 := syncerWriter.Notifier()
	assert.NotNil(t, notifier1)
	assert.NoError(t, err1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, notifier1.WaitWithTimeout(testWaitTimeout))
		tc.Logger.Info(ctx, "TEST: sync wait unblocked")
	}()

	// Write data over the limit + wait for sync
	data2Len := int(tc.Config.UncompressedBytesTrigger - datasize.ByteSize(10))
	data2 := strings.Repeat("-", data2Len)
	n2, err2 := syncerWriter.Write([]byte(data2))
	assert.Equal(t, data2Len, n2)
	notifier2 := syncerWriter.Notifier()
	assert.NotNil(t, notifier2)
	assert.NoError(t, err2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, notifier2.WaitWithTimeout(testWaitTimeout))
		tc.Logger.Info(ctx, "TEST: sync wait unblocked")
	}()

	// Wait for sync
	tc.Clock.Add(tc.Config.CheckInterval.Duration())
	wg.Wait()

	// Check output
	assert.Equal(t, data1+data2, tc.Chain.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncerWriter.Stop(ctx))

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"sync is enabled, mode=disk, sync each {count=100 or uncompressed=128KB or compressed=128KB or interval=10ms}, check each 1ms"}
{"level":"info","message":"TEST: write \"1234567890\""}
{"level":"info","message":"TEST: write \"--------------------...\""}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"info","message":"TEST: sync wait unblocked"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to disk"}
{"level":"info","message":"TEST: sync started"}
{"level":"info","message":"TEST: sync done"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"syncer stopped"}
`)
}

type writerTestCase struct {
	TB     testing.TB
	Ctx    context.Context
	Logger log.DebugLogger
	Clock  *clock.Mock
	Chain  *testChain
	Config Config
}

type testChain struct {
	Logger     log.DebugLogger
	Buffer     bytes.Buffer
	SyncLock   *sync.Mutex
	WriteError error
	FlushError error
	SyncError  error
}

type testSyncerWriter struct {
	*Syncer
	io.Writer
	BytesMeter *size.Meter
	Counter    *count.Counter
	tc         *writerTestCase
}

func (c *testChain) Write(p []byte) (int, error) {
	c.Logger.Infof(context.Background(), `TEST: write "%s"`, strhelper.Truncate(string(p), 20, "..."))
	if c.WriteError != nil {
		return 0, c.WriteError
	}
	return c.Buffer.Write(p)
}

func (c *testChain) WriteString(s string) (int, error) {
	c.Logger.Infof(context.Background(), `TEST: write "%s"`, strhelper.Truncate(s, 20, "..."))
	if c.WriteError != nil {
		return 0, c.WriteError
	}
	return c.Buffer.WriteString(s)
}

func (c *testChain) Flush(ctx context.Context) error {
	c.Logger.Infof(ctx, `TEST: sync started`)
	c.SyncLock.Lock()
	time.Sleep(5 * time.Millisecond)
	c.SyncLock.Unlock()
	c.Logger.Infof(ctx, `TEST: sync done`)
	return c.FlushError
}

func (c *testChain) Sync(ctx context.Context) error {
	c.Logger.Infof(ctx, `TEST: sync started`)
	c.SyncLock.Lock()
	time.Sleep(5 * time.Millisecond)
	c.SyncLock.Unlock()
	c.Logger.Infof(ctx, `TEST: sync done`)
	return c.SyncError
}

func newWriterTestCase(tb testing.TB) *writerTestCase {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	tb.Cleanup(func() {
		cancel()
	})

	// Define sync config for the test, it must be valid
	config := Config{
		Mode:                     ModeDisk,
		Wait:                     true,
		CheckInterval:            duration.From(1 * time.Millisecond),
		CountTrigger:             100,
		UncompressedBytesTrigger: 128 * datasize.KB,
		CompressedBytesTrigger:   128 * datasize.KB,
		IntervalTrigger:          duration.From(10 * time.Millisecond),
	}
	val := validator.New()
	require.NoError(tb, val.Validate(ctx, config))

	logger := log.NewDebugLogger()
	return &writerTestCase{
		TB:     tb,
		Ctx:    ctx,
		Logger: logger,
		Clock:  clock.NewMock(),
		Chain: &testChain{
			Logger:   logger,
			SyncLock: &sync.Mutex{},
		},
		Config: config,
	}
}

func (tc *writerTestCase) NewSyncerWriter() *testSyncerWriter {
	w := &testSyncerWriter{tc: tc}
	w.BytesMeter = size.NewMeter(tc.Chain)
	w.Counter = count.NewCounter()
	w.Writer = w.BytesMeter
	w.Syncer = NewSyncer(tc.Ctx, tc.Logger, tc.Clock, tc.Config, tc.Chain, w)
	return w
}

func (tc *writerTestCase) AssertLogs(expected string) bool {
	return tc.Logger.AssertJSONMessages(tc.TB, expected)
}

func (w *testSyncerWriter) Write(p []byte) (n int, err error) {
	n, err = w.Writer.Write(p)
	if err == nil {
		w.Counter.Add(w.tc.Clock.Now(), 1)
	}
	return n, err
}

func (w *testSyncerWriter) MustWriteTestData(t *testing.T, i int) {
	t.Helper()
	n, err := w.Write([]byte(fmt.Sprintf(`data%d`, i)))
	assert.Equal(t, 5, n)
	assert.NoError(t, err)
}

// AcceptedWrites returns count of write operations waiting for the sync.
func (w *testSyncerWriter) AcceptedWrites() uint64 {
	return w.Counter.Count()
}

// CompressedSize written to the file, measured after compression writer.
func (w *testSyncerWriter) CompressedSize() datasize.ByteSize {
	return w.BytesMeter.Size()
}

// UncompressedSize written to the file, measured before compression writer.
func (w *testSyncerWriter) UncompressedSize() datasize.ByteSize {
	return w.BytesMeter.Size()
}
