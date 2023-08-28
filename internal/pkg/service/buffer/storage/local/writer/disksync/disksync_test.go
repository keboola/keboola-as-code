package disksync

import (
	"bytes"
	"context"
	"fmt"
	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/notify"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/stretchr/testify/assert"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewSyncWriter_ModeDisabled(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeDisabled
	assert.Nil(t, tc.NewSyncer())

	// Check logs
	tc.AssertLogs(`
INFO  sync is disabled
`)
}

func TestNewSyncWriter_ModeInvalid(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Config.Mode = "invalid"
	assert.Panics(t, func() {
		tc.Config.IntervalTrigger = 0
		tc.NewSyncer()
	})
}

// TestNewSyncWriter_NoSync tests that Config.IntervalTrigger field must be > 0.
func TestNewSyncWriter_NoIntervalTrigger(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	assert.Panics(t, func() {
		tc.Config.IntervalTrigger = 0
		tc.NewSyncer()
	})
}

// TestNewSyncWriter_NoSync tests that Config.BytesTrigger field must be > 0.
func TestNewSyncWriter_NoBytesTrigger(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	assert.Panics(t, func() {
		tc.Config.BytesTrigger = 0
		tc.NewSyncer()
	})
}

func TestSyncWriter_WriteToClosedWriter(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	syncer := tc.NewSyncer()

	// Close the writer
	assert.NoError(t, syncer.Close())

	// Try Write
	n, err := syncer.Write([]byte("foo"))
	assert.Equal(t, 0, n)
	if assert.Error(t, err) {
		assert.Equal(t, "syncer is closed: context canceled", err.Error())
	}
}

func TestSyncWriter_WriteStringToClosedWriter(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	syncer := tc.NewSyncer()

	// Close the writer
	assert.NoError(t, syncer.Close())

	// Try WriteString
	n, err := syncer.WriteString("foo")
	assert.Equal(t, 0, n)
	if assert.Error(t, err) {
		assert.Equal(t, "syncer is closed: context canceled", err.Error())
	}
}

func TestSyncWriter_Write_Error(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Writer.WriteError = errors.New("some write error")
	syncer := tc.NewSyncer()

	// Try Write
	n, err := syncer.Write([]byte("foo"))
	assert.Equal(t, 0, n)
	if assert.Error(t, err) {
		assert.Equal(t, "some write error", err.Error())
	}
}

func TestSyncWriter_WriteString_Error(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Writer.WriteError = errors.New("some write error")
	syncer := tc.NewSyncer()

	// Try WriteString
	n, err := syncer.WriteString("foo")
	assert.Equal(t, 0, n)
	if assert.Error(t, err) {
		assert.Equal(t, "some write error", err.Error())
	}
}

func TestSyncWriter_SkipEmptySync(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Config.Wait = true

	syncer := tc.NewSyncer()

	// Wait for sync
	assert.NoError(t, waitForSync(t, tc, syncer.Notifier(), "sync wait unblocked", func() { syncer.Sync() }))

	// Check output
	assert.Equal(t, "", tc.Writer.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncer.Close())

	// Check logs
	tc.AssertLogs(`
INFO  sync is enabled, mode=disk, sync each "100ms" or "128KB"
DEBUG  nothing to sync
INFO  TEST: sync wait unblocked
DEBUG  closing syncer
DEBUG  nothing to sync
DEBUG  syncer closed
`)
}

// TestSyncWriter_SyncToDisk_Wait_Ok tests that w.Notifier().Wait() blocks if SyncConfig.SyncWait = true.
// Sync operation is successful.
func TestSyncWriter_SyncToDisk_Wait_Ok(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeDisk
	tc.Config.Wait = true

	syncer := tc.NewSyncer()
	assert.NotNil(t, syncer.Notifier())

	// Write data
	notifier := syncer.DoWithNotifier(func() {
		for i := 1; i <= 3; i++ {
			n, err := syncer.Write([]byte(fmt.Sprintf(`data%d`, i)))
			assert.Equal(t, 5, n)
			assert.NoError(t, err)
		}
	})
	assert.NotNil(t, notifier)

	// Wait for sync 1
	assert.NoError(t, waitForSync(t, tc, notifier, `sync wait unblocked 1`, func() { syncer.Sync() }))

	// Write data
	notifier = syncer.DoWithNotifier(func() {
		for i := 4; i <= 6; i++ {
			n, err := syncer.WriteString(fmt.Sprintf(`data%d`, i))
			assert.Equal(t, 5, n)
			assert.NoError(t, err)
		}
	})

	// Wait for sync 2
	assert.NoError(t, waitForSync(t, tc, notifier, `sync wait unblocked 2`, func() { syncer.Sync() }))

	// Check output
	assert.Equal(t, "data1data2data3data4data5data6", tc.Writer.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncer.Close())

	// Check logs
	tc.AssertLogs(`
INFO  sync is enabled, mode=disk, sync each "100ms" or "128KB"
INFO  TEST: write "data1"
INFO  TEST: write "data2"
INFO  TEST: write "data3"
DEBUG  starting sync of "15 B" to disk
INFO  TEST: sync started
INFO  TEST: sync done
DEBUG  sync to disk done
INFO  TEST: sync wait unblocked 1
INFO  TEST: write "data4"
INFO  TEST: write "data5"
INFO  TEST: write "data6"
DEBUG  starting sync of "15 B" to disk
INFO  TEST: sync started
INFO  TEST: sync done
DEBUG  sync to disk done
INFO  TEST: sync wait unblocked 2
DEBUG  closing syncer
DEBUG  nothing to sync
DEBUG  syncer closed
`)
}

// TestSyncWriter_SyncToDisk_Wait_Error tests that w.Notifier().Wait() blocks if SyncConfig.SyncWait = true.
// The sync error is returned by the Wait() method.
func TestSyncWriter_SyncToDisk_Wait_Error(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeDisk
	tc.Config.Wait = true
	tc.SyncError = errors.New("some sync error")

	syncer := tc.NewSyncer()

	// Write data
	notifier := syncer.DoWithNotifier(func() {
		for i := 1; i <= 3; i++ {
			n, err := syncer.Write([]byte(fmt.Sprintf(`data%d`, i)))
			assert.Equal(t, 5, n)
			assert.NoError(t, err)
		}
	})

	// Wait for sync
	err := waitForSync(t, tc, notifier, "sync wait unblocked", func() { syncer.Sync() })
	if assert.Error(t, err) {
		assert.Equal(t, "some sync error", err.Error())
	}

	// Check output
	assert.Equal(t, "data1data2data3", tc.Writer.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncer.Close())

	// Check logs
	tc.AssertLogs(`
INFO  sync is enabled, mode=disk, sync each "100ms" or "128KB"
INFO  TEST: write "data1"
INFO  TEST: write "data2"
INFO  TEST: write "data3"
DEBUG  starting sync of "15 B" to disk
INFO  TEST: sync started
INFO  TEST: sync done
ERROR  sync to disk failed: some sync error
INFO  TEST: sync wait unblocked
DEBUG  closing syncer
DEBUG  nothing to sync
DEBUG  syncer closed
`)
}

// TestSyncWriter_SyncToDisk_NoWait_Ok tests that w.Notifier().Wait() doesn't block if SyncConfig.SyncWait = false.
// The sync operation is successful.
func TestSyncWriter_SyncToDisk_NoWait_Ok(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeDisk
	tc.Config.Wait = false

	syncer := tc.NewSyncer()
	assert.Nil(t, syncer.Notifier())

	// Write data
	notifier := syncer.DoWithNotifier(func() {
		for i := 1; i <= 3; i++ {
			n, err := syncer.Write([]byte(fmt.Sprintf(`data%d`, i)))
			assert.Equal(t, 5, n)
			assert.NoError(t, err)
		}
	})
	assert.Nil(t, notifier)

	// Waiting for sync is disabled = no operation
	assert.NoError(t, notifier.Wait())
	syncer.SyncAndWait()

	// Check output
	assert.Equal(t, "data1data2data3", tc.Writer.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncer.Close())

	// Check logs
	tc.AssertLogs(`
INFO  sync is enabled, mode=disk, sync each "100ms" or "128KB"
INFO  TEST: write "data1"
INFO  TEST: write "data2"
INFO  TEST: write "data3"
DEBUG  starting sync of "15 B" to disk
INFO  TEST: sync started
INFO  TEST: sync done
DEBUG  sync to disk done
DEBUG  closing syncer
DEBUG  nothing to sync
DEBUG  syncer closed
`)
}

// TestSyncWriter_SyncToDisk_NoWait_Error tests that w.Notifier().Wait() doesn't block if SyncConfig.SyncWait = false.
// The sync error is logged.
func TestSyncWriter_SyncToDisk_NoWait_Error(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeDisk
	tc.Config.Wait = false
	tc.SyncError = errors.New("some sync error")

	syncer := tc.NewSyncer()

	// Write data
	notifier := syncer.DoWithNotifier(func() {
		for i := 1; i <= 3; i++ {
			n, err := syncer.Write([]byte(fmt.Sprintf(`data%d`, i)))
			assert.Equal(t, 5, n)
			assert.NoError(t, err)
		}
	})

	// Waiting for sync is disabled = no operation
	assert.NoError(t, notifier.Wait())
	syncer.SyncAndWait()

	// Check output
	assert.Equal(t, "data1data2data3", tc.Writer.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncer.Close())

	// Check logs
	tc.AssertLogs(`
INFO  sync is enabled, mode=disk, sync each "100ms" or "128KB"
INFO  TEST: write "data1"
INFO  TEST: write "data2"
INFO  TEST: write "data3"
DEBUG  starting sync of "15 B" to disk
INFO  TEST: sync started
INFO  TEST: sync done
ERROR  sync to disk failed: some sync error
DEBUG  closing syncer
DEBUG  nothing to sync
DEBUG  syncer closed
`)
}

// TestSyncWriter_SyncToCache_Wait_Ok tests that w.Notifier().Wait() blocks if SyncConfig.SyncWait = true.
// The flush operation is successful.
func TestSyncWriter_SyncToCache_Wait_Ok(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeCache
	tc.Config.Wait = true

	syncer := tc.NewSyncer()
	assert.NotNil(t, syncer.Notifier())

	// Write data
	notifier := syncer.DoWithNotifier(func() {
		for i := 1; i <= 3; i++ {
			n, err := syncer.Write([]byte(fmt.Sprintf(`data%d`, i)))
			assert.Equal(t, 5, n)
			assert.NoError(t, err)
		}
	})
	assert.NotNil(t, notifier)

	// Wait for sync 1
	assert.NoError(t, waitForSync(t, tc, notifier, "flush wait unblocked 1", func() { syncer.Sync() }))

	// Write data
	notifier = syncer.DoWithNotifier(func() {
		for i := 4; i <= 6; i++ {
			n, err := syncer.WriteString(fmt.Sprintf(`data%d`, i))
			assert.Equal(t, 5, n)
			assert.NoError(t, err)
		}
	})

	// Wait for sync 2
	assert.NoError(t, waitForSync(t, tc, notifier, "flush wait unblocked 2", func() { syncer.Sync() }))

	// Check output
	assert.Equal(t, "data1data2data3data4data5data6", tc.Writer.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncer.Close())

	// Check logs
	tc.AssertLogs(`
INFO  sync is enabled, mode=cache, sync each "100ms" or "128KB"
INFO  TEST: write "data1"
INFO  TEST: write "data2"
INFO  TEST: write "data3"
DEBUG  starting sync of "15 B" to cache
INFO  TEST: sync started
INFO  TEST: sync done
DEBUG  sync to cache done
INFO  TEST: flush wait unblocked 1
INFO  TEST: write "data4"
INFO  TEST: write "data5"
INFO  TEST: write "data6"
DEBUG  starting sync of "15 B" to cache
INFO  TEST: sync started
INFO  TEST: sync done
DEBUG  sync to cache done
INFO  TEST: flush wait unblocked 2
DEBUG  closing syncer
DEBUG  nothing to sync
DEBUG  syncer closed
`)
}

// TestSyncWriter_SyncToCache_Wait_Error tests that w.Notifier().Wait() blocks if SyncConfig.SyncWait = true.
// The flush error is returned by the Wait() method.
func TestSyncWriter_SyncToCache_Wait_Error(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeCache
	tc.Config.Wait = true
	tc.FlushError = errors.New("some flush error")

	syncer := tc.NewSyncer()

	// Write data
	notifier := syncer.DoWithNotifier(func() {
		for i := 1; i <= 3; i++ {
			n, err := syncer.Write([]byte(fmt.Sprintf(`data%d`, i)))
			assert.Equal(t, 5, n)
			assert.NoError(t, err)
		}
	})

	// Wait for sync
	err := waitForSync(t, tc, notifier, "flush wait unblocked", func() { syncer.Sync() })
	if assert.Error(t, err) {
		assert.Equal(t, "some flush error", err.Error())
	}

	// Check output
	assert.Equal(t, "data1data2data3", tc.Writer.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncer.Close())

	// Check logs
	tc.AssertLogs(`
INFO  sync is enabled, mode=cache, sync each "100ms" or "128KB"
INFO  TEST: write "data1"
INFO  TEST: write "data2"
INFO  TEST: write "data3"
DEBUG  starting sync of "15 B" to cache
INFO  TEST: sync started
INFO  TEST: sync done
ERROR  sync to cache failed: some flush error
INFO  TEST: flush wait unblocked
DEBUG  closing syncer
DEBUG  nothing to sync
DEBUG  syncer closed
`)
}

// TestSyncWriter_SyncToCache_NoWait_Ok tests that w.Notifier().Wait() doesn't block if SyncConfig.SyncWait = false.
// The flush operation is successful.
func TestSyncWriter_SyncToCache_NoWait_Ok(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeCache
	tc.Config.Wait = false

	syncer := tc.NewSyncer()
	assert.Nil(t, syncer.Notifier())

	// Write data
	notifier := syncer.DoWithNotifier(func() {
		for i := 1; i <= 3; i++ {
			n, err := syncer.Write([]byte(fmt.Sprintf(`data%d`, i)))
			assert.Equal(t, 5, n)
			assert.NoError(t, err)
		}
	})
	assert.Nil(t, notifier)

	// Waiting for sync is disabled = no operation
	assert.NoError(t, notifier.Wait())
	syncer.SyncAndWait()

	// Check output
	assert.Equal(t, "data1data2data3", tc.Writer.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncer.Close())

	// Check logs
	tc.AssertLogs(`
INFO  sync is enabled, mode=cache, sync each "100ms" or "128KB"
INFO  TEST: write "data1"
INFO  TEST: write "data2"
INFO  TEST: write "data3"
DEBUG  starting sync of "15 B" to cache
INFO  TEST: sync started
INFO  TEST: sync done
DEBUG  sync to cache done
DEBUG  closing syncer
DEBUG  nothing to sync
DEBUG  syncer closed
`)
}

// TestSyncWriter_SyncToCache_NoWait_Err tests that w.Notifier().Wait() doesn't block if SyncConfig.SyncWait = false.
// The flush error is logged.
func TestSyncWriter_SyncToCache_NoWait_Err(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeCache
	tc.Config.Wait = false
	tc.FlushError = errors.New("some flush error")

	syncer := tc.NewSyncer()

	// Write data
	notifier := syncer.DoWithNotifier(func() {
		for i := 1; i <= 3; i++ {
			n, err := syncer.Write([]byte(fmt.Sprintf(`data%d`, i)))
			assert.Equal(t, 5, n)
			assert.NoError(t, err)
		}
	})

	// Waiting for sync is disabled = no operation
	assert.NoError(t, notifier.Wait())
	syncer.SyncAndWait()

	// Check output
	assert.Equal(t, "data1data2data3", tc.Writer.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncer.Close())

	// Check logs
	tc.AssertLogs(`
INFO  sync is enabled, mode=cache, sync each "100ms" or "128KB"
INFO  TEST: write "data1"
INFO  TEST: write "data2"
INFO  TEST: write "data3"
DEBUG  starting sync of "15 B" to cache
INFO  TEST: sync started
INFO  TEST: sync done
ERROR  sync to cache failed: some flush error
DEBUG  closing syncer
DEBUG  nothing to sync
DEBUG  syncer closed
`)
}

// TestSyncWriter_WriteDuringSync tests write operations during sync in progress.
func TestSyncWriter_WriteDuringSync(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Config.Mode = ModeDisk
	tc.Config.Wait = true

	syncer := tc.NewSyncer()

	// Write data
	notifier := syncer.DoWithNotifier(func() {
		for i := 1; i <= 3; i++ {
			n, err := syncer.Write([]byte(fmt.Sprintf(`data%d`, i)))
			assert.Equal(t, 5, n)
			assert.NoError(t, err)
		}
	})

	// Wait for sync
	assert.NoError(t, waitForSync(t, tc, notifier, "sync wait unblocked", func() {
		// Block sync
		tc.SyncLock.Lock()

		// Trigger sync
		syncer.Sync()

		// Wait for sync start
		assert.Eventually(t, func() bool {
			return strings.Contains(tc.Logger.AllMessages(), `starting sync of "15 B"`)
		}, time.Second, 10*time.Millisecond)

		// Write more data
		for i := 4; i <= 7; i++ {
			n, err := syncer.Write([]byte(fmt.Sprintf(`data%d`, i)))
			assert.Equal(t, 5, n)
			assert.NoError(t, err)
		}

		// Unlock sync
		tc.SyncLock.Unlock()
	}))

	// Check output
	assert.Equal(t, "data1data2data3data4data5data6data7", tc.Writer.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncer.Close())

	// Check logs
	tc.AssertLogs(`
INFO  sync is enabled, mode=disk, sync each "100ms" or "128KB"
INFO  TEST: write "data1"
INFO  TEST: write "data2"
INFO  TEST: write "data3"
DEBUG  starting sync of "15 B" to disk
INFO  TEST: sync started
INFO  TEST: write "data4"
INFO  TEST: write "data5"
INFO  TEST: write "data6"
INFO  TEST: write "data7"
INFO  TEST: sync done
DEBUG  sync to disk done
INFO  TEST: sync wait unblocked
DEBUG  closing syncer
DEBUG  starting sync of "20 B" to disk
INFO  TEST: sync started
INFO  TEST: sync done
DEBUG  sync to disk done
DEBUG  syncer closed
`)
}

// TestSyncWriter_OnlyOneRunningSync tests that sync runs only once, if it is triggered multiple times.
func TestSyncWriter_OnlyOneRunningSync(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Config.Wait = true

	syncer := tc.NewSyncer()

	// Write data
	notifier := syncer.DoWithNotifier(func() {
		for i := 1; i <= 3; i++ {
			n, err := syncer.Write([]byte(fmt.Sprintf(`data%d`, i)))
			assert.Equal(t, 5, n)
			assert.NoError(t, err)
		}
	})

	// Trigger sync multiple times, but it should run only once
	go func() {
		for i := 0; i < 20; i++ {
			syncer.Sync()
		}
	}()

	// Wait for sync
	assert.NoError(t, notifier.Wait())

	// Check output
	assert.Equal(t, "data1data2data3", tc.Writer.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncer.Close())

	// Check logs
	tc.AssertLogs(`
INFO  sync is enabled, mode=disk, sync each "100ms" or "128KB"
INFO  TEST: write "data1"
INFO  TEST: write "data2"
INFO  TEST: write "data3"
DEBUG  starting sync of "15 B" to disk
INFO  TEST: sync started
INFO  TEST: sync done
DEBUG  sync to disk done
DEBUG  closing syncer
DEBUG  nothing to sync
DEBUG  syncer closed
`)
}

func TestSyncWriter_SyncAfterInterval(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Config.Wait = true

	syncer := tc.NewSyncer()

	// Write data
	notifier := syncer.DoWithNotifier(func() {
		for i := 1; i <= 3; i++ {
			n, err := syncer.Write([]byte(fmt.Sprintf(`data%d`, i)))
			assert.Equal(t, 5, n)
			assert.NoError(t, err)
		}
	})

	// Wait for sync
	assert.NoError(t, waitForSync(t, tc, notifier, "sync wait unblocked", func() { tc.Clock.Add(tc.Config.IntervalTrigger) }))

	// Check output
	assert.Equal(t, "data1data2data3", tc.Writer.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncer.Close())

	// Check logs
	tc.AssertLogs(`
INFO  sync is enabled, mode=disk, sync each "100ms" or "128KB"
INFO  TEST: write "data1"
INFO  TEST: write "data2"
INFO  TEST: write "data3"
DEBUG  starting sync of "15 B" to disk
INFO  TEST: sync started
INFO  TEST: sync done
DEBUG  sync to disk done
INFO  TEST: sync wait unblocked
DEBUG  closing syncer
DEBUG  nothing to sync
DEBUG  syncer closed
`)
}

func TestSyncWriter_SyncAfterBytes(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	tc.Config.Wait = true

	syncer := tc.NewSyncer()

	// Write 10 bytes
	data1 := "1234567890"
	notifier := syncer.DoWithNotifier(func() {
		n, err := syncer.Write([]byte(data1))
		assert.Equal(t, 10, n)
		assert.NoError(t, err)
	})

	// Write data over the limit + wait for sync
	data2Len := int(tc.Config.BytesTrigger - datasize.ByteSize(10))
	data2 := strings.Repeat("-", data2Len)
	assert.NoError(t, waitForSync(t, tc, notifier, "sync wait unblocked", func() {
		n, err := syncer.Write([]byte(data2))
		assert.Equal(t, data2Len, n)
		assert.NoError(t, err)
	}))

	// Check output
	assert.Equal(t, data1+data2, tc.Writer.Buffer.String())

	// Close the syncWriter - it triggers the last sync
	assert.NoError(t, syncer.Close())

	// Check logs
	tc.AssertLogs(`
INFO  sync is enabled, mode=disk, sync each "100ms" or "128KB"
INFO  TEST: write "1234567890"
INFO  TEST: write "--------------------..."
DEBUG  starting sync of "128.0 KB" to disk
INFO  TEST: sync started
INFO  TEST: sync done
DEBUG  sync to disk done
INFO  TEST: sync wait unblocked
DEBUG  closing syncer
DEBUG  nothing to sync
DEBUG  syncer closed
`)
}

type writerTestCase struct {
	T          testing.TB
	Ctx        context.Context
	Logger     log.DebugLogger
	Clock      *clock.Mock
	Writer     *testWriter
	SyncLock   *sync.Mutex
	SyncError  error
	FlushError error
	Config     Config
}

type testWriter struct {
	Logger     log.DebugLogger
	Buffer     bytes.Buffer
	WriteError error
}

type testChain struct {
	tc *writerTestCase
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.Logger.Infof(`TEST: write "%s"`, strhelper.Truncate(string(p), 20, "..."))
	if w.WriteError != nil {
		return 0, w.WriteError
	}
	return w.Buffer.Write(p)
}

func (w *testWriter) WriteString(s string) (int, error) {
	w.Logger.Infof(`TEST: write "%s"`, strhelper.Truncate(s, 20, "..."))
	if w.WriteError != nil {
		return 0, w.WriteError
	}
	return w.Buffer.WriteString(s)
}

func (s *testChain) Flush() error {
	s.tc.Logger.Infof(`TEST: sync started`)
	s.tc.SyncLock.Lock()
	time.Sleep(5 * time.Millisecond)
	s.tc.SyncLock.Unlock()
	s.tc.Logger.Infof(`TEST: sync done`)
	return s.tc.FlushError
}

func (s *testChain) Sync() error {
	s.tc.Logger.Infof(`TEST: sync started`)
	s.tc.SyncLock.Lock()
	time.Sleep(5 * time.Millisecond)
	s.tc.SyncLock.Unlock()
	s.tc.Logger.Infof(`TEST: sync done`)
	return s.tc.SyncError
}

func waitForSync(t *testing.T, tc *writerTestCase, notifier *notify.Notifier, msg string, trigger func()) error {
	// Wait for the next sync in the goroutine
	errCh := make(chan error)
	go func() {
		err := notifier.Wait()
		tc.Logger.Infof(`TEST: %s`, msg)
		errCh <- err
		close(errCh)
	}()

	// Trigger sync
	trigger()

	// Wait for the goroutine above
	select {
	case err := <-errCh:
		return err
	case <-time.After(10 * time.Second):
		assert.Fail(t, "timeout")
		return nil
	}
}

func newWriterTestCase(t testing.TB) *writerTestCase {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(func() {
		cancel()
	})

	t.Helper()
	logger := log.NewDebugLogger()
	return &writerTestCase{
		T:         t,
		Ctx:       ctx,
		Logger:    logger,
		Clock:     clock.NewMock(),
		Writer:    &testWriter{Logger: logger},
		SyncLock:  &sync.Mutex{},
		SyncError: nil,
		Config:    DefaultConfig(),
	}
}

func (tc *writerTestCase) NewSyncer() *Syncer {
	return NewSyncer(tc.Ctx, tc.Logger, tc.Clock, tc.Config, tc.Writer, &testChain{tc: tc})
}

func (tc *writerTestCase) AssertLogs(expected string) bool {
	return wildcards.Assert(tc.T, strings.TrimSpace(expected), strings.TrimSpace(tc.Logger.AllMessages()))
}
