package encoding_test

import (
	"bytes"
	"context"
	"github.com/benbjohnson/clock"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/manager"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writechain"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestWriter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := log.NewDebugLogger()
	clk := clock.New()
	dirPath := t.TempDir()
	filePath := filepath.Join(dirPath, "file")
	slice := test.NewSlice()
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	assert.NoError(t, err)

	cfg := encoding.NewConfig()
	cfg.Encoder.Factory = DummyEncoderFactory

	w, err := encoding.NewWriter(ctx, logger, clk, cfg, slice, file, events.New[encoding.Writer]())
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
	logger := log.NewDebugLogger()
	clk := clock.NewMock()
	dirPath := t.TempDir()
	filePath := filepath.Join(dirPath, "file")
	slice := test.NewSlice()
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	assert.NoError(t, err)

	cfg := encoding.NewConfig()
	cfg.Encoder.Factory = func(cfg encoder.Config, out io.Writer, slice *model.Slice) (encoder.Encoder, error) {
		w := NewDummyEncoder(cfg, out, slice, nil)
		w.FlushError = errors.New("some error")
		return w, nil
	}

	w, err := encoding.NewWriter(ctx, logger, clk, cfg, slice, file, events.New[encoding.Writer]())
	require.NoError(t, err)

	// Test Close method
	err = w.Close(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "chain sync error:\n- chain flush error:\n  - cannot flush \"*encoding_test.DummyEncoder\": some error", err.Error())
	}
}

func TestWriter_CloseError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := log.NewDebugLogger()
	clk := clock.NewMock()
	dirPath := t.TempDir()
	filePath := filepath.Join(dirPath, "file")
	slice := test.NewSlice()
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	assert.NoError(t, err)

	cfg := encoding.NewConfig()
	cfg.Encoder.Factory = func(cfg encoder.Config, out io.Writer, slice *model.Slice) (encoder.Encoder, error) {
		w := NewDummyEncoder(cfg, out, slice, nil)
		w.CloseError = errors.New("some error")
		return w, nil
	}

	w, err := encoding.NewWriter(ctx, logger, clk, cfg, slice, file, events.New[encoding.Writer]())
	require.NoError(t, err)

	// Test Close method
	err = w.Close(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "chain close error:\n- cannot close \"*encoding_test.DummyEncoder\": some error", err.Error())
	}
}

func TestVolume_OpenWriter_Ok(t *testing.T) {
	t.Parallel()
	tc := NewEncodingTestCase(t)

	w, err := tc.NewWriter()
	assert.NoError(t, err)
	assert.Len(t, tc.Manager.Writers(), 1)

	assert.NoError(t, w.Close(context.Background()))
	assert.Len(t, tc.Manager.Writers(), 0)
}

func TestVolume_OpenWriter_Duplicate(t *testing.T) {
	t.Parallel()
	tc := NewEncodingTestCase(t)

	// Create the writer first time - ok
	w, err := tc.NewWriter()
	assert.NoError(t, err)
	assert.Len(t, tc.Manager.Writers(), 1)

	// Create writer for the same slice again - error
	_, err = tc.NewWriter()
	if assert.Error(t, err) {
		assert.Equal(t, `writer for slice "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T20:00:00.000Z" already exists`, err.Error())
	}
	assert.Len(t, tc.Manager.Writers(), 1)

	assert.NoError(t, w.Close(context.Background()))
	assert.Len(t, tc.Manager.Writers(), 0)
}

func TestVolume_Writer_Sync_Enabled_Wait_ToDisk(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := NewEncodingTestCase(t)
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
	assert.Equal(t, strings.TrimSpace(`
foo,bar,123
foo,bar,123
abc,def,456
ghi,jkl,789
`), strings.TrimSpace(tc.Output.String()))

	// Check logs
	tc.AssertLogs(`
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
	tc := NewEncodingTestCase(t)
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
	assert.Equal(t, strings.TrimSpace(`
foo,bar,123
foo,bar,123
abc,def,456
ghi,jkl,789
`), strings.TrimSpace(tc.Output.String()))

	// Check logs
	tc.AssertLogs(`
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
	tc := NewEncodingTestCase(t)
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
	assert.Equal(t, strings.TrimSpace(`
foo,bar,123
foo,bar,123
abc,def,456
ghi,jkl,789
`), strings.TrimSpace(tc.Output.String()))

	// Check logs
	tc.AssertLogs(`
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
	tc := NewEncodingTestCase(t)
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
	assert.Equal(t, strings.TrimSpace(`
foo,bar,123
foo,bar,123
abc,def,456
ghi,jkl,789
`), strings.TrimSpace(tc.Output.String()))

	// Check logs
	tc.AssertLogs(`
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
	tc := NewEncodingTestCase(t)
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
	assert.Equal(t, strings.TrimSpace(`
foo,bar,123
foo,bar,123
abc,def,456
ghi,jkl,789
`), strings.TrimSpace(tc.Output.String()))

	// Check logs
	tc.AssertLogs(`
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

// EncodingTestCase is a helper to open disk writer in tests.
type EncodingTestCase struct {
	*writerSyncHelper
	T       *testing.T
	Ctx     context.Context
	Logger  log.DebugLogger
	Clock   *clock.Mock
	Events  *events.Events[encoding.Writer]
	Output  *DummyOutput
	Manager *manager.Manager
	Slice   *model.Slice
}

type writerSyncHelper struct {
	writeDone chan struct{}
	syncers   []*writesync.Syncer
}

type DummyOutput struct {
	bytes      bytes.Buffer
	SyncError  error
	CloseError error
}

func NewEncodingTestCase(t *testing.T) *EncodingTestCase {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(func() {
		cancel()
	})

	d, mock := dependencies.NewMockedServiceScope(t)

	tc := &EncodingTestCase{
		T:                t,
		writerSyncHelper: &writerSyncHelper{writeDone: make(chan struct{}, 100)},
		Ctx:              ctx,
		Logger:           mock.DebugLogger(),
		Clock:            clock.NewMock(),
		Events:           events.New[encoding.Writer](),
		Output:           &DummyOutput{},
		Slice:            test.NewSlice(),
	}

	cfg := mock.TestConfig().Storage.Level.Local.Encoding
	cfg.Encoder.Factory = tc.writerSyncHelper.NewDummyEncoder
	cfg.SyncerFactory = tc.writerSyncHelper.NewSyncer
	cfg.OutputOpener = func(sliceKey model.SliceKey) (writechain.File, error) {
		return tc.Output, nil
	}

	var err error
	tc.Manager, err = manager.New(d, cfg)
	require.NoError(t, err)

	return tc
}

func (tc *EncodingTestCase) NewWriter() (encoding.Writer, error) {
	// Slice definition must be valid
	val := validator.New()
	require.NoError(tc.T, val.Validate(context.Background(), tc.Slice))

	w, err := tc.Manager.OpenWriter(tc.Ctx, tc.Slice)
	if err != nil {
		return nil, err
	}

	return w, nil
}

func (tc *EncodingTestCase) AssertLogs(expected string) bool {
	return tc.Logger.AssertJSONMessages(tc.T, expected)
}

func (o *DummyOutput) String() string {
	return o.bytes.String()
}

func (o *DummyOutput) Write(p []byte) (n int, err error) {
	return o.bytes.Write(p)
}

func (o *DummyOutput) Sync() error {
	return o.SyncError
}

func (o *DummyOutput) Close() error {
	return o.CloseError
}

// NewDummyEncoder implements writer.WriterFactory.
// See also ExpectWritesCount and TriggerSync methods.
func (h *writerSyncHelper) NewDummyEncoder(cfg encoder.Config, out io.Writer, slice *model.Slice) (encoder.Encoder, error) {
	return NewDummyEncoder(cfg, out, slice, h.writeDone), nil
}

// NewSyncer implements writesync.SyncerFactory.
// See also ExpectWritesCount and TriggerSync methods.
func (h *writerSyncHelper) NewSyncer(ctx context.Context, logger log.Logger, clock clock.Clock, config writesync.Config, chain writesync.Chain, statistics writesync.StatisticsProvider,
) *writesync.Syncer {
	s := writesync.NewSyncer(ctx, logger, clock, config, chain, statistics)
	h.syncers = append(h.syncers, s)
	return s
}

func (h *writerSyncHelper) ExpectWritesCount(tb testing.TB, n int) {
	tb.Helper()
	tb.Logf(`waiting for %d writes`, n)
	for i := 0; i < n; i++ {
		select {
		case <-h.writeDone:
			tb.Logf(`write %d done`, i+1)
		case <-time.After(2 * time.Second):
			assert.FailNow(tb, "timeout")
			return
		}
	}
	tb.Logf(`all writes done`)
}

func (h *writerSyncHelper) TriggerSync(tb testing.TB) {
	tb.Helper()
	tb.Logf("trigger sync")

	wg := &sync.WaitGroup{}
	for _, s := range h.syncers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(tb, s.TriggerSync(context.Background(), true).Wait())
		}()
	}
	wg.Wait()

	tb.Logf("sync done")
}

// DummyEncoder implements the encoder.Encoder for tests.
// It encodes row values to one line as strings, separated by comma.
// Row is separated by the new line.
type DummyEncoder struct {
	out        io.Writer
	writeDone  chan struct{}
	FlushError error
	CloseError error
}

func DummyEncoderFactory(cfg encoder.Config, out io.Writer, slice *model.Slice) (encoder.Encoder, error) {
	return NewDummyEncoder(cfg, out, slice, nil), nil
}

func NewDummyEncoder(_ encoder.Config, out io.Writer, _ *model.Slice, writeDone chan struct{}) *DummyEncoder {
	return &DummyEncoder{out: out, writeDone: writeDone}
}

func (w *DummyEncoder) WriteRecord(values []any) error {
	var s bytes.Buffer
	for i, v := range values {
		if i > 0 {
			s.WriteString(",")
		}
		s.WriteString(cast.ToString(v))
	}
	s.WriteString("\n")

	_, err := w.out.Write(s.Bytes())
	if err == nil && w.writeDone != nil {
		w.writeDone <- struct{}{}
	}
	return err
}

func (w *DummyEncoder) Flush() error {
	return w.FlushError
}

func (w *DummyEncoder) Close() error {
	return w.CloseError
}
