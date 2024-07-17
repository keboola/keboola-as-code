package encoding_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestEncodingPipeline_Basic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := log.NewDebugLogger()
	clk := clock.New()

	slice := test.NewSlice()
	slice.LocalStorage.Encoding.Encoder.Factory = encoder.FactoryFn(dummyEncoderFactory)

	output := newDummyOutput()

	w, err := encoding.newPipeline(ctx, logger, clk, slice.LocalStorage.Encoding, slice.SliceKey, slice.Mapping, output, events.New[encoding.Pipeline]())
	require.NoError(t, err)

	// Test getters
	assert.Equal(t, slice.SliceKey, w.SliceKey())

	// Test write methods
	assert.NoError(t, w.WriteRecord(recordctx.FromHTTP(clk.Now(), &http.Request{Body: io.NopCloser(strings.NewReader("foo"))})))
	assert.NoError(t, w.WriteRecord(recordctx.FromHTTP(clk.Now(), &http.Request{Body: io.NopCloser(strings.NewReader("bar"))})))

	// Test Close method
	assert.NoError(t, w.Close(ctx))

	// Try Close again
	err = w.Close(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "writer is already closed", err.Error())
	}

	// Check output
	assert.Equal(t, "foo\nbar\n", output.String())
}

func TestEncodingPipeline_FlushError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := log.NewDebugLogger()
	clk := clock.NewMock()

	slice := test.NewSlice()
	slice.LocalStorage.Encoding.Encoder.Factory = encoder.FactoryFn(func(cfg encoder.Config, mapping any, out io.Writer) (encoder.Encoder, error) {
		w := newDummyEncoder(out, nil)
		w.FlushError = errors.New("some error")
		return w, nil
	})

	w, err := encoding.newPipeline(ctx, logger, clk, slice.LocalStorage.Encoding, slice.SliceKey, slice.Mapping, newDummyOutput(), events.New[encoding.Pipeline]())
	require.NoError(t, err)

	// Test Close method
	err = w.Close(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "chain sync error:\n- chain flush error:\n  - cannot flush \"*encoding_test.dummyEncoder\": some error", err.Error())
	}
}

func TestEncodingPipeline_CloseError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := log.NewDebugLogger()
	clk := clock.NewMock()

	slice := test.NewSlice()
	slice.LocalStorage.Encoding.Encoder.Factory = encoder.FactoryFn(func(cfg encoder.Config, mapping any, out io.Writer) (encoder.Encoder, error) {
		w := newDummyEncoder(out, nil)
		w.CloseError = errors.New("some error")
		return w, nil
	})

	w, err := encoding.newPipeline(ctx, logger, clk, slice.LocalStorage.Encoding, slice.SliceKey, slice.Mapping, newDummyOutput(), events.New[encoding.Pipeline]())
	require.NoError(t, err)

	// Test Close method
	err = w.Close(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "chain close error:\n- cannot close \"*encoding_test.dummyEncoder\": some error", err.Error())
	}
}

func TestEncodingPipeline_Open_Ok(t *testing.T) {
	t.Parallel()
	tc := newEncodingTestCase(t)

	w, err := tc.OpenPipeline()
	assert.NoError(t, err)
	assert.Len(t, tc.Manager.Pipelines(), 1)

	assert.NoError(t, w.Close(context.Background()))
	assert.Len(t, tc.Manager.Pipelines(), 0)
}

func TestEncodingPipeline_Open_Duplicate(t *testing.T) {
	t.Parallel()
	tc := newEncodingTestCase(t)

	// Create the writer first time - ok
	w, err := tc.OpenPipeline()
	assert.NoError(t, err)
	assert.Len(t, tc.Manager.Pipelines(), 1)

	// Create writer for the same slice again - error
	_, err = tc.OpenPipeline()
	if assert.Error(t, err) {
		assert.Equal(t, `encoding pipeline for slice "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T20:00:00.000Z" already exists`, err.Error())
	}
	assert.Len(t, tc.Manager.Pipelines(), 1)

	assert.NoError(t, w.Close(context.Background()))
	assert.Len(t, tc.Manager.Pipelines(), 0)
}

func TestEncodingPipeline_Sync_Enabled_Wait_ToDisk(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newEncodingTestCase(t)
	tc.Slice.LocalStorage.Encoding.Sync.Mode = writesync.ModeDisk
	tc.Slice.LocalStorage.Encoding.Sync.Wait = true

	w, err := tc.OpenPipeline()
	assert.NoError(t, err)

	// Writes are BLOCKING, each write is waiting for the next sync

	// Write two rows and trigger sync
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRecord(tc.TestRecord("foo1")))
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRecord(tc.TestRecord("foo1")))
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	tc.ExpectWritesCount(t, 2)
	tc.TriggerSync(t)
	wg.Wait()

	// Write one row and trigger sync
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRecord(tc.TestRecord("foo2")))
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	tc.ExpectWritesCount(t, 1)
	tc.TriggerSync(t)
	wg.Wait()

	// Last write
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRecord(tc.TestRecord("foo3")))
	}()
	tc.ExpectWritesCount(t, 1)

	// Close writer - it triggers the last sync
	assert.NoError(t, w.Close(ctx))

	// Wait for goroutine
	wg.Wait()

	// Check file content
	assert.Equal(t, strings.TrimSpace(`
foo1
foo1
foo2
foo3
`), strings.TrimSpace(tc.Output.String()))

	// Check logs
	tc.AssertLogs(`
{"level":"debug","message":"opening encoding pipeline"}
{"level":"info","message":"sync is enabled, mode=disk, sync each {count=500 or uncompressed=10MB or compressed=1MB or interval=50ms}, check each 1ms"}
{"level":"debug","message":"opened encoding pipeline"}
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
{"level":"debug","message":"closing encoding pipeline"}                  
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
{"level":"debug","message":"closed encoding pipeline"}
`)
}

func TestEncodingPipeline_Sync_Enabled_Wait_ToDiskCache(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newEncodingTestCase(t)
	tc.Slice.LocalStorage.Encoding.Sync.Mode = writesync.ModeCache
	tc.Slice.LocalStorage.Encoding.Sync.Wait = true

	w, err := tc.OpenPipeline()
	assert.NoError(t, err)

	// Writes are BLOCKING, each write is waiting for the next sync

	// Write two rows and trigger sync
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRecord(tc.TestRecord("foo1")))
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRecord(tc.TestRecord("foo1")))
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	tc.ExpectWritesCount(t, 2)
	tc.TriggerSync(t)
	wg.Wait()

	// Write one row and trigger sync
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRecord(tc.TestRecord("foo2")))
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	tc.ExpectWritesCount(t, 1)
	tc.TriggerSync(t)
	wg.Wait()

	// Last write
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, w.WriteRecord(tc.TestRecord("foo3")))
	}()
	tc.ExpectWritesCount(t, 1)

	// Close writer - it triggers the last sync
	assert.NoError(t, w.Close(ctx))
	wg.Wait()

	// Check file content
	assert.Equal(t, strings.TrimSpace(`
foo1
foo1
foo2
foo3
`), strings.TrimSpace(tc.Output.String()))

	// Check logs
	tc.AssertLogs(`
{"level":"debug","message":"opening encoding pipeline"}
{"level":"info","message":"sync is enabled, mode=cache, sync each {count=500 or uncompressed=10MB or compressed=1MB or interval=50ms}, check each 1ms"}
{"level":"debug","message":"opened encoding pipeline"}
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
{"level":"debug","message":"closing encoding pipeline"}
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
{"level":"debug","message":"closed encoding pipeline"}
`)
}

func TestEncodingPipeline_Sync_Enabled_NoWait_ToDisk(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newEncodingTestCase(t)
	tc.Slice.LocalStorage.Encoding.Sync.Mode = writesync.ModeDisk
	tc.Slice.LocalStorage.Encoding.Sync.Wait = false

	w, err := tc.OpenPipeline()
	assert.NoError(t, err)

	// Writes are NOT BLOCKING, write doesn't wait for the next sync

	// Write two rows and trigger sync
	assert.NoError(t, w.WriteRecord(tc.TestRecord("foo1")))
	assert.NoError(t, w.WriteRecord(tc.TestRecord("foo2")))
	tc.ExpectWritesCount(t, 2)
	tc.TriggerSync(t)

	// Write one row and trigger sync
	assert.NoError(t, w.WriteRecord(tc.TestRecord("foo3")))
	tc.ExpectWritesCount(t, 1)
	tc.TriggerSync(t)

	// Last write
	assert.NoError(t, w.WriteRecord(tc.TestRecord("foo4")))
	tc.ExpectWritesCount(t, 1)

	// Close writer - it triggers the last sync
	assert.NoError(t, w.Close(ctx))

	// Check file content
	assert.Equal(t, strings.TrimSpace(`
foo1
foo2
foo3
foo4
`), strings.TrimSpace(tc.Output.String()))

	// Check logs
	tc.AssertLogs(`
{"level":"debug","message":"opening encoding pipeline"}
{"level":"info","message":"sync is enabled, mode=disk, sync each {count=500 or uncompressed=10MB or compressed=1MB or interval=50ms}, check each 1ms"}
{"level":"debug","message":"opened encoding pipeline"}
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
{"level":"debug","message":"closing encoding pipeline"}
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
{"level":"debug","message":"closed encoding pipeline"}
`)
}

func TestEncodingPipeline_Sync_Enabled_NoWait_ToDiskCache(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newEncodingTestCase(t)
	tc.Slice.LocalStorage.Encoding.Sync.Mode = writesync.ModeCache
	tc.Slice.LocalStorage.Encoding.Sync.Wait = false

	w, err := tc.OpenPipeline()
	assert.NoError(t, err)

	// Writes are NOT BLOCKING, write doesn't wait for the next sync

	// Write two rows and trigger sync
	assert.NoError(t, w.WriteRecord(tc.TestRecord("foo1")))
	assert.NoError(t, w.WriteRecord(tc.TestRecord("foo2")))
	tc.ExpectWritesCount(t, 2)
	tc.TriggerSync(t)

	// Write one row and trigger sync
	assert.NoError(t, w.WriteRecord(tc.TestRecord("foo3")))
	tc.ExpectWritesCount(t, 1)
	tc.TriggerSync(t)

	// Last write
	assert.NoError(t, w.WriteRecord(tc.TestRecord("foo4")))
	tc.ExpectWritesCount(t, 1)

	// Close writer - it triggers the last sync
	assert.NoError(t, w.Close(ctx))

	// Check file content
	assert.Equal(t, strings.TrimSpace(`
foo1
foo2
foo3
foo4
`), strings.TrimSpace(tc.Output.String()))

	// Check logs
	tc.AssertLogs(`
{"level":"debug","message":"opening encoding pipeline"}
{"level":"info","message":"sync is enabled, mode=cache, sync each {count=500 or uncompressed=10MB or compressed=1MB or interval=50ms}, check each 1ms"}
{"level":"debug","message":"opened encoding pipeline"}
{"level":"debug","message":"starting sync to cache"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to cache done"}
{"level":"debug","message":"starting sync to cache"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to cache done"}
{"level":"debug","message":"closing encoding pipeline"}
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
{"level":"debug","message":"closed encoding pipeline"}
`)
}

func TestEncodingPipeline_Sync_Disabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newEncodingTestCase(t)
	tc.Slice.LocalStorage.Encoding.Sync = writesync.Config{Mode: writesync.ModeDisabled}

	w, err := tc.OpenPipeline()
	assert.NoError(t, err)

	// Writes are NOT BLOCKING, sync is disabled completely

	// Write two rows and trigger sync
	assert.NoError(t, w.WriteRecord(tc.TestRecord("foo1")))
	assert.NoError(t, w.WriteRecord(tc.TestRecord("foo2")))
	tc.ExpectWritesCount(t, 2)

	// Write one row and trigger sync
	assert.NoError(t, w.WriteRecord(tc.TestRecord("foo3")))
	tc.ExpectWritesCount(t, 1)

	// Last write
	assert.NoError(t, w.WriteRecord(tc.TestRecord("foo4")))
	tc.ExpectWritesCount(t, 1)

	// Close writer
	assert.NoError(t, w.Close(ctx))

	// Check file content
	assert.Equal(t, strings.TrimSpace(`
foo1
foo2
foo3
foo4
`), strings.TrimSpace(tc.Output.String()))

	// Check logs
	tc.AssertLogs(`
{"level":"debug","message":"opening encoding pipeline"}
{"level":"info","message":"sync is disabled"}
{"level":"debug","message":"opened encoding pipeline"}
{"level":"debug","message":"closing encoding pipeline"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"syncer stopped"}
{"level":"debug","message":"closing chain"}
{"level":"debug","message":"syncing file"}
{"level":"debug","message":"file synced"}
{"level":"debug","message":"chain closed"}
{"level":"debug","message":"closed encoding pipeline"}
`)
}

// encodingTestCase is a helper to open encoding pipeline in tests.
type encodingTestCase struct {
	*writerSyncHelper
	T       *testing.T
	Ctx     context.Context
	Logger  log.DebugLogger
	Clock   *clock.Mock
	Events  *events.Events[encoding.Pipeline]
	Output  *dummyOutput
	Manager *encoding.Manager
	Slice   *model.Slice
}

type writerSyncHelper struct {
	writeDone chan struct{}
	syncers   []*writesync.Syncer
}

func newEncodingTestCase(t *testing.T) *encodingTestCase {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(func() {
		cancel()
	})

	d, mock := dependencies.NewMockedServiceScope(t)

	helper := &writerSyncHelper{writeDone: make(chan struct{}, 100)}

	slice := test.NewSlice()
	slice.LocalStorage.Encoding.Encoder.Factory = helper
	slice.LocalStorage.Encoding.Sync.OverwriteSyncerFactory = helper

	tc := &encodingTestCase{
		T:                t,
		writerSyncHelper: helper,
		Ctx:              ctx,
		Logger:           mock.DebugLogger(),
		Clock:            clock.NewMock(),
		Events:           events.New[encoding.Pipeline](),
		Output:           newDummyOutput(),
		Slice:            slice,
	}

	var err error
	tc.Manager, err = encoding.NewManager(d)
	require.NoError(t, err)

	return tc
}

func (tc *encodingTestCase) OpenPipeline() (encoding.Pipeline, error) {
	// Slice definition must be valid
	val := validator.New()
	require.NoError(tc.T, val.Validate(context.Background(), tc.Slice))

	w, err := tc.Manager.OpenPipeline(tc.Ctx, tc.Slice.SliceKey, tc.Slice.LocalStorage.Encoding, tc.Slice.Mapping, tc.Output)
	if err != nil {
		return nil, err
	}

	return w, nil
}

func (tc *encodingTestCase) TestRecord(body string) recordctx.Context {
	return recordctx.FromHTTP(tc.Clock.Now(), &http.Request{Body: io.NopCloser(strings.NewReader(body))})
}

func (tc *encodingTestCase) AssertLogs(expected string) bool {
	return tc.Logger.AssertJSONMessages(tc.T, expected)
}

func (h *writerSyncHelper) NewEncoder(cfg encoder.Config, mapping any, out io.Writer) (encoder.Encoder, error) {
	return newDummyEncoder(out, h.writeDone), nil
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

// dummyEncoder implements the encoder.Encoder for tests.
// It encodes body value, followed by the new line.
type dummyEncoder struct {
	out        io.Writer
	writeDone  chan struct{}
	FlushError error
	CloseError error
}

func dummyEncoderFactory(cfg encoder.Config, mapping any, out io.Writer) (encoder.Encoder, error) {
	return newDummyEncoder(out, nil), nil
}

func newDummyEncoder(out io.Writer, writeDone chan struct{}) *dummyEncoder {
	return &dummyEncoder{out: out, writeDone: writeDone}
}

func (w *dummyEncoder) WriteRecord(record recordctx.Context) error {
	body, err := record.BodyString()
	if err != nil {
		return err
	}

	_, err = w.out.Write([]byte(body + "\n"))
	if err == nil && w.writeDone != nil {
		w.writeDone <- struct{}{}
	}
	return err
}

func (w *dummyEncoder) Flush() error {
	return w.FlushError
}

func (w *dummyEncoder) Close() error {
	return w.CloseError
}

type dummyOutput struct {
	bytes      bytes.Buffer
	SyncError  error
	CloseError error
}

func newDummyOutput() *dummyOutput {
	return &dummyOutput{}
}

func (o *dummyOutput) String() string {
	return o.bytes.String()
}

func (o *dummyOutput) Write(p []byte) (n int, err error) {
	return o.bytes.Write(p)
}

func (o *dummyOutput) Sync() error {
	return o.SyncError
}

func (o *dummyOutput) Close(_ context.Context) error {
	return o.CloseError
}
