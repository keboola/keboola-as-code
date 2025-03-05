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

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/connection"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder/result"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync/notify"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestEncodingPipeline_Basic(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	d, _ := dependencies.NewMockedSourceScope(t, ctx)

	slice := test.NewSlice()
	slice.Encoding.Encoder.OverrideEncoderFactory = encoder.FactoryFn(dummyEncoderFactory)

	output := newDummyOutput()

	w, err := d.EncodingManager().OpenPipeline(
		ctx,
		slice.SliceKey,
		d.Telemetry(),
		d.ConnectionManager(),
		slice.Mapping,
		slice.Encoding,
		slice.LocalStorage,
		false,
		func(ctx context.Context, cause string) {},
		output,
	)

	require.NoError(t, err)
	// Test getters
	assert.Equal(t, slice.SliceKey, w.SliceKey())

	// Test write methods
	n, err := w.WriteRecord(recordctx.FromHTTP(d.Clock().Now(), &http.Request{Body: io.NopCloser(strings.NewReader("foo"))}))
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	n, err = w.WriteRecord(recordctx.FromHTTP(d.Clock().Now(), &http.Request{Body: io.NopCloser(strings.NewReader("bar"))}))
	require.NoError(t, err)
	assert.Equal(t, 4, n)

	// Test Close method
	require.NoError(t, w.Close(ctx))

	// Try Close again
	err = w.Close(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "encoding pipeline is already closed", err.Error())
	}

	// Check chunks
	assert.Equal(t, "foo\nbar\n", output.String())
}

func TestEncodingPipeline_FlushError(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	d, _ := dependencies.NewMockedSourceScope(t, ctx)

	slice := test.NewSlice()
	slice.Encoding.Encoder.OverrideEncoderFactory = encoder.FactoryFn(func(cfg encoder.Config, mapping any, out io.Writer, notifier func(ctx context.Context) *notify.Notifier) (encoder.Encoder, error) {
		w := newDummyEncoder(out, nil, notifier)
		w.FlushError = errors.New("some error")
		return w, nil
	})

	w, err := d.EncodingManager().OpenPipeline(
		ctx,
		slice.SliceKey,
		d.Telemetry(),
		d.ConnectionManager(),
		slice.Mapping,
		slice.Encoding,
		slice.LocalStorage,
		false,
		func(ctx context.Context, cause string) {},
		newDummyOutput(),
	)

	require.NoError(t, err)
	// Test Close method
	err = w.Close(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "chain flush error:\n- cannot flush \"*encoding_test.dummyEncoder\": some error", err.Error())
	}
}

func TestEncodingPipeline_CloseError(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	d, _ := dependencies.NewMockedSourceScope(t, ctx)

	slice := test.NewSlice()
	slice.Encoding.Encoder.OverrideEncoderFactory = encoder.FactoryFn(func(cfg encoder.Config, mapping any, out io.Writer, notifier func(ctx context.Context) *notify.Notifier) (encoder.Encoder, error) {
		w := newDummyEncoder(out, nil, notifier)
		w.CloseError = errors.New("some error")
		return w, nil
	})

	w, err := d.EncodingManager().OpenPipeline(
		ctx,
		slice.SliceKey,
		d.Telemetry(),
		d.ConnectionManager(),
		slice.Mapping,
		slice.Encoding,
		slice.LocalStorage,
		false,
		func(ctx context.Context, cause string) {},
		newDummyOutput(),
	)
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
	require.NoError(t, err)
	assert.Len(t, tc.Manager.Pipelines(), 1)

	require.NoError(t, w.Close(t.Context()))
	assert.Empty(t, tc.Manager.Pipelines())
}

func TestEncodingPipeline_Open_Duplicate(t *testing.T) {
	t.Parallel()
	tc := newEncodingTestCase(t)

	// Create the writer first time - ok
	w, err := tc.OpenPipeline()
	require.NoError(t, err)
	assert.Len(t, tc.Manager.Pipelines(), 1)

	// Create writer for the same slice again - error
	_, err = tc.OpenPipeline()
	if assert.Error(t, err) {
		assert.Equal(t, `encoding pipeline for slice "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T20:00:00.000Z" already exists`, err.Error())
	}
	assert.Len(t, tc.Manager.Pipelines(), 1)

	require.NoError(t, w.Close(t.Context()))
	assert.Empty(t, tc.Manager.Pipelines())
}

func TestEncodingPipeline_Sync_Wait_ToDisk(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	tc := newEncodingTestCase(t)
	tc.Slice.Encoding.Sync.Mode = writesync.ModeDisk
	tc.Slice.Encoding.Sync.Wait = true

	w, err := tc.OpenPipeline()
	require.NoError(t, err)

	// Writes are BLOCKING, each write is waiting for the next sync

	// Write two rows and trigger sync
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		n, err := w.WriteRecord(tc.TestRecord("foo1"))
		require.NoError(t, err)
		assert.Equal(t, 5, n)
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	go func() {
		defer wg.Done()
		n, err := w.WriteRecord(tc.TestRecord("foo1"))
		require.NoError(t, err)
		assert.Equal(t, 5, n)
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	tc.ExpectWritesCount(t, 2)
	// When trigger sync is executed, it creates new notifier, old notifier is close and unblocks 2 writes above.
	// New notifier is handovered into encoder, so the next write is blocked on new notifier until new trigger sync is executed
	tc.TriggerSync(t)
	wg.Wait()

	// Write one row and trigger sync
	wg.Add(1)
	go func() {
		defer wg.Done()
		n, err := w.WriteRecord(tc.TestRecord("foo2"))
		require.NoError(t, err)
		assert.Equal(t, 5, n)
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	tc.ExpectWritesCount(t, 1)
	tc.TriggerSync(t)
	wg.Wait()

	// Last write
	wg.Add(1)
	go func() {
		defer wg.Done()
		n, err := w.WriteRecord(tc.TestRecord("foo3"))
		require.NoError(t, err)
		assert.Equal(t, 5, n)
	}()
	tc.ExpectWritesCount(t, 1)

	// Close writer - it triggers the last sync
	require.NoError(t, w.Close(ctx))

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
{"level":"debug","message":"sync is enabled, mode=disk, sync each {count=500 or uncompressed=10MB or compressed=1MB or interval=50ms}, check each 1ms"}
{"level":"debug","message":"opened encoding pipeline"}
{"level":"debug","message":"starting sync to disk"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"chunk completed, aligned = true, size = \"10B\""}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to disk done"}
{"level":"info","message":"TEST: write unblocked"}
{"level":"info","message":"TEST: write unblocked"}
{"level":"debug","message":"notifier obtained"}
{"level":"debug","message":"starting sync to disk"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"chunk completed, aligned = true, size = \"5B\""}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to disk done"}
{"level":"info","message":"TEST: write unblocked"}
{"level":"debug","message":"closing encoding pipeline"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to disk"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"chunk completed, aligned = true, size = \"5B\""}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"syncer stopped"}
{"level":"debug","message":"closing chain"}
{"level":"debug","message":"chain closed"}
{"level":"debug","message":"closed encoding pipeline"}
`)
}

func TestEncodingPipeline_Sync_Wait_ToDiskCache(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	tc := newEncodingTestCase(t)
	tc.Slice.Encoding.Sync.Mode = writesync.ModeCache
	tc.Slice.Encoding.Sync.Wait = true

	w, err := tc.OpenPipeline()
	require.NoError(t, err)

	// Writes are BLOCKING, each write is waiting for the next sync

	// Write two rows and trigger sync
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		n, err := w.WriteRecord(tc.TestRecord("foo1"))
		require.NoError(t, err)
		assert.Equal(t, 5, n)
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	go func() {
		defer wg.Done()
		n, err := w.WriteRecord(tc.TestRecord("foo1"))
		require.NoError(t, err)
		assert.Equal(t, 5, n)
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	tc.ExpectWritesCount(t, 2)
	tc.TriggerSync(t)
	wg.Wait()

	// Write one row and trigger sync
	wg.Add(1)
	go func() {
		defer wg.Done()
		n, err := w.WriteRecord(tc.TestRecord("foo2"))
		require.NoError(t, err)
		assert.Equal(t, 5, n)
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	tc.ExpectWritesCount(t, 1)
	tc.TriggerSync(t)
	wg.Wait()

	// Last write
	wg.Add(1)
	go func() {
		defer wg.Done()
		n, err := w.WriteRecord(tc.TestRecord("foo3"))
		require.NoError(t, err)
		assert.Equal(t, 5, n)
	}()
	tc.ExpectWritesCount(t, 1)

	// Close writer - it triggers the last sync
	require.NoError(t, w.Close(ctx))
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
{"level":"debug","message":"sync is enabled, mode=cache, sync each {count=500 or uncompressed=10MB or compressed=1MB or interval=50ms}, check each 1ms"}
{"level":"debug","message":"opened encoding pipeline"}
{"level":"debug","message":"starting sync to cache"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"chunk completed, aligned = true, size = \"10B\""}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to cache done"}
{"level":"info","message":"TEST: write unblocked"}
{"level":"info","message":"TEST: write unblocked"}
{"level":"debug","message":"starting sync to cache"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"chunk completed, aligned = true, size = \"5B\""}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to cache done"}
{"level":"info","message":"TEST: write unblocked"}
{"level":"debug","message":"closing encoding pipeline"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to cache"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"chunk completed, aligned = true, size = \"5B\""}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to cache done"}
{"level":"debug","message":"syncer stopped"}
{"level":"debug","message":"closing chain"}
{"level":"debug","message":"chain closed"}
{"level":"debug","message":"closed encoding pipeline"}
`)
}

func TestEncodingPipeline_Sync_NoWait_ToDisk(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	tc := newEncodingTestCase(t)
	tc.Slice.Encoding.Sync.Mode = writesync.ModeDisk
	tc.Slice.Encoding.Sync.Wait = false

	w, err := tc.OpenPipeline()
	require.NoError(t, err)

	// Writes are NOT BLOCKING, write doesn't wait for the next sync

	// Write two rows and trigger sync
	n, err := w.WriteRecord(tc.TestRecord("foo1"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	n, err = w.WriteRecord(tc.TestRecord("foo2"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	tc.ExpectWritesCount(t, 2)
	tc.TriggerSync(t)

	// Write one row and trigger sync
	n, err = w.WriteRecord(tc.TestRecord("foo3"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	tc.ExpectWritesCount(t, 1)
	tc.TriggerSync(t)

	// Last write
	n, err = w.WriteRecord(tc.TestRecord("foo4"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	tc.ExpectWritesCount(t, 1)

	// Close writer - it triggers the last sync
	require.NoError(t, w.Close(ctx))

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
{"level":"debug","message":"sync is enabled, mode=disk, sync each {count=500 or uncompressed=10MB or compressed=1MB or interval=50ms}, check each 1ms"}
{"level":"debug","message":"opened encoding pipeline"}
{"level":"debug","message":"starting sync to disk"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"chunk completed, aligned = true, size = \"10B\""}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"starting sync to disk"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"chunk completed, aligned = true, size = \"5B\""}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"closing encoding pipeline"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to disk"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"chunk completed, aligned = true, size = \"5B\""}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"syncer stopped"}
{"level":"debug","message":"closing chain"}
{"level":"debug","message":"chain closed"}
{"level":"debug","message":"closed encoding pipeline"}
`)
}

func TestEncodingPipeline_Sync_NoWait_ToDiskCache(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	tc := newEncodingTestCase(t)
	tc.Slice.Encoding.Sync.Mode = writesync.ModeCache
	tc.Slice.Encoding.Sync.Wait = false

	w, err := tc.OpenPipeline()
	require.NoError(t, err)

	// Writes are NOT BLOCKING, write doesn't wait for the next sync

	// Write two rows and trigger sync
	n, err := w.WriteRecord(tc.TestRecord("foo1"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	n, err = w.WriteRecord(tc.TestRecord("foo2"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	tc.ExpectWritesCount(t, 2)
	tc.TriggerSync(t)

	// Write one row and trigger sync
	n, err = w.WriteRecord(tc.TestRecord("foo3"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	tc.ExpectWritesCount(t, 1)
	tc.TriggerSync(t)

	// Last write
	n, err = w.WriteRecord(tc.TestRecord("foo4"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	tc.ExpectWritesCount(t, 1)

	// Close writer - it triggers the last sync
	require.NoError(t, w.Close(ctx))

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
{"level":"debug","message":"sync is enabled, mode=cache, sync each {count=500 or uncompressed=10MB or compressed=1MB or interval=50ms}, check each 1ms"}
{"level":"debug","message":"opened encoding pipeline"}
{"level":"debug","message":"starting sync to cache"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"chunk completed, aligned = true, size = \"10B\""}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to cache done"}
{"level":"debug","message":"starting sync to cache"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"chunk completed, aligned = true, size = \"5B\""}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to cache done"}
{"level":"debug","message":"closing encoding pipeline"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to cache"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"chunk completed, aligned = true, size = \"5B\""}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to cache done"}
{"level":"debug","message":"syncer stopped"}
{"level":"debug","message":"closing chain"}
{"level":"debug","message":"chain closed"}
{"level":"debug","message":"closed encoding pipeline"}
`)
}

func TestEncodingPipeline_TemporaryError(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	tc := newEncodingTestCase(t)
	tc.Slice.Encoding.Sync.Mode = writesync.ModeDisk
	tc.Slice.Encoding.Sync.Wait = true

	w, err := tc.OpenPipeline()
	require.NoError(t, err)

	// Writes are BLOCKING, each write is waiting for the next sync

	// Write two rows and trigger sync
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		n, err := w.WriteRecord(tc.TestRecord("foo1"))
		require.NoError(t, err)
		assert.Equal(t, 5, n)
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	go func() {
		defer wg.Done()
		n, err := w.WriteRecord(tc.TestRecord("foo1"))
		require.NoError(t, err)
		assert.Equal(t, 5, n)
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	tc.ExpectWritesCount(t, 2)
	// When trigger sync is executed, it creates new notifier, old notifier is close and unblocks 2 writes above.
	// New notifier is handovered into encoder, so the next write is blocked on new notifier until new trigger sync is executed
	tc.TriggerSync(t)
	wg.Wait()

	// Let the next write fail
	tc.Output.WriteError = errors.New("some error")

	// Write one row and trigger sync
	wg.Add(1)
	go func() {
		defer wg.Done()
		n, err := w.WriteRecord(tc.TestRecord("foo2"))
		require.NoError(t, err)
		assert.Equal(t, 5, n)
	}()
	tc.ExpectWritesCount(t, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		tc.TriggerSync(t)
	}()

	// Wait for error to be logged
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		tc.Logger.AssertJSONMessages(c, `
{"level":"warn","message":"chunks write failed: some error, waiting %s, chunks count = %s"}
`)
	}, 5*time.Second, 100*time.Millisecond)

	// Disable error and trigger retry
	tc.Output.WriteError = nil
	tc.Clock.Advance(1 * time.Second)

	// Another write
	wg.Add(1)
	go func() {
		defer wg.Done()
		n, err := w.WriteRecord(tc.TestRecord("foo3"))
		require.NoError(t, err)
		assert.Equal(t, 5, n)
		tc.Logger.Infof(ctx, "TEST: write unblocked")
	}()
	tc.ExpectWritesCount(t, 1)
	tc.TriggerSync(t)

	// Wait for goroutines
	wg.Wait()

	// Close writer - it triggers the last sync
	require.NoError(t, w.Close(ctx))

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
{"level":"debug","message":"sync is enabled, mode=disk, sync each {count=500 or uncompressed=10MB or compressed=1MB or interval=50ms}, check each 1ms"}
{"level":"debug","message":"opened encoding pipeline"}
{"level":"debug","message":"starting sync to disk"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"chunk completed, aligned = true, size = \"10B\""}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to disk done"}
{"level":"info","message":"TEST: write unblocked"}
{"level":"info","message":"TEST: write unblocked"}
{"level":"debug","message":"notifier obtained"}
{"level":"debug","message":"starting sync to disk"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"chunk completed, aligned = true, size = \"5B\""}
{"level":"debug","message":"writers flushed"}
{"level":"warn","message":"chunks write failed: some error, waiting %s, chunks count = 1"}
{"level":"debug","message":"chunk written, size \"5B\""}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"notifier obtained"}
{"level":"debug","message":"starting sync to disk"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"chunk completed, aligned = true, size = \"5B\""}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"chunk written, size \"5B\""}
{"level":"info","message":"TEST: write unblocked"}
{"level":"debug","message":"closing encoding pipeline"}
{"level":"debug","message":"stopping syncer"}
{"level":"debug","message":"starting sync to disk"}
{"level":"debug","message":"flushing writers"}
{"level":"debug","message":"writers flushed"}
{"level":"debug","message":"sync to disk done"}
{"level":"debug","message":"syncer stopped"}
{"level":"debug","message":"closing chain"}
{"level":"debug","message":"chain closed"}
{"level":"debug","message":"closed encoding pipeline"}
`)
}

// encodingTestCase is a helper to open encoding pipeline in tests.
type encodingTestCase struct {
	*writerSyncHelper
	T                 *testing.T
	Ctx               context.Context
	Logger            log.DebugLogger
	Clock             *clockwork.FakeClock
	Telemetry         telemetry.Telemetry
	ConnectionManager *connection.Manager
	Output            *dummyOutput
	Events            *events.Events[encoding.Pipeline]
	Manager           *encoding.Manager
	Slice             *model.Slice
}

type writerSyncHelper struct {
	writeDone chan struct{}
	syncers   []*writesync.Syncer
}

func newEncodingTestCase(t *testing.T) *encodingTestCase {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(func() {
		cancel()
	})

	// Disable real clocks, in tests, sync is triggered manually.
	// The sync timer may cause unexpected log messages.
	clk := clockwork.NewFakeClock()
	d, mock := dependencies.NewMockedSourceScope(t, ctx, commonDeps.WithClock(clk))

	helper := &writerSyncHelper{writeDone: make(chan struct{}, 100)}

	slice := test.NewSlice()
	slice.Encoding.Encoder.OverrideEncoderFactory = helper
	slice.Encoding.Sync.OverrideSyncerFactory = helper

	tc := &encodingTestCase{
		T:                 t,
		writerSyncHelper:  helper,
		Ctx:               ctx,
		Logger:            mock.DebugLogger(),
		Clock:             clk,
		Telemetry:         d.Telemetry(),
		ConnectionManager: d.ConnectionManager(),
		Output:            newDummyOutput(),
		Events:            events.New[encoding.Pipeline](),
		Manager:           d.EncodingManager(),
		Slice:             slice,
	}
	return tc
}

func (tc *encodingTestCase) OpenPipeline() (encoding.Pipeline, error) {
	// Slice definition must be valid
	val := validator.New()
	require.NoError(tc.T, val.Validate(context.Background(), tc.Slice))

	w, err := tc.Manager.OpenPipeline(
		tc.Ctx,
		tc.Slice.SliceKey,
		tc.Telemetry,
		tc.ConnectionManager,
		tc.Slice.Mapping,
		tc.Slice.Encoding,
		tc.Slice.LocalStorage,
		tc.Slice.Encoding.Compression.Type != compression.TypeNone,
		func(ctx context.Context, cause string) {},
		tc.Output,
	)
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

func (h *writerSyncHelper) NewEncoder(cfg encoder.Config, mapping any, out io.Writer, notifier func(ctx context.Context) *notify.Notifier) (encoder.Encoder, error) {
	return newDummyEncoder(out, h.writeDone, notifier), nil
}

// NewSyncer implements writesync.SyncerFactory.
// See also ExpectWritesCount and TriggerSync methods.
func (h *writerSyncHelper) NewSyncer(ctx context.Context, logger log.Logger, clock clockwork.Clock, config writesync.Config, chain writesync.Pipeline, statistics writesync.StatisticsProvider,
) *writesync.Syncer {
	s := writesync.NewSyncer(ctx, logger, clock, config, chain, statistics)
	h.syncers = append(h.syncers, s)
	return s
}

func (h *writerSyncHelper) ExpectWritesCount(tb testing.TB, n int) {
	tb.Helper()
	tb.Logf(`waiting for %d writes`, n)
	for i := range n {
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
			notifier := s.TriggerSync(true)
			assert.NoError(tb, notifier.Wait(tb.Context()))
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
	notifier   func(ctx context.Context) *notify.Notifier
	FlushError error
	CloseError error
}

func dummyEncoderFactory(cfg encoder.Config, mapping any, out io.Writer, notifier func(ctx context.Context) *notify.Notifier) (encoder.Encoder, error) {
	return newDummyEncoder(out, nil, notifier), nil
}

func newDummyEncoder(out io.Writer, writeDone chan struct{}, notifier func(ctx context.Context) *notify.Notifier) *dummyEncoder {
	return &dummyEncoder{out: out, writeDone: writeDone, notifier: notifier}
}

func (w *dummyEncoder) WriteRecord(record recordctx.Context) (result.WriteRecordResult, error) {
	body, err := record.BodyBytes()
	if err != nil {
		return result.WriteRecordResult{}, err
	}

	body = append(body, '\n')

	n, err := w.out.Write(body)
	wrr := result.NewNotifierWriteRecordResult(n, w.notifier(record.Ctx()))
	if err == nil && w.writeDone != nil {
		w.writeDone <- struct{}{}
	}

	return wrr, err
}

func (w *dummyEncoder) Flush() error {
	return w.FlushError
}

func (w *dummyEncoder) Close() error {
	return w.CloseError
}

type dummyOutput struct {
	bytes      bytes.Buffer
	WriteError error
	SyncError  error
	CloseError error
}

func newDummyOutput() *dummyOutput {
	return &dummyOutput{}
}

func (o *dummyOutput) String() string {
	return o.bytes.String()
}

func (o *dummyOutput) IsReady() bool {
	return true
}

func (o *dummyOutput) Write(ctx context.Context, aligned bool, p []byte) (n int, err error) {
	if o.WriteError != nil {
		return 0, o.WriteError
	}
	return o.bytes.Write(p)
}

func (o *dummyOutput) Flush(context.Context) error {
	return o.SyncError
}

func (o *dummyOutput) Sync(context.Context) error {
	return o.SyncError
}

func (o *dummyOutput) Close(context.Context) error {
	return o.CloseError
}
