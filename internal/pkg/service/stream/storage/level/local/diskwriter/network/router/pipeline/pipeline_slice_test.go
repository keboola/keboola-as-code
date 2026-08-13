package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// newTestSlicePipeline builds a SlicePipeline with the open-retry goroutine simulated directly
// (no real encoding/connection managers involved), mirroring exactly the `select { case
// <-time.After(delay): ...; case <-p.ctx.Done(): return }` shape of the real retry loop in
// NewSlicePipeline while it waits for OpenPipeline to keep failing.
func newTestSlicePipeline(t *testing.T, onClose func(ctx context.Context, cause string)) *SlicePipeline {
	t.Helper()

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(nil) })

	p := &SlicePipeline{
		logger:    log.NewNopLogger(),
		ctx:       ctx,
		cancel:    cancel,
		closeDone: make(chan struct{}),
		onClose:   onClose,
	}

	p.wg.Go(func() {
		<-p.ctx.Done()
	})

	return p
}

// TestSlicePipeline_TryOpenSkipsAlreadyClosed is a regression test for the open-retry goroutine
// signaling readiness for a slice pipeline that was actually abandoned (not opened) because Close
// had already run. tryOpen must report opened=false without touching p.encoding at all once
// p.closed is set - computed under the same lock as the closed check, so the caller doesn't need
// to infer it afterwards from state a concurrent Close can independently mutate (e.g. p.ctx.Err(),
// which Close cancels before it even attempts the lock).
func TestSlicePipeline_TryOpenSkipsAlreadyClosed(t *testing.T) {
	t.Parallel()

	p := newTestSlicePipeline(t, func(ctx context.Context, cause string) {})
	p.closed = true // as if Close already ran and is past the point tryOpen checks

	opened, err := p.tryOpen()

	require.NoError(t, err)
	assert.False(t, opened, "tryOpen must report opened=false once the pipeline is closed")
	assert.Nil(t, p.pipeline, "tryOpen must not touch p.encoding/assign a pipeline once closed")
}

// TestSlicePipeline_CloseWhileStillOpening is a regression test for the heap leak: Close used to
// return early - without cancelling the async open-retry goroutine or notifying its parent - if
// it was called while the pipeline was still opening (still in the backoff retry loop). It must
// instead wait out the retry goroutine, leave no pipeline reference behind, and still notify
// onClose.
func TestSlicePipeline_CloseWhileStillOpening(t *testing.T) {
	t.Parallel()

	var onCloseCalls atomic.Int32
	p := newTestSlicePipeline(t, func(ctx context.Context, cause string) {
		onCloseCalls.Add(1)
	})

	p.Close(t.Context(), "test close")

	// If Close returned before the goroutine's wg.Done(), a subsequent p.wg.Wait() would still
	// block; asserting it returns immediately here proves Close already waited for it.
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("open-retry goroutine did not exit by the time Close returned")
	}

	assert.Nil(t, p.pipeline, "no pipeline reference may remain once Close returns")
	assert.EqualValues(t, 1, onCloseCalls.Load(), "onClose must be called even if the pipeline never finished opening")
}

// TestSlicePipeline_CloseIsBlockingIdempotent is a regression test for Close losing its blocking
// idempotence: a concurrent second Close call must not return before the first (winning) Close
// call has fully finished, since router.go's closeSyncer.Notify relies on that to gate the
// storage coordinator on all slice pipelines being actually closed.
func TestSlicePipeline_CloseIsBlockingIdempotent(t *testing.T) {
	t.Parallel()

	onCloseStarted := make(chan struct{})
	releaseOnClose := make(chan struct{})
	var onCloseCalls atomic.Int32
	p := newTestSlicePipeline(t, func(ctx context.Context, cause string) {
		onCloseCalls.Add(1)
		close(onCloseStarted)
		<-releaseOnClose
	})

	var wg sync.WaitGroup
	wg.Add(2)

	firstDone := make(chan struct{})
	go func() {
		defer wg.Done()
		p.Close(t.Context(), "first")
		close(firstDone)
	}()

	// Wait until the first (winning) Close call is stuck inside onClose, i.e. it has already
	// set closed=true and torn down the pipeline, but hasn't returned yet.
	<-onCloseStarted

	secondDone := make(chan struct{})
	go func() {
		defer wg.Done()
		p.Close(t.Context(), "second")
		close(secondDone)
	}()

	select {
	case <-secondDone:
		t.Fatal("second (losing) Close call returned before the first Close call finished")
	case <-time.After(100 * time.Millisecond):
		// Expected: the second call is still blocked on closeDone.
	}

	close(releaseOnClose)
	wg.Wait()

	<-firstDone
	<-secondDone
	assert.EqualValues(t, 1, onCloseCalls.Load(), "onClose must run exactly once")
}
