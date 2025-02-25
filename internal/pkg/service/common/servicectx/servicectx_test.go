package servicectx

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestProcess_Add(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	proc := New(WithLogger(logger))

	// OpCtx simulates long running operations
	opCtx, opCancel := context.WithCancelCause(context.Background())

	// There are some parallel operations
	opWg := &sync.WaitGroup{}
	opWg.Add(3)
	proc.Add(func(shutdown ShutdownFn) {
		<-opCtx.Done()
		logger.Info(opCtx, "end") // STEP 4, see <-opCtx.Done()
		opWg.Done()
	})
	proc.Add(func(shutdown ShutdownFn) {
		<-opCtx.Done()
		logger.Info(opCtx, "end") // STEP 4, see <-opCtx.Done()
		opWg.Done()
	})
	proc.Add(func(shutdown ShutdownFn) {
		<-opCtx.Done()
		logger.Info(opCtx, "end") // STEP 4, see <-opCtx.Done()
		opWg.Done()
	})

	// Shutdown can be triggered from the operation
	startShutdown := make(chan struct{})
	proc.Add(func(shutdown ShutdownFn) {
		<-startShutdown
		shutdown(opCtx, errors.New("operation failed")) // STEP 2, see <-startShutdown
	})

	// Add more shutdown callbacks
	proc.OnShutdown(func(ctx context.Context) {
		logger.Info(ctx, "onShutdown1") // STEP 7, LIFO

		// Shutdown reason can be retrieved from the shutdown context
		if err := ShutdownReason(ctx); assert.Error(t, err) {
			assert.Equal(t, "operation failed", err.Error())
		}
	})
	proc.OnShutdown(func(ctx context.Context) {
		logger.Info(ctx, "onShutdown2") // STEP 6, LIFO
	})
	proc.OnShutdown(func(ctx context.Context) {
		opWg.Wait()
		logger.Info(ctx, "onShutdown3") // STEP 5, see op.Wait()
	})

	// Cancel operations from the shutdown callback
	proc.OnShutdown(func(ctx context.Context) {
		opCancel(errors.New("shutdown")) // STEP 3, LIFO
	})

	// Trigger shutdown from the operation above
	close(startShutdown) // STEP 1

	// Wait can be called multiple times
	proc.WaitForShutdown()
	proc.WaitForShutdown()
	proc.WaitForShutdown()
	proc.WaitForShutdown()

	// Shutdown can be called multiple times
	proc.Shutdown(opCtx, errors.New("ignore duplicated shutdown"))
	proc.Shutdown(opCtx, errors.New("ignore duplicated shutdown"))
	proc.Shutdown(opCtx, errors.New("ignore duplicated shutdown"))

	// Check logs
	expected := `
{"level":"info","message":"exiting (operation failed)"}
{"level":"info","message":"end"}
{"level":"info","message":"end"}
{"level":"info","message":"end"}
{"level":"info","message":"onShutdown3"}
{"level":"info","message":"onShutdown2"}
{"level":"info","message":"onShutdown1"}
{"level":"info","message":"exited"}
`
	logger.AssertJSONMessages(t, expected)
}

func TestProcess_Shutdown(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	proc := New(WithLogger(logger))

	// OpCtx simulates long running operations
	opCtx, opCancel := context.WithCancelCause(context.Background())

	// There are some parallel operations
	opWg1 := &sync.WaitGroup{}
	opWg1.Add(1)
	proc.Add(func(shutdown ShutdownFn) {
		<-opCtx.Done()
		logger.Info(opCtx, "end1") // STEP 3, see <-opCtx.Done()
		opWg1.Done()
	})
	opWg2 := &sync.WaitGroup{}
	opWg2.Add(1)
	proc.Add(func(shutdown ShutdownFn) {
		<-opCtx.Done()
		opWg1.Wait()
		logger.Info(opCtx, "end2") // STEP 4, see op1.Wait()
		opWg2.Done()
	})
	opWg3 := &sync.WaitGroup{}
	opWg3.Add(1)
	proc.Add(func(shutdown ShutdownFn) {
		<-opCtx.Done()
		opWg2.Wait()
		logger.Info(opCtx, "end3") // STEP 5, see op2.Wait()
		opWg3.Done()
	})

	// Add more shutdown callbacks
	proc.OnShutdown(func(ctx context.Context) {
		logger.Info(ctx, "onShutdown1") // STEP 8, LIFO
	})
	proc.OnShutdown(func(ctx context.Context) {
		logger.Info(ctx, "onShutdown2") // STEP 7, LIFO
	})
	proc.OnShutdown(func(ctx context.Context) {
		opWg3.Wait()
		logger.Info(ctx, "onShutdown3") // STEP 6, op3.Wait()
	})

	// Cancel operations above from the shutdown callback
	proc.OnShutdown(func(ctx context.Context) {
		opCancel(errors.New("shutdown")) // STEP 2
	})

	// Trigger shutdown directly
	proc.Shutdown(opCtx, errors.New("some error")) // STEP 1
	proc.WaitForShutdown()

	// Check logs
	expected := `
{"level":"info","message":"exiting (some error)"}
{"level":"info","message":"end1"}
{"level":"info","message":"end2"}
{"level":"info","message":"end3"}
{"level":"info","message":"onShutdown3"}
{"level":"info","message":"onShutdown2"}
{"level":"info","message":"onShutdown1"}
{"level":"info","message":"exited"}
`
	logger.AssertJSONMessages(t, expected)
}
