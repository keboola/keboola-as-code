package servicectx

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestProcess_Add(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	logger.ConnectTo(os.Stdout)
	proc, err := New(WithLogger(logger), WithUniqueID("<id>"))
	assert.NoError(t, err)

	// OpCtx simulates long running operations
	opCtx, opCancel := context.WithCancel(context.Background())

	// There are some parallel operations
	opWg := &sync.WaitGroup{}
	opWg.Add(3)
	proc.Add(func(shutdown ShutdownFn) {
		<-opCtx.Done()
		logger.InfoCtx(opCtx, "end") // STEP 4, see <-opCtx.Done()
		opWg.Done()
	})
	proc.Add(func(shutdown ShutdownFn) {
		<-opCtx.Done()
		logger.InfoCtx(opCtx, "end") // STEP 4, see <-opCtx.Done()
		opWg.Done()
	})
	proc.Add(func(shutdown ShutdownFn) {
		<-opCtx.Done()
		logger.InfoCtx(opCtx, "end") // STEP 4, see <-opCtx.Done()
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
		logger.InfoCtx(ctx, "onShutdown1") // STEP 7, LIFO

		// Shutdown reason can be retrieved from the shutdown context
		if err := ShutdownReason(ctx); assert.Error(t, err) {
			assert.Equal(t, "operation failed", err.Error())
		}
	})
	proc.OnShutdown(func(ctx context.Context) {
		logger.InfoCtx(ctx, "onShutdown2") // STEP 6, LIFO
	})
	proc.OnShutdown(func(ctx context.Context) {
		opWg.Wait()
		logger.InfoCtx(ctx, "onShutdown3") // STEP 5, see op.Wait()
	})

	// Cancel operations from the shutdown callback
	proc.OnShutdown(func(ctx context.Context) {
		opCancel() // STEP 3, LIFO
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
INFO  process unique id "<id>"
INFO  exiting (operation failed)
INFO  end
INFO  end
INFO  end
INFO  onShutdown3
INFO  onShutdown2
INFO  onShutdown1
INFO  exited
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), logger.AllMessages())
}

func TestProcess_Shutdown(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	logger.ConnectTo(os.Stdout)
	proc, err := New(WithLogger(logger), WithUniqueID("<id>"))
	assert.NoError(t, err)

	// OpCtx simulates long running operations
	opCtx, opCancel := context.WithCancel(context.Background())

	// There are some parallel operations
	opWg1 := &sync.WaitGroup{}
	opWg1.Add(1)
	proc.Add(func(shutdown ShutdownFn) {
		<-opCtx.Done()
		logger.InfoCtx(opCtx, "end1") // STEP 3, see <-opCtx.Done()
		opWg1.Done()
	})
	opWg2 := &sync.WaitGroup{}
	opWg2.Add(1)
	proc.Add(func(shutdown ShutdownFn) {
		<-opCtx.Done()
		opWg1.Wait()
		logger.InfoCtx(opCtx, "end2") // STEP 4, see op1.Wait()
		opWg2.Done()
	})
	opWg3 := &sync.WaitGroup{}
	opWg3.Add(1)
	proc.Add(func(shutdown ShutdownFn) {
		<-opCtx.Done()
		opWg2.Wait()
		logger.InfoCtx(opCtx, "end3") // STEP 5, see op2.Wait()
		opWg3.Done()
	})

	// Add more shutdown callbacks
	proc.OnShutdown(func(ctx context.Context) {
		logger.InfoCtx(ctx, "onShutdown1") // STEP 8, LIFO
	})
	proc.OnShutdown(func(ctx context.Context) {
		logger.InfoCtx(ctx, "onShutdown2") // STEP 7, LIFO
	})
	proc.OnShutdown(func(ctx context.Context) {
		opWg3.Wait()
		logger.InfoCtx(ctx, "onShutdown3") // STEP 6, op3.Wait()
	})

	// Cancel operations above from the shutdown callback
	proc.OnShutdown(func(ctx context.Context) {
		opCancel() // STEP 2
	})

	// Trigger shutdown directly
	proc.Shutdown(opCtx, errors.New("some error")) // STEP 1
	proc.WaitForShutdown()

	// Check logs
	expected := `
INFO  process unique id "<id>"
INFO  exiting (some error)
INFO  end1
INFO  end2
INFO  end3
INFO  onShutdown3
INFO  onShutdown2
INFO  onShutdown1
INFO  exited
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), logger.AllMessages())
}
