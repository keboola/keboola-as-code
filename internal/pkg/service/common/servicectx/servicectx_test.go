package servicectx

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestProcess_Add(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	logger := log.NewDebugLogger()
	proc, err := New(ctx, cancel, WithLogger(logger), WithUniqueID("<id>"))
	assert.NoError(t, err)

	op1 := &sync.WaitGroup{}
	op1.Add(1)
	op2 := &sync.WaitGroup{}
	op2.Add(1)
	op3 := &sync.WaitGroup{}
	op3.Add(1)

	// Do some work, operations run in parallel
	proc.Add(func(ctx context.Context, shutdown ShutdownFn) {
		<-ctx.Done()
		logger.InfoCtx(ctx, "end1")
		op1.Done()
	})
	proc.Add(func(ctx context.Context, shutdown ShutdownFn) {
		<-ctx.Done()
		op1.Wait()
		logger.InfoCtx(ctx, "end2")
		op2.Done()
	})
	proc.Add(func(ctx context.Context, shutdown ShutdownFn) {
		<-ctx.Done()
		op2.Wait()
		logger.InfoCtx(ctx, "end3")
		op3.Done()
	})
	startShutdown := make(chan struct{})
	proc.Add(func(ctx context.Context, shutdown ShutdownFn) {
		<-startShutdown
		shutdown(ctx, errors.New("operation failed"))
	})
	proc.OnShutdown(func(ctx context.Context) {
		logger.InfoCtx(ctx, "onShutdown1")
	})
	proc.OnShutdown(func(ctx context.Context) {
		logger.InfoCtx(ctx, "onShutdown2")
	})
	proc.OnShutdown(func(ctx context.Context) {
		op3.Wait()
		logger.InfoCtx(ctx, "onShutdown3")
	})

	// Wait can be called multiple times
	close(startShutdown)
	proc.WaitForShutdown()
	proc.WaitForShutdown()
	proc.WaitForShutdown()
	proc.WaitForShutdown()

	// Shutdown can be called multiple times
	proc.Shutdown(ctx, errors.New("ignore duplicated shutdown"))
	proc.Shutdown(ctx, errors.New("ignore duplicated shutdown"))
	proc.Shutdown(ctx, errors.New("ignore duplicated shutdown"))

	// Check logs
	expected := `
INFO  process unique id "<id>"
INFO  exiting (operation failed)
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

func TestProcess_Shutdown(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	logger := log.NewDebugLogger()
	proc, err := New(ctx, cancel, WithLogger(logger), WithUniqueID("<id>"))
	assert.NoError(t, err)

	op1 := &sync.WaitGroup{}
	op1.Add(1)
	op2 := &sync.WaitGroup{}
	op2.Add(1)
	op3 := &sync.WaitGroup{}
	op3.Add(1)

	// Do some work, operations run in parallel
	proc.Add(func(ctx context.Context, shutdown ShutdownFn) {
		<-ctx.Done()
		logger.InfoCtx(ctx, "end1")
		op1.Done()
	})
	proc.Add(func(ctx context.Context, shutdown ShutdownFn) {
		<-ctx.Done()
		op1.Wait()
		logger.InfoCtx(ctx, "end2")
		op2.Done()
	})
	proc.Add(func(ctx context.Context, shutdown ShutdownFn) {
		<-ctx.Done()
		op2.Wait()
		logger.InfoCtx(ctx, "end3")
		op3.Done()
	})
	proc.OnShutdown(func(ctx context.Context) {
		logger.InfoCtx(ctx, "onShutdown1")
	})
	proc.OnShutdown(func(ctx context.Context) {
		logger.InfoCtx(ctx, "onShutdown2")
	})
	proc.OnShutdown(func(ctx context.Context) {
		op3.Wait()
		logger.InfoCtx(ctx, "onShutdown3")
	})
	proc.Shutdown(ctx, errors.New("some error"))
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
