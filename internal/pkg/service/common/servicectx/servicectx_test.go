package servicectx

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestProcess_Add(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	logger := log.NewDebugLogger()
	proc, err := New(ctx, cancel, logger, WithUniqueID("<id>"))
	assert.NoError(t, err)

	// Do some work
	proc.Add(func(ctx context.Context, errCh chan<- error) {
		<-ctx.Done()
		time.Sleep(100 * time.Millisecond)
		logger.Info("end1")
	})
	proc.Add(func(ctx context.Context, errCh chan<- error) {
		<-ctx.Done()
		time.Sleep(200 * time.Millisecond)
		logger.Info("end2")
	})
	proc.Add(func(ctx context.Context, errCh chan<- error) {
		<-ctx.Done()
		time.Sleep(300 * time.Millisecond)
		logger.Info("end3")
	})
	proc.Add(func(ctx context.Context, errCh chan<- error) {
		errCh <- errors.New("operation failed")
	})
	proc.WaitForShutdown()

	// Check logs
	expected := `
INFO  process unique id "<id>"
INFO  exiting (operation failed)
INFO  end1
INFO  end2
INFO  end3
INFO  exited
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), logger.AllMessages())
}

func TestProcess_Shutdown(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	logger := log.NewDebugLogger()
	proc, err := New(ctx, cancel, logger, WithUniqueID("<id>"))
	assert.NoError(t, err)

	// Do some work
	proc.Add(func(ctx context.Context, errCh chan<- error) {
		<-ctx.Done()
		time.Sleep(100 * time.Millisecond)
		logger.Info("end1")
	})
	proc.Add(func(ctx context.Context, errCh chan<- error) {
		<-ctx.Done()
		time.Sleep(200 * time.Millisecond)
		logger.Info("end2")
	})
	proc.Add(func(ctx context.Context, errCh chan<- error) {
		<-ctx.Done()
		time.Sleep(300 * time.Millisecond)
		logger.Info("end3")
	})
	proc.Shutdown(errors.New("some error"))
	proc.WaitForShutdown()

	// Check logs
	expected := `
INFO  process unique id "<id>"
INFO  exiting (some error)
INFO  end1
INFO  end2
INFO  end3
INFO  exited
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), logger.AllMessages())
}
