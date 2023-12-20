package rollback_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestRollback(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	main := New(logger)

	main.Add(func(ctx context.Context) error {
		logger.DebugCtx(ctx, "main 1")
		_ = logger.Sync()
		return nil
	})
	main.Add(func(ctx context.Context) error {
		logger.DebugCtx(ctx, "main 2")
		_ = logger.Sync()
		return nil
	})
	main.Add(func(ctx context.Context) error {
		return errors.New("main 3 failed")
	})

	sub1 := main.AddParallel()
	sub1.Add(func(ctx context.Context) error {
		logger.DebugCtx(ctx, "parallel operation")
		_ = logger.Sync()
		return nil
	})
	sub1.Add(func(ctx context.Context) error {
		logger.DebugCtx(ctx, "parallel operation")
		_ = logger.Sync()
		return nil
	})
	sub1.Add(func(ctx context.Context) error {
		return errors.New("parallel operation failed")
	})

	sub2 := main.AddLIFO()
	sub2.Add(func(ctx context.Context) error {
		logger.DebugCtx(ctx, "lifo 1")
		_ = logger.Sync()
		return nil
	})
	sub2.Add(func(ctx context.Context) error {
		logger.DebugCtx(ctx, "lifo 2")
		_ = logger.Sync()
		return nil
	})
	sub2.Add(func(ctx context.Context) error {
		return errors.New("lifo 3 failed")
	})

	main.Add(func(ctx context.Context) error {
		logger.DebugCtx(ctx, "main 4")
		_ = logger.Sync()
		return nil
	})

	main.Invoke(context.Background())

	expected := `
DEBUG  main 4
DEBUG  lifo 2
DEBUG  lifo 1
DEBUG  parallel operation
DEBUG  parallel operation
DEBUG  main 2
DEBUG  main 1
WARN  rollback failed:
- lifo 3 failed
- parallel operation failed
- main 3 failed
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), logger.AllMessages())
}
