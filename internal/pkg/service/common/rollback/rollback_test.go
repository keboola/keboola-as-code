package rollback_test

import (
	"context"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestRollback(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	main := New(logger)

	main.Add(func(ctx context.Context) error {
		logger.Debug(ctx, "main 1")
		_ = logger.Sync()
		return nil
	})
	main.Add(func(ctx context.Context) error {
		logger.Debug(ctx, "main 2")
		_ = logger.Sync()
		return nil
	})
	main.Add(func(ctx context.Context) error {
		return errors.New("main 3 failed")
	})

	sub1 := main.AddParallel()
	sub1.Add(func(ctx context.Context) error {
		logger.Debug(ctx, "parallel operation")
		_ = logger.Sync()
		return nil
	})
	sub1.Add(func(ctx context.Context) error {
		logger.Debug(ctx, "parallel operation")
		_ = logger.Sync()
		return nil
	})
	sub1.Add(func(ctx context.Context) error {
		return errors.New("parallel operation failed")
	})

	sub2 := main.AddLIFO()
	sub2.Add(func(ctx context.Context) error {
		logger.Debug(ctx, "lifo 1")
		_ = logger.Sync()
		return nil
	})
	sub2.Add(func(ctx context.Context) error {
		logger.Debug(ctx, "lifo 2")
		_ = logger.Sync()
		return nil
	})
	sub2.Add(func(ctx context.Context) error {
		return errors.New("lifo 3 failed")
	})

	main.Add(func(ctx context.Context) error {
		logger.Debug(ctx, "main 4")
		_ = logger.Sync()
		return nil
	})

	main.Invoke(t.Context())

	expected := `
{"level":"debug","message":"main 4"}
{"level":"debug","message":"lifo 2"}
{"level":"debug","message":"lifo 1"}
{"level":"debug","message":"parallel operation"}
{"level":"debug","message":"parallel operation"}
{"level":"debug","message":"main 2"}
{"level":"debug","message":"main 1"}
{"level":"warn","message":"rollback failed:\n- lifo 3 failed\n- parallel operation failed\n- main 3 failed"}
`
	logger.AssertJSONMessages(t, expected)
}
