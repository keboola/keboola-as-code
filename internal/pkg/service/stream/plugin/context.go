package plugin

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
)

type ctxKey string

const value = ctxKey("value")

type Operation struct {
	*op.AtomicOp[op.NoResult]
	now time.Time
}

func X(ctx context.Context, now time.Time) (context.Context, *Operation) {
	plugin, ok := ctx.Value(value).(*Operation)
	if !ok {
		plugin = &Operation{
			AtomicOp: op.Atomic(nil, &op.NoResult{}),
			now:      now,
		}
		ctx = context.WithValue(ctx, value, plugin)
	}

	return ctx, plugin
}

func FromContext(ctx context.Context) *Operation {
	plugin, ok := ctx.Value(value).(*Operation)
	if !ok {
		panic(errors.New("operation is not connected to the plugin system"))
	}
	return plugin
}

func (c *Operation) Now() time.Time {
	return c.now
}

func (c *Operation) Do(ctx context.Context) (op.Op, error) {
	client := op.AtomicOpFromCtx(ctx)

	writeTxn := op.Txn(client)

	// Invoke read operations
	for {
		readTxn := op.Txn(client)

		readOps := c.AtomicOp.ReadPhaseOps()
		writeOps := c.AtomicOp.WritePhaseOps()
		if len(readOps) == 0 && len(writeOps) == 0 {
			break
		}

		c.AtomicOp = op.Atomic(nil, &op.NoResult{})

		for _, factory := range readOps {
			if readOp, err := factory(ctx); err != nil {
				return nil, err
			} else if readOp != nil {
				readTxn.Merge(readOp)
			}
		}

		if err := readTxn.Do(ctx).Err(); err != nil {
			return nil, err
		}

		for _, factory := range writeOps {
			if writeOp, err := factory(ctx); err != nil {
				return nil, err
			} else if writeOp != nil {
				writeTxn.Merge(writeOp)
			}
		}
	}

	return writeTxn, nil
}
