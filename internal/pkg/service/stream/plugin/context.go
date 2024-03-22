package plugin

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
)

type SaveContext struct {
	*op.AtomicOp[op.NoResult]
	now time.Time
}

func NewSaveContext(now time.Time) *SaveContext {
	return &SaveContext{
		AtomicOp: op.Atomic(nil, &op.NoResult{}),
		now:      now,
	}
}

func (c *SaveContext) Now() time.Time {
	return c.now
}

func (c *SaveContext) Do(ctx context.Context) (op.Op, error) {
	client := op.ClientFromCtx(ctx)

	// Invoke read operations
	readTxn := op.Txn(client)
	for _, factory := range c.ReadPhaseOps() {
		if readOp, err := factory(ctx); err != nil {
			return nil, err
		} else if readOp != nil {
			readTxn.Merge(readOp)
		}
	}
	if err := readTxn.Do(ctx).Err(); err != nil {
		return nil, err
	}

	// Create write operations
	writeTxn := op.Txn(client)
	for _, factory := range c.WritePhaseOps() {
		if writeOp, err := factory(ctx); err != nil {
			return nil, err
		} else if writeOp != nil {
			writeTxn.Merge(writeOp)
		}
	}
	return writeTxn, nil
}
