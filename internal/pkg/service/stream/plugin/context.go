package plugin

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
)

type SaveContext struct {
	now       time.Time
	writeTxn  *op.TxnOp[op.NoResult]
	atomicOps []op.AtomicOpInterface
}

func NewSaveContext(now time.Time) *SaveContext {
	return &SaveContext{
		now:      now,
		writeTxn: op.Txn(nil),
	}
}

func (c *SaveContext) Now() time.Time {
	return c.now
}

func (c *SaveContext) AddOp(ops ...op.Op) {
	c.writeTxn.Merge(ops...)
}

func (c *SaveContext) AddAtomicOp(ops ...op.AtomicOpInterface) {
	c.atomicOps = append(c.atomicOps, ops...)
}

func (c *SaveContext) Apply(ctx context.Context) (op.Op, error) {
	client := op.ClientFromCtx(ctx)

	// Create read operations
	readTxn := op.Txn(client)
	for _, item := range c.atomicOps {
		for _, factory := range item.ReadPhaseOps() {
			if readOp, err := factory(ctx); err != nil {
				return nil, err
			} else if readOp != nil {
				readTxn.Merge(readOp)
			}
		}
	}

	// Invoke read operations
	if err := readTxn.Do(ctx).Err(); err != nil {
		return nil, err
	}

	// Create write operations
	writeTxn := op.Txn(client).Merge(c.writeTxn)
	for _, item := range c.atomicOps {
		// Create write operations
		for _, factory := range item.WritePhaseOps() {
			if writeOp, err := factory(ctx); err != nil {
				return nil, err
			} else if writeOp != nil {
				writeTxn.Merge(writeOp)
			}
		}
	}

	// Return write operation
	return writeTxn, nil
}
