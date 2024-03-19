package plugin

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"time"
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
	readTxn := op.Txn(nil)
	for _, item := range c.atomicOps {
		// Create read operations
		for _, factory := range item.ReadPhaseOps() {
			if readOp, err := factory(ctx); err == nil {
				readTxn.Merge(readOp)
			} else {
				return nil, err
			}
		}
		// Create write operations
		for _, factory := range item.WritePhaseOps() {
			if writeOp, err := factory(ctx); err == nil {
				c.writeTxn.Merge(writeOp)
			} else {
				return nil, err
			}
		}
	}

	// Invoke read operations
	if err := readTxn.Do(ctx).Err(); err != nil {
		return nil, err
	}

	// Return write operation
	return c.writeTxn, nil
}
