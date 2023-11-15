package repository

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// transformFn transforms statistics value, for example to set StagingSize after the upload.
type transformFn func(value *statistics.Value)

// Move returns an etcd operation to move slice statistics to a different storage level.
// This operation should not be used separately but atomically together with changing the slice state.
func (r *Repository) Move(sliceKey storage.SliceKey, from, to storage.Level, transform ...transformFn) *op.AtomicOp {
	if from == to {
		panic(errors.Errorf(`from and to categories are same and equal to "%s"`, to))
	}

	fromKey := r.schema.InLevel(from).InSlice(sliceKey)
	toKey := r.schema.InLevel(to).InSlice(sliceKey)

	ops := op.Atomic(r.client)
	var value statistics.Value

	// Load statistics value from the old key
	ops.Read(func() op.Op {
		return fromKey.Get(r.client).WithOnResult(func(result *op.KeyValueT[statistics.Value]) {
			if result != nil {
				value = result.Value
			}
		})
	})

	// Delete the old key
	ops.Write(func() op.Op {
		return fromKey.Delete(r.client)
	})

	// Save the value to the new key
	ops.Write(func() op.Op {
		for _, fn := range transform {
			fn(&value)
		}
		if value.RecordsCount > 0 {
			return toKey.Put(r.client, value)
		} else {
			return nil
		}
	})

	return ops
}
