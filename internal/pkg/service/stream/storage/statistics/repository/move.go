package repository

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// TransformFn transforms statistics value, for example to set StagingSize after the upload.
type TransformFn func(value *statistics.Value)

// Move moves slice statistics to a different storage level.
// This operation should not be used separately but atomically together with changing the slice state.
func (r *Repository) Move(sliceKey model.SliceKey, from, to level.Level, transform ...TransformFn) *op.AtomicOp[statistics.Value] {
	return r.MoveAll(sliceKey, from, to, transform...)
}

// MoveAll moves all statistics in the parentKey to a different storage level.
// Returned value is sum of all slices statistics.
// This operation should not be used separately but atomically together with changing the slice state.
func (r *Repository) MoveAll(parentKey fmt.Stringer, from, to level.Level, transform ...TransformFn) *op.AtomicOp[statistics.Value] {
	if from == to {
		panic(errors.Errorf(`"from" and "to" storage levels are same and equal to "%s"`, to))
	}

	var sum statistics.Value
	atomicOp := op.Atomic(r.client, &sum)

	var valueKVs op.KeyValuesT[statistics.Value]
	if k, ok := parentKey.(model.SliceKey); ok {
		// Get
		atomicOp.ReadOp(r.schema.InLevel(from).InSlice(k).GetKV(r.client).WithOnResult(func(kv *op.KeyValueT[statistics.Value]) {
			if kv == nil {
				valueKVs = nil
			} else {
				valueKVs = []*op.KeyValueT[statistics.Value]{kv}
			}
		}))
	} else {
		// List
		atomicOp.ReadOp(r.schema.InLevel(from).InObject(parentKey).GetAll(r.client).WithAllKVsTo(&valueKVs))
	}

	return atomicOp.
		Write(func(ctx context.Context) op.Op {
			txn := op.Txn(r.client)

			// Process statistics for all slices
			for _, kv := range valueKVs {
				// Apply transform functions
				for _, fn := range transform {
					fn(&kv.Value)
				}

				// Add value to the sum
				sum = sum.Add(kv.Value)

				// Compose typed key from the result KV
				oldKey := etcdop.NewTypedKey[statistics.Value](kv.Key(), r.schema.Serde())

				// Save value to the new destination
				if kv.Value.RecordsCount > 0 {
					newKey := oldKey.ReplacePrefix(r.schema.InLevel(from).Prefix(), r.schema.InLevel(to).Prefix())
					txn.Then(newKey.Put(r.client, kv.Value))
				}

				// Delete old record
				txn.Then(oldKey.Delete(r.client))
			}

			if txn.Empty() {
				return nil
			}

			return txn
		})
}
