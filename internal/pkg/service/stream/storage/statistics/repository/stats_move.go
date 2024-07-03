package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// TransformFn transforms statistics value, for example to set StagingSize after the upload.
type TransformFn func(value *statistics.Value)

func (r *Repository) moveStatisticsOnSliceUpdate() {
	r.plugins.Collection().OnSliceUpdate(func(ctx context.Context, now time.Time, original, updated *model.Slice) error {
		fromLevel := original.State.Level()
		toLevel := updated.State.Level()
		if fromLevel != toLevel {
			op.AtomicOpFromCtx(ctx).AddFrom(r.moveAll(updated.SliceKey, fromLevel, toLevel, func(value *statistics.Value) {
				// There is actually no additional compression, when uploading slice to the staging storage
				if toLevel == model.LevelStaging {
					value.StagingSize = value.CompressedSize
				}
			}))
		}
		return nil
	})
}

// moveAll moves all statistics in the parentKey to a different storage level.
// Returned value is sum of all slices statistics.
func (r *Repository) moveAll(parentKey fmt.Stringer, from, to model.Level, transform ...TransformFn) *op.AtomicOp[statistics.Value] {
	if from == to {
		panic(errors.Errorf(`"from" and "to" storage levels are same and equal to "%s"`, to))
	}

	var sum statistics.Value
	atomicOp := op.Atomic(r.client, &sum)

	// List
	var valueKVs op.KeyValuesT[statistics.Value]
	return atomicOp.
		Read(func(ctx context.Context) op.Op {
			return r.schema.InLevel(from).InObject(parentKey).GetAll(r.client).WithAllKVsTo(&valueKVs)
		}).
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
				newKey := oldKey.ReplacePrefix(r.schema.InLevel(from).Prefix(), r.schema.InLevel(to).Prefix())
				txn.Then(newKey.Put(r.client, kv.Value))

				// Delete old record
				txn.Then(oldKey.Delete(r.client))
			}

			if txn.Empty() {
				return nil
			}

			return txn
		})
}
