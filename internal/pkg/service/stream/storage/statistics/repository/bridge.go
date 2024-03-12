package repository

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/hook"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

func (r *Repository) bridge(h *hook.Registry) {
	h.OnFileStateTransition(func(fileKey model.FileKey, from, to model.FileState, atomicOp *op.AtomicOp[model.File]) {
		// Move statistics to the target storage level, if needed
		fromLevel := from.Level()
		toLevel := to.Level()
		if fromLevel != toLevel {
			atomicOp.AddFrom(r.MoveAll(fileKey, fromLevel, toLevel, func(value *statistics.Value) {
				// There is actually no additional compression, when uploading slice to the staging storage
				if toLevel == level.Staging {
					value.StagingSize = value.CompressedSize
				}
			}))
		}
	})

	h.OnSliceStateTransition(func(sliceKey model.SliceKey, from, to model.SliceState, atomicOp *op.AtomicOp[model.Slice]) {
		// Move statistics to the target storage level, if needed
		fromLevel := from.Level()
		toLevel := to.Level()
		if fromLevel != toLevel {
			atomicOp.AddFrom(r.Move(sliceKey, fromLevel, toLevel, func(value *statistics.Value) {
				// There is actually no additional compression, when uploading slice to the staging storage
				if toLevel == level.Staging {
					value.StagingSize = value.CompressedSize
				}
			}))
		}
	})

	h.OnFileDelete(func(fileKey model.FileKey, atomicOp *op.AtomicOp[op.NoResult]) {
		// Delete/rollup statistics
		atomicOp.AddFrom(r.Delete(fileKey))
	})

	h.OnSliceDelete(func(sliceKey model.SliceKey, atomicOp *op.AtomicOp[op.NoResult]) {
		// Delete/rollup statistics
		atomicOp.AddFrom(r.Delete(sliceKey))
	})
}
