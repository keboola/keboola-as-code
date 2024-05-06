package slice

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"time"
)

// IncrementRetry increments retry attempt and backoff delay on an error.
// Retry is reset on StateTransition.
func (r *Repository) IncrementRetry(now time.Time, sliceKey model.SliceKey, reason string) *op.AtomicOp[model.Slice] {
	return r.updateOne(sliceKey, now, func(slice model.Slice) (model.Slice, error) {
		slice.IncrementRetryAttempt(r.backoff, now, reason)
		return slice, nil
	})
}
