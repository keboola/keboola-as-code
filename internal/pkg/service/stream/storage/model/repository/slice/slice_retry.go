package slice

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// IncrementRetryAttempt increments retry attempt and backoff delay on an error.
// Retry is reset on a state transition, model.Slice.WithState method.
func (r *Repository) IncrementRetryAttempt(sliceKey model.SliceKey, now time.Time, reason string) *op.AtomicOp[model.Slice] {
	return r.update(sliceKey, now, func(slice model.Slice) (model.Slice, error) {
		slice.IncrementRetryAttempt(r.backoff, now, reason)
		return slice, nil
	})
}
