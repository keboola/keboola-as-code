package file

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// IncrementRetry increments retry attempt and backoff delay on an error.
// Retry is reset on StateTransition.
func (r *Repository) IncrementRetry(k model.FileKey, now time.Time, reason string) *op.AtomicOp[model.File] {
	return r.update(k, now, func(slice model.File) (model.File, error) {
		slice.IncrementRetry(r.backoff, now, reason)
		return slice, nil
	})
}
