package file

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// IncrementRetryAttempt increments retry attempt and backoff delay on an error.
// Retry is reset on a state transition, model.File.WithState method.
func (r *Repository) IncrementRetryAttempt(k model.FileKey, now time.Time, reason string) *op.AtomicOp[model.File] {
	return r.update(k, now, func(file model.File) (model.File, error) {
		file.IncrementRetryAttempt(r.backoff, now, reason)
		return file, nil
	})
}
