package sink

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Update a Branch, if the entity changes between Read and Write phase, then the operation is retried.
func (r *Repository) Update(k key.SinkKey, now time.Time, by definition.By, versionDescription string, updateFn func(definition.Sink) (definition.Sink, error)) *op.AtomicOp[definition.Sink] {
	return r.update(k, now, by, versionDescription, func(sink definition.Sink) (definition.Sink, error) {
		// Store old state
		disabled := sink.Disabled
		deleted := sink.Deleted

		// Update
		var err error
		sink, err = updateFn(sink)
		if err != nil {
			return definition.Sink{}, err
		}

		// Disabled and Deleted fields cannot be modified by the Update operation
		if disabled != sink.Disabled {
			return definition.Sink{}, errors.Errorf(`"Disabled" field cannot be modified by the Update operation`)
		}
		if deleted != sink.Deleted {
			return definition.Sink{}, errors.Errorf(`"Deleted" field cannot be modified by the Update operation`)
		}

		return sink, err
	})
}
