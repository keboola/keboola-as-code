package source

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Update a Branch, if the entity changes between Read and Write phase, then the operation is retried.
func (r *Repository) Update(k key.SourceKey, now time.Time, by definition.By, versionDescription string, updateFn func(definition.Source) (definition.Source, error)) *op.AtomicOp[definition.Source] {
	return r.update(k, now, by, versionDescription, func(source definition.Source) (definition.Source, error) {
		// Store old state
		disabled := source.Disabled
		deleted := source.Deleted

		// Update
		var err error
		source, err = updateFn(source)
		if err != nil {
			return definition.Source{}, err
		}

		// Disabled and Deleted fields cannot be modified by the Update operation
		if disabled != source.Disabled {
			return definition.Source{}, errors.Errorf(`"Disabled" field cannot be modified by the Update operation`)
		}
		if deleted != source.Deleted {
			return definition.Source{}, errors.Errorf(`"Deleted" field cannot be modified by the Update operation`)
		}

		return source, nil
	})
}
