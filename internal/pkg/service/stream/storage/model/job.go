package model

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

// Job represents workload on sink. It is used to limit the sink when too many jobs are ongoing.
// At the end throttle the sink, so it does not overloads other service
type Job struct {
	key.JobKey
	Deleted bool `json:"-"` // internal field to mark the entity for deletion, there is no soft delete
}
